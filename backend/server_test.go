package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type staticIdentity Identity

func (i staticIdentity) Identity(_ context.Context, _ *http.Request) (Identity, error) {
	return validateIdentity(Identity(i))
}

type staticQuota struct {
	response QuotaResponse
	err      error
}

func (q staticQuota) Quota(_ context.Context, namespace string) (QuotaResponse, error) {
	if q.err != nil {
		return QuotaResponse{}, q.err
	}
	response := q.response
	response.Namespace = namespace
	return response, nil
}

func newTestServer() (*Server, *MemoryStore) {
	store := NewMemoryStore()
	return &Server{
		Store:    store,
		Metrics:  FixedMetricsProvider{Values: map[string]MetricResponse{"broker_count": {Unit: "count", Values: []MetricPoint{{Value: 1}}}}},
		Identity: staticIdentity{Namespace: "ns-test", UserID: "user-1"},
	}, store
}

func seedReadyMessageQueue(t *testing.T, store *MemoryStore, namespace, name string) {
	t.Helper()
	_, err := store.Create(context.Background(), namespace, MessageQueue{
		Metadata: Metadata{Name: name},
		Spec:     MessageQueueSpec{Engine: "kafka", Kafka: KafkaSpec{Version: "3.9.0", Replicas: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	resource := store.items[namespace][name]
	resource.Status = MessageQueueStatus{
		Phase:           "Ready",
		ClientSecretRef: name + "-client",
		Endpoints:       []string{name + "-kafka-bootstrap." + namespace + ".svc:9093"},
		ReadyReplicas:   1,
	}
	store.items[namespace][name] = resource
	store.mu.Unlock()
}

func TestServerRequiresServerSideIdentity(t *testing.T) {
	store := NewMemoryStore()
	server := &Server{Store: store, Metrics: UnavailableMetricsProvider{}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues", nil)
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured server status = %d, want 503", recording.Code)
	}

	server.Identity = staticIdentity{Namespace: "ns-test", UserID: "user-1"}
	recording = httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK {
		t.Fatalf("configured server status = %d, want 200", recording.Code)
	}
}

func encodedKubeconfig(namespace, user, server, token string) string {
	contextNamespace := ""
	if namespace != "" {
		contextNamespace = fmt.Sprintf("      namespace: %s\n", namespace)
	}
	data := fmt.Sprintf(`apiVersion: v1
kind: Config
current-context: current
clusters:
  - name: cluster
    cluster:
      server: %s
      insecure-skip-tls-verify: true
contexts:
  - name: current
    context:
      cluster: cluster
      user: %s
%susers:
  - name: %s
    user:
      token: %s
`, server, user, contextNamespace, user, token)
	return url.PathEscape(data)
}

func TestHealthAndReadinessDoNotRequireWorkspaceIdentity(t *testing.T) {
	server := &Server{}
	for _, path := range []string{"/healthz", "/readyz"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		recording := httptest.NewRecorder()
		server.ServeHTTP(recording, request)
		if recording.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", path, recording.Code)
		}
		if got := recording.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("%s Cache-Control = %q, want no-store", path, got)
		}
		if got := recording.Header().Get("Content-Type"); got != "application/json" {
			t.Fatalf("%s Content-Type = %q, want application/json", path, got)
		}
		body := strings.TrimSpace(recording.Body.String())
		if body != `{"service":"messagequeue","status":"ok"}` {
			t.Fatalf("%s body = %s", path, body)
		}
	}
}

func TestCreateListDetailAndStatusUseIdentityNamespace(t *testing.T) {
	server, store := newTestServer()
	body := `{"name":"orders","spec":{"engine":"kafka","kafka":{"replicas":1},"resources":{"cpu":"1","memory":"2Gi"},"storage":{"size":"10Gi"}}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", recording.Code, recording.Body.String())
	}
	created := MessageQueueView{}
	if err := json.Unmarshal(recording.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Namespace != "ns-test" || created.Name != "orders" {
		t.Fatalf("created identity = %#v", created)
	}
	if _, err := store.Get(context.Background(), "browser-controlled", "orders"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("resource was written outside identity namespace: %v", err)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders", nil)
	recording = httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK {
		t.Fatalf("detail status = %d", recording.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders/status", nil)
	recording = httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK || !strings.Contains(recording.Body.String(), `"phase":"Pending"`) {
		t.Fatalf("status response = %d %s", recording.Code, recording.Body.String())
	}
}

func TestKubeconfigIdentityProviderUsesAuthorizationNamespace(t *testing.T) {
	server, store := newTestServer()
	server.Identity = KubeconfigIdentityProvider{Fallback: staticIdentity{Namespace: "ns-fallback", UserID: "fallback"}}
	body := `{"name":"orders","spec":{"engine":"kafka"}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
	request.Header.Set("Authorization", encodedKubeconfig("ns-alice", "alice", "https://kubernetes.example", "token-1"))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", recording.Code, recording.Body.String())
	}
	if _, err := store.Get(context.Background(), "ns-alice", "orders"); err != nil {
		t.Fatalf("resource was not written to kubeconfig namespace: %v", err)
	}
	if _, err := store.Get(context.Background(), "ns-fallback", "orders"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("invalid fallback write: %v", err)
	}
}

func TestKubeconfigIdentityProviderRejectsInvalidAuthorizationWithoutFallback(t *testing.T) {
	server, _ := newTestServer()
	server.Identity = KubeconfigIdentityProvider{Fallback: staticIdentity{Namespace: "ns-fallback", UserID: "fallback"}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues", nil)
	request.Header.Set("Authorization", "Bearer not-a-kubeconfig")
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusUnauthorized {
		t.Fatalf("invalid authorization status = %d, body=%s", recording.Code, recording.Body.String())
	}
}

func TestKubeconfigIdentityProviderFallsBackToUserNamespace(t *testing.T) {
	_, identity, err := accessFromAuthorization(encodedKubeconfig("", "alice", "https://kubernetes.example", "token-1"))
	if err != nil {
		t.Fatal(err)
	}
	if identity.Namespace != "ns-alice" || identity.UserID != "alice" {
		t.Fatalf("identity = %#v", identity)
	}
}

func TestWritesRequireServerSideWorkspaceIdentity(t *testing.T) {
	server := &Server{
		Store:    NewMemoryStore(),
		Metrics:  UnavailableMetricsProvider{},
		Identity: staticIdentity{Namespace: "", UserID: "user-1"},
	}
	requests := []*http.Request{
		httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(`{"name":"orders","engine":"kafka"}`)),
		httptest.NewRequest(http.MethodDelete, "/api/v1/messagequeues/orders", nil),
		httptest.NewRequest(http.MethodPut, "/api/v1/messagequeues/orders/external-access", strings.NewReader(`{"enabled":true}`)),
	}
	for _, request := range requests {
		recording := httptest.NewRecorder()
		server.ServeHTTP(recording, request)
		if recording.Code != http.StatusUnauthorized {
			t.Fatalf("%s status = %d, body=%s", request.Method, recording.Code, recording.Body.String())
		}
	}
}

func TestCreateRejectsBrowserNamespaceAndSecretPayload(t *testing.T) {
	server, _ := newTestServer()
	body := `{"name":"orders","namespace":"other-ns","spec":{"engine":"kafka","resources":{"cpu":"1","memory":"2Gi"},"storage":{"size":"10Gi"}},"secret":{"password":"do-not-accept"}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusBadRequest {
		t.Fatalf("unknown namespace/secret fields status = %d", recording.Code)
	}
}

func TestExternalAccessUsesIdentityNamespaceAndIsIdempotent(t *testing.T) {
	server, store := newTestServer()
	seedReadyMessageQueue(t, store, "ns-test", "orders")
	seedReadyMessageQueue(t, store, "other-ns", "orders")

	for range 2 {
		request := httptest.NewRequest(http.MethodPut, "/api/v1/messagequeues/orders/external-access", strings.NewReader(`{"enabled":true}`))
		recording := httptest.NewRecorder()
		server.ServeHTTP(recording, request)
		if recording.Code != http.StatusOK {
			t.Fatalf("external access status = %d, body=%s", recording.Code, recording.Body.String())
		}
	}

	updated, err := store.Get(context.Background(), "ns-test", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Kafka.Listeners == nil || updated.Spec.Kafka.Listeners.External == nil || !updated.Spec.Kafka.Listeners.External.Enabled {
		t.Fatalf("external intent was not stored: %#v", updated.Spec.Kafka)
	}
	if updated.Metadata.Generation != 2 {
		t.Fatalf("idempotent update generation = %d, want 2", updated.Metadata.Generation)
	}
	other, err := store.Get(context.Background(), "other-ns", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if other.Spec.Kafka.Listeners != nil {
		t.Fatalf("external update crossed namespace: %#v", other.Spec.Kafka.Listeners)
	}
}

func TestExternalAccessRejectsNonExactBodiesAndWrongMethods(t *testing.T) {
	server, store := newTestServer()
	seedReadyMessageQueue(t, store, "ns-test", "orders")

	for _, body := range []string{
		`{}`,
		`{"enabled":null}`,
		`{"enabled":"true"}`,
		`{"enabled":true,"namespace":"other-ns"}`,
		`{"enabled":true,"enabled":false}`,
		`{"enabled":true}{"enabled":false}`,
	} {
		request := httptest.NewRequest(http.MethodPut, "/api/v1/messagequeues/orders/external-access", strings.NewReader(body))
		recording := httptest.NewRecorder()
		server.ServeHTTP(recording, request)
		if recording.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, body=%s", body, recording.Code, recording.Body.String())
		}
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues/orders/external-access", strings.NewReader(`{"enabled":true}`))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusMethodNotAllowed || recording.Header().Get("Allow") != http.MethodPut {
		t.Fatalf("wrong method response = %d allow=%q", recording.Code, recording.Header().Get("Allow"))
	}
}

func TestSuspensionUsesIdentityNamespaceAndIsIdempotent(t *testing.T) {
	server, store := newTestServer()
	seedReadyMessageQueue(t, store, "ns-test", "orders")
	seedReadyMessageQueue(t, store, "other-ns", "orders")

	for range 2 {
		request := httptest.NewRequest(http.MethodPut, "/api/v1/messagequeues/orders/suspension", strings.NewReader(`{"suspended":true}`))
		recording := httptest.NewRecorder()
		server.ServeHTTP(recording, request)
		if recording.Code != http.StatusOK {
			t.Fatalf("suspension status = %d, body=%s", recording.Code, recording.Body.String())
		}
	}

	updated, err := store.Get(context.Background(), "ns-test", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if !updated.Spec.Suspend || updated.Metadata.Generation != 2 {
		t.Fatalf("suspension patch = suspend:%v generation:%d, want true generation 2", updated.Spec.Suspend, updated.Metadata.Generation)
	}
	other, err := store.Get(context.Background(), "other-ns", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if other.Spec.Suspend {
		t.Fatalf("suspension crossed namespace: %#v", other.Spec)
	}
}

func TestSuspensionRejectsNonExactBodiesAndWrongMethods(t *testing.T) {
	server, store := newTestServer()
	seedReadyMessageQueue(t, store, "ns-test", "orders")

	for _, body := range []string{
		`{}`,
		`{"suspended":null}`,
		`{"suspended":"true"}`,
		`{"suspended":true,"namespace":"other-ns"}`,
		`{"suspended":true,"suspended":false}`,
		`{"suspended":true}{"suspended":false}`,
	} {
		request := httptest.NewRequest(http.MethodPut, "/api/v1/messagequeues/orders/suspension", strings.NewReader(body))
		recording := httptest.NewRecorder()
		server.ServeHTTP(recording, request)
		if recording.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, body=%s", body, recording.Code, recording.Body.String())
		}
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues/orders/suspension", strings.NewReader(`{"suspended":true}`))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusMethodNotAllowed || recording.Header().Get("Allow") != http.MethodPut {
		t.Fatalf("wrong method response = %d allow=%q", recording.Code, recording.Header().Get("Allow"))
	}
}

func TestExternalAccessConfigurationDefaultsAndRejectsUnsafeTypes(t *testing.T) {
	t.Setenv("MESSAGEQUEUE_EXTERNAL_LISTENER_TYPE", "")
	t.Setenv("MESSAGEQUEUE_EXTERNAL_NODE_ADDRESS_TYPE", "")
	config, err := externalAccessConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if config.ListenerType != "nodeport" || config.PreferredNodePortAddressType != "InternalIP" {
		t.Fatalf("external defaults = %#v", config)
	}

	t.Setenv("MESSAGEQUEUE_EXTERNAL_LISTENER_TYPE", "loadbalancer")
	if _, err := externalAccessConfigFromEnv(); err == nil {
		t.Fatal("loadbalancer listener type should be rejected")
	}
	t.Setenv("MESSAGEQUEUE_EXTERNAL_LISTENER_TYPE", "nodeport")
	t.Setenv("MESSAGEQUEUE_EXTERNAL_NODE_ADDRESS_TYPE", "PrivateIP")
	if _, err := externalAccessConfigFromEnv(); err == nil {
		t.Fatal("unknown node address type should be rejected")
	}
}

func TestCreateAcceptsFirstPartyFlatContract(t *testing.T) {
	server, store := newTestServer()
	body := `{"name":"orders","engine":"kafka","kafka":{"version":"3.9.0","brokers":3,"cpu":"500m","memory":"1Gi","storageGi":20,"storageClass":"fast"},"deletionPolicy":"retain","monitoring":{"enabled":true},"console":{"enabled":false}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusCreated {
		t.Fatalf("flat create status = %d, body=%s", recording.Code, recording.Body.String())
	}
	created, err := store.Get(context.Background(), "ns-test", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if created.Spec.Kafka.Replicas != 3 || created.Spec.Kafka.Version != "3.9.0" || created.Spec.Resources.CPU != "500m" || created.Spec.Resources.Memory != "1Gi" || created.Spec.Storage.Size != "20Gi" || created.Spec.Storage.ClassName != "fast" || created.Spec.DeletionPolicy != "Retain" {
		t.Fatalf("flat request translation = %#v", created.Spec)
	}
}

func TestCreateAppliesDevelopmentDefaults(t *testing.T) {
	server, store := newTestServer()
	body := `{"name":"orders","spec":{"engine":"kafka"}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", recording.Code, recording.Body.String())
	}
	created, err := store.Get(context.Background(), "ns-test", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if created.Spec.Kafka.Version != "3.9.0" || created.Spec.Kafka.Replicas != 1 || created.Spec.Resources.CPU != "500m" || created.Spec.Resources.Memory != "1Gi" || created.Spec.Storage.Size != "10Gi" || created.Spec.DeletionPolicy != "Retain" {
		t.Fatalf("defaults = %#v", created.Spec)
	}
}

func TestCreateRejectsQuotaExceededBeforeWrite(t *testing.T) {
	server, store := newTestServer()
	server.Quota = staticQuota{response: QuotaResponse{Items: []QuotaItem{
		{Type: "cpu", Limit: 4, Used: 3.8, Available: 0.2, Unit: "cores"},
		{Type: "memory", Limit: 8, Used: 1, Available: 7, Unit: "Gi"},
		{Type: "storage", Limit: 100, Used: 10, Available: 90, Unit: "Gi"},
	}}}
	body := `{"name":"orders","spec":{"engine":"kafka","kafka":{"replicas":1},"resources":{"cpu":"500m","memory":"1Gi"},"storage":{"size":"10Gi"}}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusForbidden || !strings.Contains(recording.Body.String(), `"code":"quota_exceeded"`) {
		t.Fatalf("quota response = %d %s", recording.Code, recording.Body.String())
	}
	if _, err := store.Get(context.Background(), "ns-test", "orders"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("quota failure should not create resource: %v", err)
	}
}

func TestCreateAllowsDegradedQuotaPreflight(t *testing.T) {
	server, store := newTestServer()
	server.Quota = staticQuota{response: QuotaResponse{Degraded: true, Message: "workspace quota is not configured"}}
	body := `{"name":"orders","spec":{"engine":"kafka"}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusCreated {
		t.Fatalf("degraded quota create status = %d, body=%s", recording.Code, recording.Body.String())
	}
	if _, err := store.Get(context.Background(), "ns-test", "orders"); err != nil {
		t.Fatal(err)
	}
}

func TestQuotaEndpointReturnsWorkspaceQuota(t *testing.T) {
	server, _ := newTestServer()
	server.Quota = staticQuota{response: QuotaResponse{Items: []QuotaItem{{Type: "cpu", Limit: 4, Used: 1, Available: 3, Unit: "cores"}}}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/-/quota", nil)
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK || !strings.Contains(recording.Body.String(), `"namespace":"ns-test"`) || !strings.Contains(recording.Body.String(), `"type":"cpu"`) {
		t.Fatalf("quota response = %d %s", recording.Code, recording.Body.String())
	}
}

func TestCreateRejectsInvalidResourceQuantities(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "product_cpu",
			body: `{"name":"orders","spec":{"engine":"kafka","resources":{"cpu":"0","memory":"1Gi"},"storage":{"size":"10Gi"}}}`,
			want: "resources.cpu",
		},
		{
			name: "product_memory",
			body: `{"name":"orders","spec":{"engine":"kafka","resources":{"cpu":"500m","memory":"1024"},"storage":{"size":"10Gi"}}}`,
			want: "resources.memory",
		},
		{
			name: "flat_cpu",
			body: `{"name":"orders","engine":"kafka","kafka":{"brokers":1,"cpu":"half","memory":"1Gi"}}`,
			want: "resources.cpu",
		},
		{
			name: "flat_memory",
			body: `{"name":"orders","engine":"kafka","kafka":{"brokers":1,"cpu":"500m","memory":"1gi"}}`,
			want: "resources.memory",
		},
		{
			name: "product_storage",
			body: `{"name":"orders","spec":{"engine":"kafka","resources":{"cpu":"500m","memory":"1Gi"},"storage":{"size":"not-a-quantity"}}}`,
			want: "storage.size",
		},
		{
			name: "cpu_too_large",
			body: `{"name":"orders","spec":{"engine":"kafka","resources":{"cpu":"9","memory":"1Gi"},"storage":{"size":"10Gi"}}}`,
			want: "resources.cpu",
		},
		{
			name: "memory_too_large",
			body: `{"name":"orders","spec":{"engine":"kafka","resources":{"cpu":"500m","memory":"65Gi"},"storage":{"size":"10Gi"}}}`,
			want: "resources.memory",
		},
		{
			name: "storage_too_large",
			body: `{"name":"orders","spec":{"engine":"kafka","resources":{"cpu":"500m","memory":"1Gi"},"storage":{"size":"2Ti"}}}`,
			want: "storage.size",
		},
		{
			name: "storage_class_name",
			body: `{"name":"orders","spec":{"engine":"kafka","storage":{"size":"10Gi","className":"bad/class"}}}`,
			want: "storage.className",
		},
		{
			name: "flat_storage_too_large",
			body: `{"name":"orders","engine":"kafka","kafka":{"brokers":1,"cpu":"500m","memory":"1Gi","storageGi":1025}}`,
			want: "kafka.storageGi",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newTestServer()
			request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(tc.body))
			recording := httptest.NewRecorder()
			server.ServeHTTP(recording, request)
			if recording.Code != http.StatusBadRequest || !strings.Contains(recording.Body.String(), tc.want) {
				t.Fatalf("invalid quantity status = %d, body=%s", recording.Code, recording.Body.String())
			}
		})
	}
}

func TestCreateRejectsOperatorOnlySpecFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "suspend",
			body: `{"name":"orders","spec":{"engine":"kafka","suspend":true}}`,
			want: "spec.suspend",
		},
		{
			name: "suspend_false",
			body: `{"name":"orders","spec":{"engine":"kafka","suspend":false}}`,
			want: "spec.suspend",
		},
		{
			name: "topology",
			body: `{"name":"orders","spec":{"engine":"kafka","topology":{"mode":"combined","replicas":1}}}`,
			want: "spec.topology",
		},
		{
			name: "external_listener",
			body: `{"name":"orders","spec":{"engine":"kafka","kafka":{"listeners":{"external":{"enabled":true,"type":"loadbalancer"}}}}}`,
			want: "external-access endpoint",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newTestServer()
			request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(tc.body))
			recording := httptest.NewRecorder()
			server.ServeHTTP(recording, request)
			if recording.Code != http.StatusBadRequest || !strings.Contains(recording.Body.String(), tc.want) {
				t.Fatalf("operator field status = %d, body=%s", recording.Code, recording.Body.String())
			}
		})
	}
}

func TestCreateRejectsStorageDeleteClaimFromBrowser(t *testing.T) {
	server, _ := newTestServer()
	body := `{"name":"orders","spec":{"engine":"kafka","storage":{"deleteClaim":false}}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusBadRequest || !strings.Contains(recording.Body.String(), "deletionPolicy") {
		t.Fatalf("deleteClaim status = %d, body=%s", recording.Code, recording.Body.String())
	}
}

func TestCreateRejectsUnsupportedKafkaVersion(t *testing.T) {
	for _, version := range []string{"3.7.2", "3.8.0", "4.0.1"} {
		t.Run(version, func(t *testing.T) {
			server, _ := newTestServer()
			body := `{"name":"orders","engine":"kafka","kafka":{"version":"` + version + `","brokers":1}}`
			request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
			recording := httptest.NewRecorder()
			server.ServeHTTP(recording, request)
			if recording.Code != http.StatusBadRequest {
				t.Fatalf("unsupported version status = %d, body=%s", recording.Code, recording.Body.String())
			}
		})
	}
}

func TestClientConfigIsSecretFree(t *testing.T) {
	server, store := newTestServer()
	seedReadyMessageQueue(t, store, "ns-test", "orders")

	request := httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders/client-config", nil)
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK {
		t.Fatalf("client-config status = %d, body=%s", recording.Code, recording.Body.String())
	}
	config := ClientConfigResponse{}
	if err := json.Unmarshal(recording.Body.Bytes(), &config); err != nil {
		t.Fatal(err)
	}
	if config.Namespace != "ns-test" || config.Username != "orders-client" || config.SecretRef != "orders-client" || len(config.BootstrapServers) != 1 {
		t.Fatalf("unexpected client config: %#v", config)
	}
	if config.BootstrapServers[0] != "orders-kafka-bootstrap.ns-test.svc:9093" || config.SecurityProtocol != "SASL_SSL" || config.CASecretRef != "orders-cluster-ca-cert" {
		t.Fatalf("unexpected secure client config: %#v", config)
	}
	body := strings.ToLower(recording.Body.String())
	for _, forbidden := range []string{"password", "privatekey", "kubeconfig", "secretdata", "\"data\""} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("client config contains sensitive field %q: %s", forbidden, recording.Body.String())
		}
	}
}

func TestClientCredentialsExplicitlyReturnsSecretMaterialWithNoStore(t *testing.T) {
	server, store := newTestServer()
	seedReadyMessageQueue(t, store, "ns-test", "orders")
	store.SetClientCredentials("ns-test", "orders", ClientCredentialsResponse{
		Password:      "alice-secret",
		CACertificate: "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n",
	})

	request := httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders/client-credentials", nil)
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK {
		t.Fatalf("client-credentials status = %d, body=%s", recording.Code, recording.Body.String())
	}
	if recording.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("credentials Cache-Control = %q, want no-store", recording.Header().Get("Cache-Control"))
	}
	var credentials ClientCredentialsResponse
	if err := json.Unmarshal(recording.Body.Bytes(), &credentials); err != nil {
		t.Fatal(err)
	}
	if credentials.Namespace != "ns-test" || credentials.Username != "orders-client" || credentials.Password != "alice-secret" || credentials.CACertificate == "" {
		t.Fatalf("unexpected credentials: %#v", credentials)
	}
	if credentials.SecretRef != "orders-client" || credentials.CASecretRef != "orders-cluster-ca-cert" || credentials.SecurityProtocol != "SASL_SSL" {
		t.Fatalf("unexpected credential metadata: %#v", credentials)
	}
}

func TestClientCredentialsDegradesBeforeSecretIsReady(t *testing.T) {
	server, store := newTestServer()
	seedReadyMessageQueue(t, store, "ns-test", "orders")

	request := httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders/client-credentials", nil)
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK {
		t.Fatalf("client-credentials status = %d, body=%s", recording.Code, recording.Body.String())
	}
	if !strings.Contains(recording.Body.String(), `"degraded":true`) || strings.Contains(recording.Body.String(), `"password"`) {
		t.Fatalf("degraded credentials response = %s", recording.Body.String())
	}
}

func TestClientConfigUsesOnlyObservedExternalEndpoints(t *testing.T) {
	server, store := newTestServer()
	seedReadyMessageQueue(t, store, "ns-test", "orders")
	store.mu.Lock()
	resource := store.items["ns-test"]["orders"]
	resource.Spec.Kafka.Listeners = &KafkaListeners{External: &ExternalListener{Enabled: true}}
	resource.Status.ExternalEndpoints = []string{"192.168.0.62:31234"}
	store.items["ns-test"]["orders"] = resource
	store.mu.Unlock()

	request := httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders/client-config", nil)
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK {
		t.Fatalf("client-config status = %d, body=%s", recording.Code, recording.Body.String())
	}
	var config ClientConfigResponse
	if err := json.Unmarshal(recording.Body.Bytes(), &config); err != nil {
		t.Fatal(err)
	}
	if len(config.ExternalBootstrapServers) != 1 || config.ExternalBootstrapServers[0] != "192.168.0.62:31234" {
		t.Fatalf("external bootstrap servers = %#v", config.ExternalBootstrapServers)
	}
	if strings.Contains(strings.ToLower(recording.Body.String()), "password") {
		t.Fatalf("client config contains secret material: %s", recording.Body.String())
	}
}

func TestDeleteUsesIdentityNamespace(t *testing.T) {
	server, store := newTestServer()
	seedReadyMessageQueue(t, store, "ns-test", "orders")
	seedReadyMessageQueue(t, store, "other-ns", "orders")

	request := httptest.NewRequest(http.MethodDelete, "/api/v1/messagequeues/orders", nil)
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body=%s", recording.Code, recording.Body.String())
	}
	if _, err := store.Get(context.Background(), "ns-test", "orders"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted resource still exists or wrong error: %v", err)
	}
	if _, err := store.Get(context.Background(), "other-ns", "orders"); err != nil {
		t.Fatalf("delete crossed identity namespace: %v", err)
	}
}

func TestLogsUseFixedContract(t *testing.T) {
	server, store := newTestServer()
	_, err := store.Create(context.Background(), "ns-test", MessageQueue{Metadata: Metadata{Name: "orders"}, Spec: MessageQueueSpec{Engine: "kafka"}})
	if err != nil {
		t.Fatal(err)
	}
	store.SetLogs("ns-test", "orders", LogResponse{Component: "broker", Lines: []LogLine{{Message: "ready"}, {Message: "listening"}}})

	request := httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders/logs?component=broker&tailLines=1", nil)
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK || !strings.Contains(recording.Body.String(), "listening") || strings.Contains(recording.Body.String(), "ready") {
		t.Fatalf("logs response = %d %s", recording.Code, recording.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders/logs?query=raw-logs-query", nil)
	recording = httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusBadRequest {
		t.Fatalf("raw logs query status = %d", recording.Code)
	}
}

func TestMetricsUseFixedKeysAndDoNotReturnQuery(t *testing.T) {
	server, _ := newTestServer()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders/metrics?key=broker_count", nil)
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusOK || !strings.Contains(recording.Body.String(), `"key":"broker_count"`) {
		t.Fatalf("metrics response = %d %s", recording.Code, recording.Body.String())
	}
	if strings.Contains(recording.Body.String(), "query") || strings.Contains(recording.Body.String(), "promql") {
		t.Fatalf("metrics response exposes query details: %s", recording.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/messagequeues/orders/metrics?key=raw_promql", nil)
	recording = httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusBadRequest {
		t.Fatalf("raw metrics query status = %d", recording.Code)
	}
}

func TestVictoriaMetricsProviderQueriesFixedRange(t *testing.T) {
	var gotPath string
	var gotValues url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotValues = r.URL.Query()
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1785808800,"0.25"],[1785808830,"0.5"]]}]}}`))
	}))
	defer server.Close()
	provider := &VictoriaMetricsProvider{
		BaseURL: server.URL,
		Client:  server.Client(),
		Now:     func() time.Time { return time.Unix(1785808830, 0) },
	}

	response, err := provider.Metrics(context.Background(), "ns-test", "orders", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if response.Degraded || response.Unit != "cores" || len(response.Values) != 2 || response.Values[1].Value != 0.5 {
		t.Fatalf("metrics response = %#v", response)
	}
	if gotPath != "/api/v1/query_range" || gotValues.Get("start") != "1785807030" || gotValues.Get("end") != "1785808830" || gotValues.Get("step") != "30" {
		t.Fatalf("query range = %s %#v", gotPath, gotValues)
	}
	query := gotValues.Get("query")
	for _, selector := range []string{`container_cpu_usage_seconds_total`, `namespace="ns-test"`, `pod=~"^orders-orders-pool-[0-9]+$"`, `container="kafka"`} {
		if !strings.Contains(query, selector) {
			t.Fatalf("fixed query %q does not contain %q", query, selector)
		}
	}
	encoded, _ := json.Marshal(response)
	if strings.Contains(string(encoded), "query") || strings.Contains(string(encoded), "promql") {
		t.Fatalf("response exposes private query: %s", encoded)
	}
}

func TestMetricsProviderFromEnvironment(t *testing.T) {
	t.Setenv("MESSAGEQUEUE_METRICS_URL", "")
	if _, ok := metricsProviderFromEnv().(UnavailableMetricsProvider); !ok {
		t.Fatal("missing metrics URL should use the degraded provider")
	}
	t.Setenv("MESSAGEQUEUE_METRICS_URL", "http://victoria-metrics.monitoring.svc/select/0/prometheus")
	if _, ok := metricsProviderFromEnv().(*VictoriaMetricsProvider); !ok {
		t.Fatal("configured metrics URL should use VictoriaMetrics")
	}
}

func TestFixedMetricQueriesUseExpectedMetricFamilies(t *testing.T) {
	wantFamilies := map[string]string{
		"broker_count":     "kafka_server_replicamanager_underreplicatedpartitions",
		"partition_health": "kafka_server_replicamanager_underreplicatedpartitions",
		"throughput":       "kafka_server_brokertopicmetrics_messagesin_total",
		"consumer_lag":     "kafka_consumergroup_lag",
		"cpu":              "container_cpu_usage_seconds_total",
		"memory":           "container_memory_working_set_bytes",
		"storage":          "kafka_log_log_size",
	}
	for key, family := range wantFamilies {
		query, ok := fixedMetricQuery(key, "ns-test", "orders")
		if !ok || !strings.Contains(query, family) || !strings.Contains(query, `namespace="ns-test"`) {
			t.Fatalf("query for %s = %q", key, query)
		}
	}
}

func TestVictoriaMetricsProviderDegradesEmptyInvalidAndProviderErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusCode int
		body       string
	}{
		{name: "empty", statusCode: http.StatusOK, body: `{"status":"success","data":{"resultType":"matrix","result":[]}}`},
		{name: "invalid", statusCode: http.StatusOK, body: `{"status":"success","data":{"resultType":"matrix","result":[{"values":[[1785808800,"NaN"]]}]}}`},
		{name: "provider_error", statusCode: http.StatusBadGateway, body: `private upstream error: raw promql details`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			provider := &VictoriaMetricsProvider{BaseURL: server.URL, Client: server.Client()}
			response, err := provider.Metrics(context.Background(), "ns-test", "orders", "throughput")
			if err != nil {
				t.Fatal(err)
			}
			if !response.Degraded || len(response.Values) != 0 || response.Message == "" {
				t.Fatalf("degraded response = %#v", response)
			}
			if strings.Contains(response.Message, "promql") || strings.Contains(response.Message, "upstream") {
				t.Fatalf("degraded response leaked provider details: %#v", response)
			}
		})
	}
}

func TestKubernetesStoreUsesNamespacePathAndNeverSecretPayload(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/messagequeues") {
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"orders","namespace":"wrong"},"spec":{"engine":"kafka"},"status":{"phase":"Ready","secretRefs":[{"name":"orders-user"}]}}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	store := &KubernetesStore{BaseURL: server.URL, Client: server.Client()}
	items, err := store.List(context.Background(), "ns-test")
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/apis/messagequeue.sealos.io/v1alpha1/namespaces/ns-test/messagequeues" {
		t.Fatalf("Kubernetes path = %s", gotPath)
	}
	encoded, _ := json.Marshal(viewOf(items[0], "ns-test"))
	if strings.Contains(string(encoded), "password") || strings.Contains(string(encoded), "secretData") {
		t.Fatalf("view contains secret payload: %s", encoded)
	}
}

func TestKubernetesStoreDeleteUsesNamespacePath(t *testing.T) {
	var gotMethod, gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	store := &KubernetesStore{BaseURL: server.URL, Client: server.Client()}
	if err := store.Delete(context.Background(), "ns-test", "orders"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/apis/messagequeue.sealos.io/v1alpha1/namespaces/ns-test/messagequeues/orders" {
		t.Fatalf("Kubernetes delete = %s %s", gotMethod, gotPath)
	}
}

func TestKubernetesStoreExternalAccessUsesNarrowMergePatch(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"metadata":{"name":"orders","generation":7},"spec":{"engine":"kafka","kafka":{"version":"3.9.0","replicas":3},"resources":{"cpu":"1","memory":"2Gi"},"storage":{"size":"20Gi"}}}`))
		case http.MethodPatch:
			if r.URL.Path != "/apis/messagequeue.sealos.io/v1alpha1/namespaces/ns-test/messagequeues/orders" {
				t.Fatalf("patch path = %s", r.URL.Path)
			}
			if r.Header.Get("Content-Type") != "application/merge-patch+json" {
				t.Fatalf("patch content type = %q", r.Header.Get("Content-Type"))
			}
			var patch map[string]any
			if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(patch)
			if string(encoded) != `{"spec":{"kafka":{"listeners":{"external":{"bootstrapAlternativeNames":[],"enabled":true,"preferredNodePortAddressType":"InternalIP","type":"nodeport"}}}}}` {
				t.Fatalf("external access patch = %s", encoded)
			}
			_, _ = w.Write([]byte(`{"metadata":{"name":"orders","generation":8},"spec":{"engine":"kafka","kafka":{"version":"3.9.0","replicas":3,"listeners":{"external":{"enabled":true}}},"resources":{"cpu":"1","memory":"2Gi"},"storage":{"size":"20Gi"}}}`))
		default:
			t.Fatalf("unexpected request method %s", r.Method)
		}
	}))
	defer server.Close()
	store := &KubernetesStore{BaseURL: server.URL, Client: server.Client()}
	updated, err := store.UpdateExternalAccess(context.Background(), "ns-test", "orders", ExternalListener{Enabled: true, Type: "nodeport", PreferredNodePortAddressType: "InternalIP"})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || updated.Metadata.Generation != 8 || updated.Spec.Resources.CPU != "1" || updated.Spec.Storage.Size != "20Gi" || updated.Spec.Kafka.Listeners == nil || !updated.Spec.Kafka.Listeners.External.Enabled {
		t.Fatalf("updated resource = %#v, requests=%d", updated, requests)
	}
}

func TestKubernetesStoreSuspensionUsesNarrowMergePatch(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"metadata":{"name":"orders","generation":7},"spec":{"engine":"kafka","kafka":{"version":"3.9.0","replicas":3},"resources":{"cpu":"1","memory":"2Gi"},"storage":{"size":"20Gi"}}}`))
		case http.MethodPatch:
			if r.URL.Path != "/apis/messagequeue.sealos.io/v1alpha1/namespaces/ns-test/messagequeues/orders" {
				t.Fatalf("patch path = %s", r.URL.Path)
			}
			if r.Header.Get("Content-Type") != "application/merge-patch+json" {
				t.Fatalf("patch content type = %q", r.Header.Get("Content-Type"))
			}
			var patch map[string]any
			if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(patch)
			if string(encoded) != `{"spec":{"suspend":true}}` {
				t.Fatalf("suspension patch = %s", encoded)
			}
			_, _ = w.Write([]byte(`{"metadata":{"name":"orders","generation":8},"spec":{"engine":"kafka","suspend":true,"kafka":{"version":"3.9.0","replicas":3},"resources":{"cpu":"1","memory":"2Gi"},"storage":{"size":"20Gi"}}}`))
		default:
			t.Fatalf("unexpected request method %s", r.Method)
		}
	}))
	defer server.Close()
	store := &KubernetesStore{BaseURL: server.URL, Client: server.Client()}
	updated, err := store.UpdateSuspension(context.Background(), "ns-test", "orders", true)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || updated.Metadata.Generation != 8 || !updated.Spec.Suspend || updated.Spec.Resources.CPU != "1" || updated.Spec.Storage.Size != "20Gi" {
		t.Fatalf("updated resource = %#v, requests=%d", updated, requests)
	}
}

func TestKubernetesStoreClientCredentialsReadsDerivedSecrets(t *testing.T) {
	var requested []string
	password := base64.StdEncoding.EncodeToString([]byte("alice-secret"))
	ca := base64.StdEncoding.EncodeToString([]byte("-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		switch r.URL.Path {
		case "/apis/messagequeue.sealos.io/v1alpha1/namespaces/ns-test/messagequeues/orders":
			_, _ = w.Write([]byte(`{"metadata":{"name":"orders","generation":7},"spec":{"engine":"kafka","kafka":{"version":"3.9.0","replicas":1}},"status":{"phase":"Ready","kafkaRef":"orders","clientSecretRef":"orders-client","endpoints":["orders-kafka-bootstrap.ns-test.svc:9092","orders-kafka-bootstrap.ns-test.svc:9093"],"externalEndpoints":["192.168.0.62:32501"]}}`))
		case "/api/v1/namespaces/ns-test/secrets/orders-client":
			_, _ = w.Write([]byte(`{"data":{"password":"` + password + `"}}`))
		case "/api/v1/namespaces/ns-test/secrets/orders-cluster-ca-cert":
			_, _ = w.Write([]byte(`{"data":{"ca.crt":"` + ca + `"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store := &KubernetesStore{BaseURL: server.URL, Client: server.Client()}
	response, err := store.ClientCredentials(context.Background(), "ns-test", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if response.Password != "alice-secret" || !strings.Contains(response.CACertificate, "BEGIN CERTIFICATE") {
		t.Fatalf("credentials response = %#v", response)
	}
	if len(response.BootstrapServers) != 1 || response.BootstrapServers[0] != "orders-kafka-bootstrap.ns-test.svc:9093" || response.ExternalBootstrapServers[0] != "192.168.0.62:32501" {
		t.Fatalf("bootstrap servers = %#v external=%#v", response.BootstrapServers, response.ExternalBootstrapServers)
	}
	want := []string{
		"/apis/messagequeue.sealos.io/v1alpha1/namespaces/ns-test/messagequeues/orders",
		"/api/v1/namespaces/ns-test/secrets/orders-client",
		"/api/v1/namespaces/ns-test/secrets/orders-cluster-ca-cert",
	}
	if strings.Join(requested, "\n") != strings.Join(want, "\n") {
		t.Fatalf("requested paths = %#v", requested)
	}
}

func TestKubernetesStoreLogsRequireMessageQueueExistence(t *testing.T) {
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		if strings.Contains(r.URL.Path, "/pods") {
			t.Fatalf("logs queried pods before proving MessageQueue exists: %s", r.URL.String())
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	store := &KubernetesStore{BaseURL: server.URL, Client: server.Client()}
	_, err := store.Logs(context.Background(), "ns-test", "orders", LogRequest{Component: "broker", TailLines: 10})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("logs missing resource error = %v", err)
	}
	if len(requested) != 1 || requested[0] != "/apis/messagequeue.sealos.io/v1alpha1/namespaces/ns-test/messagequeues/orders" {
		t.Fatalf("unexpected requests: %v", requested)
	}
}

func TestKubernetesStoreOperatorLogsDegradeWithoutPodLookup(t *testing.T) {
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		if strings.Contains(r.URL.Path, "/pods") {
			t.Fatalf("operator logs should not query workspace pods: %s", r.URL.String())
		}
		_, _ = w.Write([]byte(`{"metadata":{"name":"orders"},"spec":{"engine":"kafka"},"status":{"phase":"Ready"}}`))
	}))
	defer server.Close()

	store := &KubernetesStore{BaseURL: server.URL, Client: server.Client()}
	response, err := store.Logs(context.Background(), "ns-test", "orders", LogRequest{Component: "operator", TailLines: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Degraded || response.Message == "" {
		t.Fatalf("operator logs should degrade explicitly: %#v", response)
	}
	if len(requested) != 1 {
		t.Fatalf("unexpected requests: %v", requested)
	}
}
