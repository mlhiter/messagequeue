package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
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
	body := strings.ToLower(recording.Body.String())
	for _, forbidden := range []string{"password", "privatekey", "kubeconfig", "secretdata", "\"data\""} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("client config contains sensitive field %q: %s", forbidden, recording.Body.String())
		}
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
