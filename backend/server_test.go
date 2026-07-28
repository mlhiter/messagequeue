package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type staticIdentity Identity

func (i staticIdentity) Identity(_ context.Context, _ *http.Request) (Identity, error) {
	return validateIdentity(Identity(i))
}

func newTestServer() (*Server, *MemoryStore) {
	store := NewMemoryStore()
	return &Server{
		Store:       store,
		Metrics:     FixedMetricsProvider{Values: map[string]MetricResponse{"broker_count": {Unit: "count", Values: []MetricPoint{{Value: 1}}}}},
		Identity:    staticIdentity{Namespace: "ns-test", UserID: "user-1"},
		AllowCreate: true,
	}, store
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

func TestHealthAndReadinessDoNotRequireWorkspaceIdentity(t *testing.T) {
	server := &Server{}
	for _, path := range []string{"/healthz", "/readyz"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		recording := httptest.NewRecorder()
		server.ServeHTTP(recording, request)
		if recording.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", path, recording.Code)
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

func TestCreateDisabledRequiresAuthenticatedWorkspaceSession(t *testing.T) {
	server, _ := newTestServer()
	server.AllowCreate = false
	body := `{"name":"orders","spec":{"engine":"kafka","kafka":{"replicas":1},"resources":{"cpu":"1","memory":"2Gi"},"storage":{"size":"10Gi"}}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messagequeues", strings.NewReader(body))
	recording := httptest.NewRecorder()
	server.ServeHTTP(recording, request)
	if recording.Code != http.StatusForbidden {
		t.Fatalf("create disabled status = %d, body=%s", recording.Code, recording.Body.String())
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
	body := `{"name":"orders","engine":"kafka","kafka":{"version":"3.9.0","brokers":3,"storageGi":20,"storageClass":"fast"},"deletionPolicy":"retain","monitoring":{"enabled":true},"console":{"enabled":false}}`
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
	if created.Spec.Kafka.Replicas != 3 || created.Spec.Kafka.Version != "3.9.0" || created.Spec.Storage.Size != "20Gi" || created.Spec.Storage.ClassName != "fast" || created.Spec.DeletionPolicy != "Retain" {
		t.Fatalf("flat request translation = %#v", created.Spec)
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
