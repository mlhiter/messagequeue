package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	apiPrefix = "/api/v1/messagequeues"
	// The API deliberately caps log output. Large streams belong in the
	// platform log viewer, not in a management API response.
	maxLogLines = 5000
)

type contextKey string

const identityContextKey contextKey = "messagequeue.identity"

// Identity is populated by the authenticated server-side Sealos session
// adapter. Namespace is intentionally absent from request DTOs.
type Identity struct {
	UserID    string
	Namespace string
	Roles     []string
}

func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, identityContextKey, identity)
}

// ContextIdentityProvider is useful when the ingress/session adapter has
// already authenticated a request and put its identity in the context.
type ContextIdentityProvider struct{}

func (ContextIdentityProvider) Identity(ctx context.Context, _ *http.Request) (Identity, error) {
	identity, ok := ctx.Value(identityContextKey).(Identity)
	if !ok {
		return Identity{}, ErrUnauthenticated
	}
	return validateIdentity(identity)
}

// EnvIdentityProvider is an explicit server-side fallback for a single
// workspace deployment. It never reads namespace from a browser header/body.
// Multi-tenant deployments should use ContextIdentityProvider instead.
type EnvIdentityProvider struct {
	Namespace string
	UserID    string
}

func (p EnvIdentityProvider) Identity(_ context.Context, _ *http.Request) (Identity, error) {
	return validateIdentity(Identity{UserID: p.UserID, Namespace: p.Namespace})
}

func validateIdentity(identity Identity) (Identity, error) {
	identity.Namespace = strings.TrimSpace(identity.Namespace)
	identity.UserID = strings.TrimSpace(identity.UserID)
	if identity.Namespace == "" || !validDNSLabel(identity.Namespace) {
		return Identity{}, ErrUnauthenticated
	}
	if identity.UserID == "" {
		return Identity{}, ErrUnauthenticated
	}
	return identity, nil
}

func identityFromEnv() (Identity, error) {
	return validateIdentity(Identity{
		Namespace: os.Getenv("MESSAGEQUEUE_WORKSPACE_NAMESPACE"),
		UserID:    envOr("MESSAGEQUEUE_USER_ID", "messagequeue-backend"),
	})
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

type MessageQueueStore interface {
	List(context.Context, string) ([]MessageQueue, error)
	Create(context.Context, string, MessageQueue) (MessageQueue, error)
	Get(context.Context, string, string) (MessageQueue, error)
	Logs(context.Context, string, string, LogRequest) (LogResponse, error)
}

// MetricsProvider is deliberately keyed rather than query-based. Providers
// map these keys to server-owned queries and never expose the underlying query
// language to the client.
type MetricsProvider interface {
	Metrics(context.Context, string, string, string) (MetricResponse, error)
}

type Server struct {
	Store    MessageQueueStore
	Metrics  MetricsProvider
	Identity IdentityProvider
	Logger   *slog.Logger
	Now      func() time.Time
}

type IdentityProvider interface {
	Identity(context.Context, *http.Request) (Identity, error)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	if s.Store == nil || s.Identity == nil {
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "messagequeue backend is not configured")
		return
	}
	identity, err := s.Identity.Identity(r.Context(), r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "an authenticated workspace session is required")
		return
	}
	identity, err = validateIdentity(identity)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "an authenticated workspace session is required")
		return
	}
	ctx := WithIdentity(r.Context(), identity)
	r = r.WithContext(ctx)

	if r.URL.Path == apiPrefix || r.URL.Path == apiPrefix+"/" {
		if r.Method == http.MethodGet {
			s.list(w, r)
			return
		}
		if r.Method == http.MethodPost {
			s.create(w, r)
			return
		}
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
		return
	}
	if !strings.HasPrefix(r.URL.Path, apiPrefix+"/") {
		writeError(w, http.StatusNotFound, "not_found", "route not found")
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, apiPrefix+"/"), "/"), "/")
	if len(parts) == 0 || parts[0] == "" || !validDNSLabel(parts[0]) {
		writeError(w, http.StatusNotFound, "not_found", "messagequeue not found")
		return
	}
	name := parts[0]
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.detail(w, r, name)
		return
	}
	if len(parts) != 2 || r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	switch parts[1] {
	case "status":
		s.status(w, r, name)
	case "logs":
		s.logs(w, r, name)
	case "metrics":
		s.metrics(w, r, name)
	default:
		writeError(w, http.StatusNotFound, "not_found", "route not found")
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	identity := identityFromContext(r.Context())
	items, err := s.Store.List(r.Context(), identity.Namespace)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	views := make([]MessageQueueView, 0, len(items))
	for _, item := range items {
		views = append(views, viewOf(item, identity.Namespace))
	}
	writeJSON(w, http.StatusOK, ListResponse{Items: views})
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	identity := identityFromContext(r.Context())
	var request CreateRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must contain one JSON object")
		return
	}
	if err := request.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	resource := MessageQueue{
		APIVersion: "messagequeue.sealos.io/v1alpha1",
		Kind:       "MessageQueue",
		Metadata: Metadata{
			Name:      request.Name,
			Namespace: identity.Namespace,
		},
		Spec: request.ProductSpec(),
	}
	created, err := s.Store.Create(r.Context(), identity.Namespace, resource)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, viewOf(created, identity.Namespace))
}

func (s *Server) detail(w http.ResponseWriter, r *http.Request, name string) {
	item, err := s.Store.Get(r.Context(), identityFromContext(r.Context()).Namespace, name)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewOf(item, identityFromContext(r.Context()).Namespace))
}

func (s *Server) status(w http.ResponseWriter, r *http.Request, name string) {
	item, err := s.Store.Get(r.Context(), identityFromContext(r.Context()).Namespace, name)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewOf(item, identityFromContext(r.Context()).Namespace).Status)
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request, name string) {
	request, err := parseLogRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	response, err := s.Store.Logs(r.Context(), identityFromContext(r.Context()).Namespace, name, request)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request, name string) {
	key := r.URL.Query().Get("key")
	if !metricKeys[key] || len(r.URL.Query()) != 1 || len(r.URL.Query()["key"]) != 1 {
		writeError(w, http.StatusBadRequest, "invalid_request", "key must be one of the supported metric keys")
		return
	}
	if s.Metrics == nil {
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "metrics are not configured")
		return
	}
	response, err := s.Metrics.Metrics(r.Context(), identityFromContext(r.Context()).Namespace, name, key)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func parseLogRequest(r *http.Request) (LogRequest, error) {
	if len(r.URL.Query()) > 2 {
		return LogRequest{}, errors.New("only component and tailLines are supported")
	}
	for key := range r.URL.Query() {
		if key != "component" && key != "tailLines" {
			return LogRequest{}, errors.New("only component and tailLines are supported")
		}
		if len(r.URL.Query()[key]) != 1 {
			return LogRequest{}, errors.New("each log parameter may be supplied only once")
		}
	}
	component := r.URL.Query().Get("component")
	if component == "" {
		component = "broker"
	}
	if !logComponents[component] {
		return LogRequest{}, errors.New("component must be broker, controller, or operator")
	}
	tailLines := 200
	if raw := r.URL.Query().Get("tailLines"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxLogLines {
			return LogRequest{}, fmt.Errorf("tailLines must be between 1 and %d", maxLogLines)
		}
		tailLines = parsed
	}
	return LogRequest{Component: component, TailLines: tailLines}, nil
}

func identityFromContext(ctx context.Context) Identity {
	identity, _ := ctx.Value(identityContextKey).(Identity)
	return identity
}

func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "internal_error"
	message := "the request could not be completed"
	switch {
	case errors.Is(err, ErrNotFound):
		status, code, message = http.StatusNotFound, "not_found", "messagequeue not found"
	case errors.Is(err, ErrConflict):
		status, code, message = http.StatusConflict, "conflict", "messagequeue already exists"
	case errors.Is(err, ErrForbidden):
		status, code, message = http.StatusForbidden, "forbidden", "the workspace is not allowed to perform this operation"
	case errors.Is(err, ErrDependencyUnavailable):
		status, code, message = http.StatusServiceUnavailable, "dependency_unavailable", "the requested dependency is unavailable"
	case errors.Is(err, ErrInvalid):
		status, code, message = http.StatusBadRequest, "invalid_request", "the request is invalid"
	}
	writeError(w, status, code, message)
}

func methodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorResponse{Error: APIError{Code: code, Message: message}})
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	identity, err := identityFromEnv()
	if err != nil {
		logger.Error("backend cannot start without server-side workspace identity", "error", err)
		os.Exit(1)
	}
	store, err := NewInClusterStoreFromEnv()
	if err != nil {
		logger.Error("backend cannot connect to Kubernetes", "error", err)
		os.Exit(1)
	}
	server := &Server{
		Store:    store,
		Metrics:  UnavailableMetricsProvider{},
		Identity: EnvIdentityProvider{Namespace: identity.Namespace, UserID: identity.UserID},
		Logger:   logger,
		Now:      time.Now,
	}
	listen := envOr("MESSAGEQUEUE_LISTEN_ADDR", ":8080")
	logger.Info("messagequeue backend listening", "addr", listen, "namespace", identity.Namespace)
	httpServer := &http.Server{
		Addr:              listen,
		Handler:           server,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      35 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if err := httpServer.ListenAndServe(); err != nil {
		logger.Error("backend stopped", "error", err)
		os.Exit(1)
	}
}

func NewInClusterStoreFromEnv() (*KubernetesStore, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	if host == "" {
		return nil, errors.New("KUBERNETES_SERVICE_HOST is required")
	}
	port := envOr("KUBERNETES_SERVICE_PORT", "443")
	baseURL := "https://" + host + ":" + port
	tokenPath := envOr("MESSAGEQUEUE_SERVICE_ACCOUNT_TOKEN", "/var/run/secrets/kubernetes.io/serviceaccount/token")
	token, err := os.ReadFile(filepath.Clean(tokenPath))
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	caPath := envOr("MESSAGEQUEUE_SERVICE_ACCOUNT_CA", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	caData, err := os.ReadFile(filepath.Clean(caPath))
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caData) {
		return nil, errors.New("service account CA is not valid PEM")
	}
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		Timeout:   30 * time.Second,
	}
	return &KubernetesStore{BaseURL: baseURL, Token: strings.TrimSpace(string(token)), Client: client}, nil
}
