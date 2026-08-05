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
	"net"
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

func optionalIdentityFromEnv() (Identity, bool, error) {
	namespace := strings.TrimSpace(os.Getenv("MESSAGEQUEUE_WORKSPACE_NAMESPACE"))
	if namespace == "" {
		return Identity{}, false, nil
	}
	identity, err := validateIdentity(Identity{
		Namespace: namespace,
		UserID:    envOr("MESSAGEQUEUE_USER_ID", "messagequeue-backend"),
	})
	return identity, true, err
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
	UpdateExternalAccess(context.Context, string, string, ExternalListener) (MessageQueue, error)
	UpdateSuspension(context.Context, string, string, bool) (MessageQueue, error)
	ClientCredentials(context.Context, string, string) (ClientCredentialsResponse, error)
	Delete(context.Context, string, string) error
	Logs(context.Context, string, string, LogRequest) (LogResponse, error)
}

// MetricsProvider is deliberately keyed rather than query-based. Providers
// map these keys to server-owned queries and never expose the underlying query
// language to the client.
type MetricsProvider interface {
	Metrics(context.Context, string, string, string) (MetricResponse, error)
}

type Server struct {
	Store          MessageQueueStore
	Metrics        MetricsProvider
	Quota          QuotaProvider
	Identity       IdentityProvider
	ExternalAccess ExternalAccessConfig
	Logger         *slog.Logger
	Now            func() time.Time
}

type ExternalAccessConfig struct {
	ListenerType                 string
	PreferredNodePortAddressType string
	BootstrapAlternativeNames    []string
}

func externalAccessConfigFromEnv() (ExternalAccessConfig, error) {
	config := ExternalAccessConfig{
		ListenerType:                 strings.ToLower(envOr("MESSAGEQUEUE_EXTERNAL_LISTENER_TYPE", "nodeport")),
		PreferredNodePortAddressType: envOr("MESSAGEQUEUE_EXTERNAL_NODE_ADDRESS_TYPE", "InternalIP"),
		BootstrapAlternativeNames:    splitCSV(os.Getenv("MESSAGEQUEUE_EXTERNAL_BOOTSTRAP_ALTERNATIVE_NAMES")),
	}
	return config.normalized()
}

func (c ExternalAccessConfig) normalized() (ExternalAccessConfig, error) {
	if strings.TrimSpace(c.ListenerType) == "" {
		c.ListenerType = "nodeport"
	}
	if strings.TrimSpace(c.PreferredNodePortAddressType) == "" {
		c.PreferredNodePortAddressType = "InternalIP"
	}
	c.ListenerType = strings.ToLower(strings.TrimSpace(c.ListenerType))
	c.PreferredNodePortAddressType = strings.TrimSpace(c.PreferredNodePortAddressType)
	c.BootstrapAlternativeNames = normalizedAlternativeNames(c.BootstrapAlternativeNames)
	if c.ListenerType != "nodeport" {
		return ExternalAccessConfig{}, errors.New("external listener type must be nodeport")
	}
	validAddressTypes := map[string]bool{
		"ExternalDNS": true,
		"ExternalIP":  true,
		"Hostname":    true,
		"InternalDNS": true,
		"InternalIP":  true,
	}
	if !validAddressTypes[c.PreferredNodePortAddressType] {
		return ExternalAccessConfig{}, errors.New("external node address type is invalid")
	}
	for _, name := range c.BootstrapAlternativeNames {
		if net.ParseIP(name) == nil && !validDNSSubdomain(name) {
			return ExternalAccessConfig{}, errors.New("external bootstrap alternative names must be IP addresses or DNS names")
		}
	}
	return c, nil
}

func (c ExternalAccessConfig) listener(enabled bool) (ExternalListener, error) {
	normalized, err := c.normalized()
	if err != nil {
		return ExternalListener{}, err
	}
	return ExternalListener{
		Enabled:                      enabled,
		Type:                         normalized.ListenerType,
		PreferredNodePortAddressType: normalized.PreferredNodePortAddressType,
		BootstrapAlternativeNames:    append([]string(nil), normalized.BootstrapAlternativeNames...),
	}, nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		if item := strings.TrimSpace(part); item != "" {
			items = append(items, item)
		}
	}
	return items
}

func normalizedAlternativeNames(values []string) []string {
	seen := map[string]bool{}
	items := make([]string, 0, len(values))
	for _, value := range values {
		item := strings.TrimSpace(value)
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		items = append(items, item)
	}
	return items
}

type IdentityProvider interface {
	Identity(context.Context, *http.Request) (Identity, error)
}

type KubernetesContextProvider interface {
	KubernetesContext(context.Context, *http.Request) (context.Context, error)
}

type publicError struct {
	err     error
	status  int
	code    string
	message string
}

func (e publicError) Error() string {
	return e.err.Error() + ": " + e.message
}

func (e publicError) Unwrap() error {
	return e.err
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, map[string]string{"service": "messagequeue", "status": "ok"})
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
	if provider, ok := s.Identity.(KubernetesContextProvider); ok {
		ctx, err = provider.KubernetesContext(ctx, r)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "an authenticated workspace session is required")
			return
		}
	}
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
	tail := strings.Trim(strings.TrimPrefix(r.URL.Path, apiPrefix+"/"), "/")
	if tail == "-/quota" {
		if r.Method == http.MethodGet {
			s.quota(w, r)
			return
		}
		methodNotAllowed(w, http.MethodGet)
		return
	}
	parts := strings.Split(tail, "/")
	if len(parts) == 0 || parts[0] == "" || !validDNSLabel(parts[0]) {
		writeError(w, http.StatusNotFound, "not_found", "messagequeue not found")
		return
	}
	name := parts[0]
	if len(parts) == 1 {
		if r.Method == http.MethodGet {
			s.detail(w, r, name)
			return
		}
		if r.Method == http.MethodDelete {
			s.delete(w, r, name)
			return
		}
		methodNotAllowed(w, http.MethodGet, http.MethodDelete)
		return
	}
	if len(parts) == 2 && parts[1] == "external-access" {
		if r.Method == http.MethodPut {
			s.externalAccess(w, r, name)
			return
		}
		methodNotAllowed(w, http.MethodPut)
		return
	}
	if len(parts) == 2 && parts[1] == "suspension" {
		if r.Method == http.MethodPut {
			s.suspension(w, r, name)
			return
		}
		methodNotAllowed(w, http.MethodPut)
		return
	}
	if len(parts) == 2 && parts[1] == "client-credentials" {
		if r.Method == http.MethodGet {
			s.clientCredentials(w, r, name)
			return
		}
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if len(parts) != 2 || r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	switch parts[1] {
	case "status":
		s.status(w, r, name)
	case "client-config":
		s.clientConfig(w, r, name)
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
	spec := request.ProductSpec()
	if err := s.preflightCreate(r.Context(), identity.Namespace, spec); err != nil {
		s.writeStoreError(w, err)
		return
	}
	resource := MessageQueue{
		APIVersion: "messagequeue.sealos.io/v1alpha1",
		Kind:       "MessageQueue",
		Metadata: Metadata{
			Name:      request.Name,
			Namespace: identity.Namespace,
		},
		Spec: spec,
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

func (s *Server) delete(w http.ResponseWriter, r *http.Request, name string) {
	identity := identityFromContext(r.Context())
	if err := s.Store.Delete(r.Context(), identity.Namespace, name); err != nil {
		s.writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) externalAccess(w http.ResponseWriter, r *http.Request, name string) {
	request, err := decodeExternalAccessRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must contain exactly one boolean enabled field")
		return
	}

	identity := identityFromContext(r.Context())
	listener, err := s.ExternalAccess.listener(*request.Enabled)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "external access is not configured")
		return
	}
	updated, err := s.Store.UpdateExternalAccess(r.Context(), identity.Namespace, name, listener)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewOf(updated, identity.Namespace))
}

func (s *Server) suspension(w http.ResponseWriter, r *http.Request, name string) {
	request, err := decodeSuspensionRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must contain exactly one boolean suspended field")
		return
	}

	identity := identityFromContext(r.Context())
	updated, err := s.Store.UpdateSuspension(r.Context(), identity.Namespace, name, *request.Suspended)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewOf(updated, identity.Namespace))
}

func decodeExternalAccessRequest(body io.Reader) (ExternalAccessRequest, error) {
	return decodeBooleanBody[ExternalAccessRequest](body, "enabled", func(value bool) ExternalAccessRequest {
		return ExternalAccessRequest{Enabled: &value}
	}, func(request ExternalAccessRequest) bool { return request.Enabled != nil })
}

func decodeSuspensionRequest(body io.Reader) (SuspensionRequest, error) {
	return decodeBooleanBody[SuspensionRequest](body, "suspended", func(value bool) SuspensionRequest {
		return SuspensionRequest{Suspended: &value}
	}, func(request SuspensionRequest) bool { return request.Suspended != nil })
}

func decodeBooleanBody[T any](body io.Reader, field string, assign func(bool) T, hasValue func(T) bool) (T, error) {
	decoder := json.NewDecoder(io.LimitReader(body, 1<<20))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		var zero T
		return zero, ErrInvalid
	}
	var request T
	for decoder.More() {
		rawKey, err := decoder.Token()
		if err != nil {
			var zero T
			return zero, ErrInvalid
		}
		key, ok := rawKey.(string)
		if !ok || key != field || hasValue(request) {
			var zero T
			return zero, ErrInvalid
		}
		var rawEnabled json.RawMessage
		if err := decoder.Decode(&rawEnabled); err != nil {
			var zero T
			return zero, ErrInvalid
		}
		var enabled bool
		switch strings.TrimSpace(string(rawEnabled)) {
		case "true":
			enabled = true
		case "false":
			enabled = false
		default:
			var zero T
			return zero, ErrInvalid
		}
		request = assign(enabled)
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') || !hasValue(request) {
		var zero T
		return zero, ErrInvalid
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		var zero T
		return zero, ErrInvalid
	}
	return request, nil
}

func (s *Server) status(w http.ResponseWriter, r *http.Request, name string) {
	item, err := s.Store.Get(r.Context(), identityFromContext(r.Context()).Namespace, name)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewOf(item, identityFromContext(r.Context()).Namespace).Status)
}

func (s *Server) clientConfig(w http.ResponseWriter, r *http.Request, name string) {
	identity := identityFromContext(r.Context())
	item, err := s.Store.Get(r.Context(), identity.Namespace, name)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, clientConfigOf(viewOf(item, identity.Namespace), identity.Namespace))
}

func clientConfigOf(view MessageQueueView, namespace string) ClientConfigResponse {
	response := ClientConfigResponse{
		Name:                     view.Name,
		Namespace:                namespace,
		BootstrapServers:         preferredTLSBootstrapServers(view.Status.Endpoints),
		ExternalBootstrapServers: append([]string{}, view.Status.ExternalEndpoints...),
		Username:                 view.Name + "-client",
		SecretRef:                view.Status.ClientSecretRef,
		CASecretRef:              caSecretRefFor(view),
		Transport:                "TLS",
		Mechanism:                "SCRAM-SHA-512",
		SecurityProtocol:         "SASL_SSL",
	}
	if len(response.BootstrapServers) == 0 && view.Status.Endpoint != "" {
		response.BootstrapServers = []string{view.Status.Endpoint}
	}
	if len(response.BootstrapServers) == 0 || response.SecretRef == "" {
		response.Degraded = true
		response.Message = "client configuration is not available yet"
	}
	return response
}

func preferredTLSBootstrapServers(endpoints []string) []string {
	if len(endpoints) == 0 {
		return nil
	}
	tlsEndpoints := make([]string, 0, len(endpoints))
	otherEndpoints := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		trimmed := strings.TrimSpace(endpoint)
		if trimmed == "" {
			continue
		}
		if strings.HasSuffix(trimmed, ":9093") {
			tlsEndpoints = append(tlsEndpoints, trimmed)
		} else {
			otherEndpoints = append(otherEndpoints, trimmed)
		}
	}
	if len(tlsEndpoints) > 0 {
		return tlsEndpoints
	}
	return otherEndpoints
}

func caSecretRefFor(view MessageQueueView) string {
	kafkaName := strings.TrimSpace(view.Status.KafkaRef)
	if kafkaName == "" {
		kafkaName = view.Name
	}
	if kafkaName == "" {
		return ""
	}
	return kafkaName + "-cluster-ca-cert"
}

func (s *Server) clientCredentials(w http.ResponseWriter, r *http.Request, name string) {
	identity := identityFromContext(r.Context())
	response, err := s.Store.ClientCredentials(r.Context(), identity.Namespace, name)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, response)
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
	var p publicError
	if errors.As(err, &p) {
		writeError(w, p.status, p.code, p.message)
		return
	}
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
	case errors.Is(err, ErrQuotaExceeded):
		status, code, message = http.StatusForbidden, "quota_exceeded", "workspace quota is not sufficient for this instance"
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
	fallbackIdentity, hasFallbackIdentity, err := optionalIdentityFromEnv()
	if err != nil {
		logger.Error("backend cannot start with invalid fallback workspace identity", "error", err)
		os.Exit(1)
	}
	store, err := NewInClusterStoreFromEnv()
	if err != nil {
		logger.Error("backend cannot connect to Kubernetes", "error", err)
		os.Exit(1)
	}
	externalAccess, err := externalAccessConfigFromEnv()
	if err != nil {
		logger.Error("backend cannot start with invalid external access configuration", "error", err)
		os.Exit(1)
	}
	var fallbackProvider IdentityProvider
	if hasFallbackIdentity {
		fallbackProvider = EnvIdentityProvider{Namespace: fallbackIdentity.Namespace, UserID: fallbackIdentity.UserID}
	}
	server := &Server{
		Store:          store,
		Metrics:        metricsProviderFromEnv(),
		Quota:          store,
		Identity:       KubeconfigIdentityProvider{Fallback: fallbackProvider},
		ExternalAccess: externalAccess,
		Logger:         logger,
		Now:            time.Now,
	}
	listen := envOr("MESSAGEQUEUE_LISTEN_ADDR", ":8080")
	logger.Info("messagequeue backend listening", "addr", listen, "fallbackWorkspace", hasFallbackIdentity)
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
