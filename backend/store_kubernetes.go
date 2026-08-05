package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

type KubernetesStore struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

func (s *KubernetesStore) access(ctx context.Context) KubernetesAccess {
	if access, ok := kubernetesAccessFromContext(ctx); ok {
		return access
	}
	return KubernetesAccess{BaseURL: s.BaseURL, Token: s.Token, Client: s.Client}
}

func (s *KubernetesStore) List(ctx context.Context, namespace string) ([]MessageQueue, error) {
	var response struct {
		Items []MessageQueue `json:"items"`
	}
	if err := s.requestJSON(ctx, http.MethodGet, resourcePath(namespace, ""), nil, &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func (s *KubernetesStore) Create(ctx context.Context, namespace string, resource MessageQueue) (MessageQueue, error) {
	resource.Metadata.Namespace = ""
	resource.Kind = "MessageQueue"
	resource.APIVersion = "messagequeue.sealos.io/v1alpha1"
	var created MessageQueue
	if err := s.requestJSON(ctx, http.MethodPost, resourcePath(namespace, ""), resource, &created); err != nil {
		return MessageQueue{}, err
	}
	return created, nil
}

func (s *KubernetesStore) Get(ctx context.Context, namespace, name string) (MessageQueue, error) {
	var resource MessageQueue
	if err := s.requestJSON(ctx, http.MethodGet, resourcePath(namespace, name), nil, &resource); err != nil {
		return MessageQueue{}, err
	}
	return resource, nil
}

func (s *KubernetesStore) UpdateExternalAccess(ctx context.Context, namespace, name string, listener ExternalListener) (MessageQueue, error) {
	listener = normalizeExternalListener(listener)
	resource, err := s.Get(ctx, namespace, name)
	if err != nil {
		return MessageQueue{}, err
	}
	if resource.Spec.Kafka.Listeners != nil && resource.Spec.Kafka.Listeners.External != nil && externalListenersEqual(*resource.Spec.Kafka.Listeners.External, listener) {
		return resource, nil
	}
	if !listener.Enabled && (resource.Spec.Kafka.Listeners == nil || resource.Spec.Kafka.Listeners.External == nil) {
		return resource, nil
	}

	patch := map[string]any{
		"spec": map[string]any{
			"kafka": map[string]any{
				"listeners": map[string]any{
					"external": map[string]any{
						"enabled":                      listener.Enabled,
						"type":                         listener.Type,
						"preferredNodePortAddressType": listener.PreferredNodePortAddressType,
						"bootstrapAlternativeNames":    listener.BootstrapAlternativeNames,
					},
				},
			},
		},
	}
	var updated MessageQueue
	if err := s.requestJSONWithContentType(ctx, http.MethodPatch, resourcePath(namespace, name), patch, &updated, "application/merge-patch+json"); err != nil {
		return MessageQueue{}, err
	}
	return updated, nil
}

func normalizeExternalListener(listener ExternalListener) ExternalListener {
	listener.BootstrapAlternativeNames = normalizedAlternativeNames(listener.BootstrapAlternativeNames)
	if listener.BootstrapAlternativeNames == nil {
		listener.BootstrapAlternativeNames = []string{}
	}
	return listener
}

func externalListenersEqual(left, right ExternalListener) bool {
	return reflect.DeepEqual(normalizeExternalListener(left), normalizeExternalListener(right))
}

func (s *KubernetesStore) UpdateSuspension(ctx context.Context, namespace, name string, suspended bool) (MessageQueue, error) {
	resource, err := s.Get(ctx, namespace, name)
	if err != nil {
		return MessageQueue{}, err
	}
	if resource.Spec.Suspend == suspended {
		return resource, nil
	}

	patch := map[string]any{
		"spec": map[string]any{
			"suspend": suspended,
		},
	}
	var updated MessageQueue
	if err := s.requestJSONWithContentType(ctx, http.MethodPatch, resourcePath(namespace, name), patch, &updated, "application/merge-patch+json"); err != nil {
		return MessageQueue{}, err
	}
	return updated, nil
}

func (s *KubernetesStore) ClientCredentials(ctx context.Context, namespace, name string) (ClientCredentialsResponse, error) {
	resource, err := s.Get(ctx, namespace, name)
	if err != nil {
		return ClientCredentialsResponse{}, err
	}
	config := clientConfigOf(viewOf(resource, namespace), namespace)
	response := ClientCredentialsResponse{
		Name:                     config.Name,
		Namespace:                config.Namespace,
		BootstrapServers:         append([]string(nil), config.BootstrapServers...),
		ExternalBootstrapServers: append([]string(nil), config.ExternalBootstrapServers...),
		Username:                 config.Username,
		SecretRef:                config.SecretRef,
		CASecretRef:              config.CASecretRef,
		Transport:                config.Transport,
		Mechanism:                config.Mechanism,
		SecurityProtocol:         config.SecurityProtocol,
		Degraded:                 config.Degraded,
		Message:                  config.Message,
	}
	if config.SecretRef == "" {
		response.Degraded = true
		response.Message = "client credentials are not available yet"
		return response, nil
	}

	password, err := s.secretDataValue(ctx, namespace, config.SecretRef, "password")
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			response.Degraded = true
			response.Message = "client credentials are not available yet"
			return response, nil
		}
		return ClientCredentialsResponse{}, err
	}
	if strings.TrimSpace(password) == "" {
		response.Degraded = true
		response.Message = "client credentials are not available yet"
		return response, nil
	}
	response.Password = password

	if config.CASecretRef != "" {
		ca, err := s.secretDataValue(ctx, namespace, config.CASecretRef, "ca.crt")
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				return ClientCredentialsResponse{}, err
			}
			response.Degraded = true
			response.Message = "client CA certificate is not available yet"
		} else {
			response.CACertificate = ca
		}
	}
	return response, nil
}

func (s *KubernetesStore) Delete(ctx context.Context, namespace, name string) error {
	return s.requestJSON(ctx, http.MethodDelete, resourcePath(namespace, name), nil, nil)
}

func (s *KubernetesStore) Logs(ctx context.Context, namespace, name string, request LogRequest) (LogResponse, error) {
	if !logComponents[request.Component] {
		return LogResponse{}, ErrInvalid
	}
	if _, err := s.Get(ctx, namespace, name); err != nil {
		return LogResponse{}, err
	}
	if request.Component == "operator" {
		return LogResponse{Name: name, Component: request.Component, Lines: []LogLine{}, Degraded: true, Message: "operator logs require a system log adapter"}, nil
	}
	// The selector is generated from the authenticated namespace and route
	// resource name. Clients cannot select another pod or namespace.
	selector := "strimzi.io/cluster=" + name
	if request.Component == "controller" {
		selector = "app.kubernetes.io/part-of=messagequeue,app.kubernetes.io/instance=" + name
	}
	var pods struct {
		Items []struct {
			Metadata Metadata `json:"metadata"`
			Spec     struct {
				Containers []struct {
					Name string `json:"name"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"items"`
	}
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods?labelSelector=" + url.QueryEscape(selector)
	if err := s.requestJSON(ctx, http.MethodGet, path, nil, &pods); err != nil {
		return LogResponse{}, err
	}
	if len(pods.Items) == 0 {
		return LogResponse{Name: name, Component: request.Component, Lines: []LogLine{}, Degraded: true, Message: "logs are not available yet"}, nil
	}
	pod := pods.Items[0]
	container := request.Component
	if len(pod.Spec.Containers) > 0 {
		container = pod.Spec.Containers[0].Name
	}
	logsPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods/" + url.PathEscape(pod.Metadata.Name) + "/log?container=" + url.QueryEscape(container) + "&tailLines=" + strconv.Itoa(request.TailLines)
	body, err := s.requestText(ctx, http.MethodGet, logsPath)
	if err != nil {
		return LogResponse{}, err
	}
	lines := make([]LogLine, 0)
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if line != "" {
			lines = append(lines, LogLine{Message: line})
		}
	}
	return LogResponse{Name: name, Component: request.Component, Lines: lines}, nil
}

func resourcePath(namespace, name string) string {
	base := "/apis/messagequeue.sealos.io/v1alpha1/namespaces/" + url.PathEscape(namespace) + "/messagequeues"
	if name != "" {
		base += "/" + url.PathEscape(name)
	}
	return base
}

func secretPath(namespace, name string) string {
	return "/api/v1/namespaces/" + url.PathEscape(namespace) + "/secrets/" + url.PathEscape(name)
}

func (s *KubernetesStore) secretDataValue(ctx context.Context, namespace, name, key string) (string, error) {
	var secret struct {
		Data map[string]string `json:"data"`
	}
	if err := s.requestJSON(ctx, http.MethodGet, secretPath(namespace, name), nil, &secret); err != nil {
		return "", err
	}
	encoded := secret.Data[key]
	if encoded == "" {
		return "", nil
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", ErrDependencyUnavailable
	}
	return string(decoded), nil
}

func (s *KubernetesStore) requestJSON(ctx context.Context, method, path string, payload any, output any) error {
	return s.requestJSONWithContentType(ctx, method, path, payload, output, "application/json")
}

func (s *KubernetesStore) requestJSONWithContentType(ctx context.Context, method, path string, payload any, output any, contentType string) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode Kubernetes request: %w", err)
		}
		body = strings.NewReader(string(encoded))
	}
	access := s.access(ctx)
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(access.BaseURL, "/")+path, body)
	if err != nil {
		return fmt.Errorf("create Kubernetes request: %w", err)
	}
	if access.Token != "" {
		request.Header.Set("Authorization", "Bearer "+access.Token)
	}
	if payload != nil {
		request.Header.Set("Content-Type", contentType)
	}
	client := access.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("Kubernetes request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return kubernetesError(response)
	}
	if output == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(output); err != nil {
		return fmt.Errorf("decode Kubernetes response: %w", err)
	}
	return nil
}

func (s *KubernetesStore) requestText(ctx context.Context, method, path string) (string, error) {
	access := s.access(ctx)
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(access.BaseURL, "/")+path, nil)
	if err != nil {
		return "", err
	}
	if access.Token != "" {
		request.Header.Set("Authorization", "Bearer "+access.Token)
	}
	client := access.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("Kubernetes request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", kubernetesError(response)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func kubernetesError(response *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(response.Body, 16<<10))
	message := strings.TrimSpace(string(data))
	switch response.StatusCode {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, message)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", ErrConflict, message)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", ErrForbidden, message)
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		return fmt.Errorf("%w: %s", ErrDependencyUnavailable, message)
	default:
		if message == "" {
			message = response.Status
		}
		return errors.New(message)
	}
}
