package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type KubernetesStore struct {
	BaseURL string
	Token   string
	Client  *http.Client
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

func (s *KubernetesStore) Logs(ctx context.Context, namespace, name string, request LogRequest) (LogResponse, error) {
	if !logComponents[request.Component] {
		return LogResponse{}, ErrInvalid
	}
	// The selector is generated from the authenticated namespace and route
	// resource name. Clients cannot select another pod or namespace.
	selector := "strimzi.io/cluster=" + name
	if request.Component == "controller" {
		selector = "app.kubernetes.io/part-of=messagequeue,app.kubernetes.io/instance=" + name
	}
	if request.Component == "operator" {
		selector = "app.kubernetes.io/part-of=strimzi"
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

func (s *KubernetesStore) requestJSON(ctx context.Context, method, path string, payload any, output any) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode Kubernetes request: %w", err)
		}
		body = strings.NewReader(string(encoded))
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.BaseURL, "/")+path, body)
	if err != nil {
		return fmt.Errorf("create Kubernetes request: %w", err)
	}
	if s.Token != "" {
		request.Header.Set("Authorization", "Bearer "+s.Token)
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	client := s.Client
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
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.BaseURL, "/")+path, nil)
	if err != nil {
		return "", err
	}
	if s.Token != "" {
		request.Header.Set("Authorization", "Bearer "+s.Token)
	}
	client := s.Client
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
