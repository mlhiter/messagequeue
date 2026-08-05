package main

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore is used by API tests and local contract development. It models
// the Kubernetes store boundary; it is never wired by the production main.
type MemoryStore struct {
	mu          sync.RWMutex
	items       map[string]map[string]MessageQueue
	logData     map[string]LogResponse
	credentials map[string]ClientCredentialsResponse
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		items:       make(map[string]map[string]MessageQueue),
		logData:     make(map[string]LogResponse),
		credentials: make(map[string]ClientCredentialsResponse),
	}
}

func (s *MemoryStore) List(_ context.Context, namespace string) ([]MessageQueue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]MessageQueue, 0, len(s.items[namespace]))
	for _, item := range s.items[namespace] {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Metadata.Name < items[j].Metadata.Name })
	return items, nil
}

func (s *MemoryStore) Create(_ context.Context, namespace string, resource MessageQueue) (MessageQueue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items[namespace] == nil {
		s.items[namespace] = make(map[string]MessageQueue)
	}
	if _, exists := s.items[namespace][resource.Metadata.Name]; exists {
		return MessageQueue{}, ErrConflict
	}
	resource.Metadata.Namespace = namespace
	resource.Metadata.Generation = 1
	resource.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
	resource.Status = MessageQueueStatus{Phase: "Pending"}
	s.items[namespace][resource.Metadata.Name] = resource
	return resource, nil
}

func (s *MemoryStore) Get(_ context.Context, namespace, name string) (MessageQueue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	resource, ok := s.items[namespace][name]
	if !ok {
		return MessageQueue{}, ErrNotFound
	}
	return resource, nil
}

func (s *MemoryStore) UpdateExternalAccess(_ context.Context, namespace, name string, listener ExternalListener) (MessageQueue, error) {
	listener = normalizeExternalListener(listener)
	s.mu.Lock()
	defer s.mu.Unlock()
	resource, ok := s.items[namespace][name]
	if !ok {
		return MessageQueue{}, ErrNotFound
	}
	if resource.Spec.Kafka.Listeners != nil && resource.Spec.Kafka.Listeners.External != nil && externalListenersEqual(*resource.Spec.Kafka.Listeners.External, listener) {
		return resource, nil
	}
	if !listener.Enabled && (resource.Spec.Kafka.Listeners == nil || resource.Spec.Kafka.Listeners.External == nil) {
		return resource, nil
	}
	listeners := KafkaListeners{}
	if resource.Spec.Kafka.Listeners != nil {
		listeners = *resource.Spec.Kafka.Listeners
	}
	listeners.External = &listener
	resource.Spec.Kafka.Listeners = &listeners
	resource.Metadata.Generation++
	s.items[namespace][name] = resource
	return resource, nil
}

func (s *MemoryStore) UpdateSuspension(_ context.Context, namespace, name string, suspended bool) (MessageQueue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resource, ok := s.items[namespace][name]
	if !ok {
		return MessageQueue{}, ErrNotFound
	}
	if resource.Spec.Suspend == suspended {
		return resource, nil
	}
	resource.Spec.Suspend = suspended
	resource.Metadata.Generation++
	s.items[namespace][name] = resource
	return resource, nil
}

func (s *MemoryStore) ClientCredentials(_ context.Context, namespace, name string) (ClientCredentialsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	resource, ok := s.items[namespace][name]
	if !ok {
		return ClientCredentialsResponse{}, ErrNotFound
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
		Degraded:                 true,
		Message:                  "client credentials are not available yet",
	}
	if stored, ok := s.credentials[namespace+"/"+name]; ok {
		stored.Name = response.Name
		stored.Namespace = response.Namespace
		stored.BootstrapServers = append([]string(nil), response.BootstrapServers...)
		stored.ExternalBootstrapServers = append([]string(nil), response.ExternalBootstrapServers...)
		stored.Username = response.Username
		stored.SecretRef = response.SecretRef
		stored.CASecretRef = response.CASecretRef
		stored.Transport = response.Transport
		stored.Mechanism = response.Mechanism
		stored.SecurityProtocol = response.SecurityProtocol
		return stored, nil
	}
	return response, nil
}

func (s *MemoryStore) Delete(_ context.Context, namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[namespace][name]; !ok {
		return ErrNotFound
	}
	delete(s.items[namespace], name)
	return nil
}

func (s *MemoryStore) Logs(_ context.Context, namespace, name string, request LogRequest) (LogResponse, error) {
	if _, err := s.Get(context.Background(), namespace, name); err != nil {
		return LogResponse{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := namespace + "/" + name + "/" + request.Component
	response, ok := s.logData[key]
	if !ok {
		return LogResponse{Name: name, Component: request.Component, Lines: []LogLine{}, Degraded: true, Message: "logs are not available yet"}, nil
	}
	if len(response.Lines) > request.TailLines {
		response.Lines = response.Lines[len(response.Lines)-request.TailLines:]
	}
	return response, nil
}

func (s *MemoryStore) SetLogs(namespace, name string, response LogResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.logData == nil {
		s.logData = make(map[string]LogResponse)
	}
	s.logData[namespace+"/"+name+"/"+response.Component] = response
}

func (s *MemoryStore) SetClientCredentials(namespace, name string, response ClientCredentialsResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.credentials == nil {
		s.credentials = make(map[string]ClientCredentialsResponse)
	}
	s.credentials[namespace+"/"+name] = response
}

type UnavailableMetricsProvider struct{}

func (UnavailableMetricsProvider) Metrics(_ context.Context, _ string, name, key string) (MetricResponse, error) {
	return MetricResponse{Name: name, Key: key, Values: []MetricPoint{}, Degraded: true, Message: "metrics provider is not configured"}, nil
}
