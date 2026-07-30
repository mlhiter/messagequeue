package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrUnauthenticated       = errors.New("unauthenticated")
	ErrNotFound              = errors.New("not found")
	ErrConflict              = errors.New("conflict")
	ErrForbidden             = errors.New("forbidden")
	ErrDependencyUnavailable = errors.New("dependency unavailable")
	ErrInvalid               = errors.New("invalid request")
)

var dnsLabelPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func validDNSLabel(value string) bool {
	return len(value) <= 63 && value != "" && dnsLabelPattern.MatchString(value)
}

var (
	logComponents = map[string]bool{
		"broker":     true,
		"controller": true,
		"operator":   true,
	}
	metricKeys = map[string]bool{
		"broker_count":     true,
		"partition_health": true,
		"throughput":       true,
		"consumer_lag":     true,
		"cpu":              true,
		"memory":           true,
		"storage":          true,
	}
	supportedKafkaVersions = map[string]bool{
		"3.9.0": true,
		"4.0.0": true,
	}
)

type Metadata struct {
	Name               string `json:"name"`
	Namespace          string `json:"namespace,omitempty"`
	Generation         int64  `json:"generation,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	CreationTimestamp  string `json:"creationTimestamp,omitempty"`
}

type MessageQueue struct {
	APIVersion string             `json:"apiVersion,omitempty"`
	Kind       string             `json:"kind,omitempty"`
	Metadata   Metadata           `json:"metadata"`
	Spec       MessageQueueSpec   `json:"spec"`
	Status     MessageQueueStatus `json:"status,omitempty"`
}

type MessageQueueSpec struct {
	Engine         string       `json:"engine,omitempty"`
	Kafka          KafkaSpec    `json:"kafka,omitempty"`
	Version        string       `json:"version,omitempty"`
	Replicas       int32        `json:"replicas,omitempty"`
	Topology       TopologySpec `json:"topology,omitempty"`
	Resources      ResourceSpec `json:"resources,omitempty"`
	Storage        StorageSpec  `json:"storage,omitempty"`
	DeletionPolicy string       `json:"deletionPolicy,omitempty"`
	Suspend        bool         `json:"suspend,omitempty"`
}

type KafkaSpec struct {
	Version  string `json:"version,omitempty"`
	Replicas int32  `json:"replicas,omitempty"`
}

type TopologySpec struct {
	Mode     string `json:"mode,omitempty"`
	Replicas int32  `json:"replicas,omitempty"`
}

type ResourceSpec struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

type StorageSpec struct {
	Size        string `json:"size,omitempty"`
	ClassName   string `json:"className,omitempty"`
	DeleteClaim *bool  `json:"deleteClaim,omitempty"`
}

type MessageQueueStatus struct {
	Phase              string         `json:"phase,omitempty"`
	ObservedGeneration int64          `json:"observedGeneration,omitempty"`
	KafkaRef           string         `json:"kafkaRef,omitempty"`
	NodePoolRef        string         `json:"nodePoolRef,omitempty"`
	ClientSecretRef    string         `json:"clientSecretRef,omitempty"`
	Endpoints          []string       `json:"endpoints,omitempty"`
	ReadyReplicas      int32          `json:"readyReplicas,omitempty"`
	Message            string         `json:"message,omitempty"`
	Endpoint           string         `json:"endpoint,omitempty"`
	Conditions         []Condition    `json:"conditions,omitempty"`
	Topology           TopologyStatus `json:"topology,omitempty"`
	LastTransitionTime string         `json:"lastTransitionTime,omitempty"`
}

type Condition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

type TopologyStatus struct {
	Brokers int32 `json:"brokers,omitempty"`
}

type CreateRequest struct {
	Name           string              `json:"name"`
	Spec           MessageQueueSpec    `json:"spec"`
	Engine         string              `json:"engine,omitempty"`
	Kafka          *FrontendKafkaSpec  `json:"kafka,omitempty"`
	DeletionPolicy string              `json:"deletionPolicy,omitempty"`
	Monitoring     *IntegrationSetting `json:"monitoring,omitempty"`
	Console        *IntegrationSetting `json:"console,omitempty"`
}

type FrontendKafkaSpec struct {
	Version      string `json:"version,omitempty"`
	Brokers      int32  `json:"brokers,omitempty"`
	StorageGi    int32  `json:"storageGi,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
}

type IntegrationSetting struct {
	Enabled bool `json:"enabled,omitempty"`
}

func (r CreateRequest) ProductSpec() MessageQueueSpec {
	if r.Spec.Engine != "" || r.Spec.Kafka.Version != "" || r.Spec.Kafka.Replicas != 0 || r.Spec.Resources.CPU != "" || r.Spec.Resources.Memory != "" || r.Spec.Storage.Size != "" || r.Spec.DeletionPolicy != "" {
		return r.Spec
	}
	spec := MessageQueueSpec{Engine: r.Engine, DeletionPolicy: titlePolicy(r.DeletionPolicy)}
	if r.Kafka != nil {
		spec.Kafka = KafkaSpec{Version: r.Kafka.Version, Replicas: r.Kafka.Brokers}
		if r.Kafka.StorageGi > 0 {
			spec.Storage.Size = fmt.Sprintf("%dGi", r.Kafka.StorageGi)
		}
		spec.Storage.ClassName = r.Kafka.StorageClass
	}
	return spec
}

func titlePolicy(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "retain") {
		return "Retain"
	}
	if strings.EqualFold(strings.TrimSpace(value), "delete") {
		return "Delete"
	}
	return value
}

func (r CreateRequest) Validate() error {
	if !validDNSLabel(r.Name) {
		return errors.New("name must be a lowercase DNS label no longer than 63 characters")
	}
	if r.Spec.Storage.DeleteClaim != nil {
		return errors.New("storage.deleteClaim is managed by deletionPolicy")
	}
	spec := r.ProductSpec()
	if spec.Engine != "kafka" && spec.Engine != "" {
		return errors.New("only kafka is supported")
	}
	if spec.Kafka.Replicas < 0 || spec.Kafka.Replicas > 9 || spec.Replicas < 0 || spec.Replicas > 9 {
		return errors.New("kafka broker count must be between 0 and 9")
	}
	version := strings.TrimSpace(spec.Kafka.Version)
	if version == "" {
		version = strings.TrimSpace(spec.Version)
	}
	if version != "" && !supportedKafkaVersions[version] {
		return errors.New("unsupported kafka version")
	}
	if r.Kafka != nil && r.Kafka.StorageGi < 0 {
		return errors.New("kafka.storageGi must not be negative")
	}
	if spec.DeletionPolicy != "" && !strings.EqualFold(spec.DeletionPolicy, "delete") && !strings.EqualFold(spec.DeletionPolicy, "retain") {
		return errors.New("deletionPolicy must be delete or retain")
	}
	return nil
}

type MessageQueueView struct {
	Name              string             `json:"name"`
	Namespace         string             `json:"namespace"`
	Generation        int64              `json:"generation,omitempty"`
	CreationTimestamp string             `json:"creationTimestamp,omitempty"`
	Spec              MessageQueueSpec   `json:"spec"`
	Status            MessageQueueStatus `json:"status"`
}

func viewOf(resource MessageQueue, namespace string) MessageQueueView {
	// Namespace comes from identity, never from the object returned to the
	// browser. This also makes accidental cross-namespace store bugs visible.
	status := safeStatusOf(resource.Status)
	if status.Endpoint == "" && len(status.Endpoints) > 0 {
		status.Endpoint = status.Endpoints[0]
	}
	if status.Topology.Brokers == 0 {
		status.Topology.Brokers = resource.Spec.Kafka.Replicas
		if status.Topology.Brokers == 0 {
			status.Topology.Brokers = resource.Spec.Replicas
		}
	}
	return MessageQueueView{
		Name:              resource.Metadata.Name,
		Namespace:         namespace,
		Generation:        resource.Metadata.Generation,
		CreationTimestamp: resource.Metadata.CreationTimestamp,
		Spec:              resource.Spec,
		Status:            status,
	}
}

func safeStatusOf(status MessageQueueStatus) MessageQueueStatus {
	return MessageQueueStatus{
		Phase:              status.Phase,
		ObservedGeneration: status.ObservedGeneration,
		KafkaRef:           status.KafkaRef,
		NodePoolRef:        status.NodePoolRef,
		ClientSecretRef:    status.ClientSecretRef,
		Endpoints:          append([]string(nil), status.Endpoints...),
		ReadyReplicas:      status.ReadyReplicas,
		Message:            status.Message,
		Endpoint:           status.Endpoint,
		Conditions:         append([]Condition(nil), status.Conditions...),
		Topology:           status.Topology,
		LastTransitionTime: status.LastTransitionTime,
	}
}

type ListResponse struct {
	Items []MessageQueueView `json:"items"`
}

type ErrorResponse struct {
	Error APIError `json:"error"`
}

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type LogRequest struct {
	Component string `json:"component"`
	TailLines int    `json:"tailLines"`
}

type ClientConfigResponse struct {
	Name             string   `json:"name"`
	Namespace        string   `json:"namespace"`
	BootstrapServers []string `json:"bootstrapServers"`
	Username         string   `json:"username"`
	SecretRef        string   `json:"secretRef,omitempty"`
	Transport        string   `json:"transport"`
	Mechanism        string   `json:"mechanism"`
	Degraded         bool     `json:"degraded,omitempty"`
	Message          string   `json:"message,omitempty"`
}

type LogResponse struct {
	Name      string    `json:"name"`
	Component string    `json:"component"`
	Lines     []LogLine `json:"lines"`
	Degraded  bool      `json:"degraded,omitempty"`
	Message   string    `json:"message,omitempty"`
}

type LogLine struct {
	Timestamp string `json:"timestamp,omitempty"`
	Stream    string `json:"stream,omitempty"`
	Message   string `json:"message"`
}

type MetricResponse struct {
	Name     string        `json:"name"`
	Key      string        `json:"key"`
	Unit     string        `json:"unit"`
	Values   []MetricPoint `json:"values"`
	Degraded bool          `json:"degraded,omitempty"`
	Message  string        `json:"message,omitempty"`
}

type MetricPoint struct {
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value"`
}

func (m MetricResponse) Validate() error {
	if !metricKeys[m.Key] {
		return fmt.Errorf("unsupported metric key %q", m.Key)
	}
	return nil
}
