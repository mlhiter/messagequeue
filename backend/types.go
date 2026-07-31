package main

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrUnauthenticated       = errors.New("unauthenticated")
	ErrNotFound              = errors.New("not found")
	ErrConflict              = errors.New("conflict")
	ErrForbidden             = errors.New("forbidden")
	ErrQuotaExceeded         = errors.New("quota exceeded")
	ErrDependencyUnavailable = errors.New("dependency unavailable")
	ErrInvalid               = errors.New("invalid request")
)

var (
	dnsLabelPattern     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	dnsSubdomainPattern = regexp.MustCompile(
		`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`,
	)
	cpuPattern    = regexp.MustCompile(`^([1-9][0-9]*m|[1-9][0-9]*(\.[0-9]+)?|0\.[0-9]*[1-9][0-9]*)$`)
	memoryPattern = regexp.MustCompile(`^[1-9][0-9]*(Ei|Pi|Ti|Gi|Mi|Ki|E|P|T|G|M|K)$`)
)

const (
	kib = int64(1024)
	mib = kib * 1024
	gib = mib * 1024
	tib = gib * 1024
	pib = tib * 1024
	eib = pib * 1024

	kilo = int64(1000)
	mega = kilo * 1000
	giga = mega * 1000
	tera = giga * 1000
	peta = tera * 1000
	exa  = peta * 1000

	maxBrokerCPUMilli     = 8000
	maxBrokerMemoryBytes  = 64 * gib
	maxBrokerStorageBytes = 1024 * gib
	maxStorageGi          = 1024
)

func validDNSLabel(value string) bool {
	return len(value) <= 63 && value != "" && dnsLabelPattern.MatchString(value)
}

func validDNSSubdomain(value string) bool {
	return len(value) <= 253 && value != "" && dnsSubdomainPattern.MatchString(value)
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
	Spec           ProductCreateSpec   `json:"spec"`
	Engine         string              `json:"engine,omitempty"`
	Kafka          *FrontendKafkaSpec  `json:"kafka,omitempty"`
	DeletionPolicy string              `json:"deletionPolicy,omitempty"`
	Monitoring     *IntegrationSetting `json:"monitoring,omitempty"`
	Console        *IntegrationSetting `json:"console,omitempty"`
}

type ProductCreateSpec struct {
	Engine         string             `json:"engine,omitempty"`
	Kafka          KafkaSpec          `json:"kafka,omitempty"`
	Version        string             `json:"version,omitempty"`
	Replicas       int32              `json:"replicas,omitempty"`
	Topology       *TopologySpec      `json:"topology,omitempty"`
	Resources      ResourceSpec       `json:"resources,omitempty"`
	Storage        ProductStorageSpec `json:"storage,omitempty"`
	DeletionPolicy string             `json:"deletionPolicy,omitempty"`
	Suspend        *bool              `json:"suspend,omitempty"`
}

type ProductStorageSpec struct {
	Size        string `json:"size,omitempty"`
	ClassName   string `json:"className,omitempty"`
	DeleteClaim *bool  `json:"deleteClaim,omitempty"`
}

type FrontendKafkaSpec struct {
	Version      string `json:"version,omitempty"`
	Brokers      int32  `json:"brokers,omitempty"`
	CPU          string `json:"cpu,omitempty"`
	Memory       string `json:"memory,omitempty"`
	StorageGi    int32  `json:"storageGi,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
}

type IntegrationSetting struct {
	Enabled bool `json:"enabled,omitempty"`
}

func (r CreateRequest) ProductSpec() MessageQueueSpec {
	var spec MessageQueueSpec
	if r.Spec.hasValues() {
		spec = r.Spec.messageQueueSpec()
	} else {
		spec = MessageQueueSpec{Engine: r.Engine, DeletionPolicy: titlePolicy(r.DeletionPolicy)}
		if r.Kafka != nil {
			spec.Kafka = KafkaSpec{Version: r.Kafka.Version, Replicas: r.Kafka.Brokers}
			spec.Resources = ResourceSpec{CPU: strings.TrimSpace(r.Kafka.CPU), Memory: strings.TrimSpace(r.Kafka.Memory)}
			if r.Kafka.StorageGi > 0 {
				spec.Storage.Size = fmt.Sprintf("%dGi", r.Kafka.StorageGi)
			}
			spec.Storage.ClassName = r.Kafka.StorageClass
		}
	}
	return applyProductDefaults(spec)
}

func (s ProductCreateSpec) hasValues() bool {
	return strings.TrimSpace(s.Engine) != "" ||
		strings.TrimSpace(s.Kafka.Version) != "" ||
		s.Kafka.Replicas != 0 ||
		strings.TrimSpace(s.Version) != "" ||
		s.Replicas != 0 ||
		s.Topology != nil ||
		strings.TrimSpace(s.Resources.CPU) != "" ||
		strings.TrimSpace(s.Resources.Memory) != "" ||
		strings.TrimSpace(s.Storage.Size) != "" ||
		strings.TrimSpace(s.Storage.ClassName) != "" ||
		s.Storage.DeleteClaim != nil ||
		strings.TrimSpace(s.DeletionPolicy) != "" ||
		s.Suspend != nil
}

func (s ProductCreateSpec) messageQueueSpec() MessageQueueSpec {
	return MessageQueueSpec{
		Engine:   strings.TrimSpace(s.Engine),
		Kafka:    KafkaSpec{Version: strings.TrimSpace(s.Kafka.Version), Replicas: s.Kafka.Replicas},
		Version:  strings.TrimSpace(s.Version),
		Replicas: s.Replicas,
		Resources: ResourceSpec{
			CPU:    strings.TrimSpace(s.Resources.CPU),
			Memory: strings.TrimSpace(s.Resources.Memory),
		},
		Storage: StorageSpec{
			Size:      strings.TrimSpace(s.Storage.Size),
			ClassName: strings.TrimSpace(s.Storage.ClassName),
		},
		DeletionPolicy: titlePolicy(s.DeletionPolicy),
	}
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
	if r.Spec.Topology != nil {
		return errors.New("spec.topology is managed by the controller")
	}
	if r.Spec.Suspend != nil {
		return errors.New("spec.suspend is not accepted by create")
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
	if r.Kafka != nil && r.Kafka.StorageGi > maxStorageGi {
		return fmt.Errorf("kafka.storageGi must not exceed %dGi per broker", maxStorageGi)
	}
	if spec.DeletionPolicy != "" && !strings.EqualFold(spec.DeletionPolicy, "delete") && !strings.EqualFold(spec.DeletionPolicy, "retain") {
		return errors.New("deletionPolicy must be delete or retain")
	}
	if err := validateCPUQuantity("resources.cpu", spec.Resources.CPU); err != nil {
		return err
	}
	if err := validateSizedQuantity("resources.memory", spec.Resources.Memory, maxBrokerMemoryBytes, "64Gi"); err != nil {
		return err
	}
	if err := validateSizedQuantity("storage.size", spec.Storage.Size, maxBrokerStorageBytes, "1024Gi"); err != nil {
		return err
	}
	if spec.Storage.ClassName != "" && !validDNSSubdomain(spec.Storage.ClassName) {
		return errors.New("storage.className must be a valid Kubernetes resource name")
	}
	return nil
}

func validateCPUQuantity(field, value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	if !cpuPattern.MatchString(trimmed) {
		return fmt.Errorf("%s must be a positive Kubernetes quantity", field)
	}
	milli, err := cpuMilli(trimmed)
	if err != nil {
		return fmt.Errorf("%s must be a positive Kubernetes quantity", field)
	}
	if milli > maxBrokerCPUMilli {
		return fmt.Errorf("%s must not exceed 8 CPU per broker", field)
	}
	return nil
}

func cpuMilli(value string) (float64, error) {
	if strings.HasSuffix(value, "m") {
		parsed, err := strconv.ParseInt(strings.TrimSuffix(value, "m"), 10, 64)
		if err != nil || parsed <= 0 {
			return 0, errors.New("invalid cpu quantity")
		}
		return float64(parsed), nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 {
		return 0, errors.New("invalid cpu quantity")
	}
	return parsed * 1000, nil
}

func validateSizedQuantity(field, value string, maxBytes int64, maxLabel string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	if !memoryPattern.MatchString(trimmed) {
		return fmt.Errorf("%s must be a positive Kubernetes quantity", field)
	}
	bytes, err := quantityBytes(trimmed)
	if err != nil {
		return fmt.Errorf("%s must be a positive Kubernetes quantity", field)
	}
	if bytes > maxBytes {
		return fmt.Errorf("%s must not exceed %s per broker", field, maxLabel)
	}
	return nil
}

func quantityBytes(value string) (int64, error) {
	for _, suffix := range quantitySuffixes {
		if strings.HasSuffix(value, suffix.suffix) {
			number := strings.TrimSuffix(value, suffix.suffix)
			parsed, err := strconv.ParseInt(number, 10, 64)
			if err != nil || parsed <= 0 {
				return 0, errors.New("invalid quantity")
			}
			if parsed > (1<<63-1)/suffix.multiplier {
				return 0, errors.New("quantity overflow")
			}
			return parsed * suffix.multiplier, nil
		}
	}
	return 0, errors.New("invalid quantity")
}

var quantitySuffixes = []struct {
	suffix     string
	multiplier int64
}{
	{"Ei", eib},
	{"Pi", pib},
	{"Ti", tib},
	{"Gi", gib},
	{"Mi", mib},
	{"Ki", kib},
	{"E", exa},
	{"P", peta},
	{"T", tera},
	{"G", giga},
	{"M", mega},
	{"K", kilo},
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
