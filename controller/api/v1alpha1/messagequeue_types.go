package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	DefaultKafkaVersion = "3.9.0"
	DefaultStorageSize  = "10Gi"

	EngineKafka = "kafka"

	TopologyCombined = "combined"

	PhasePending      = "Pending"
	PhaseProvisioning = "Provisioning"
	PhaseReady        = "Ready"
	PhaseDegraded     = "Degraded"
	PhaseSuspended    = "Suspended"
	PhaseDeleting     = "Deleting"
	PhaseFailed       = "Failed"

	ConditionReady       = "Ready"
	ConditionProgressing = "Progressing"
	ConditionDegraded    = "Degraded"
	ConditionSuspended   = "Suspended"
)

// MessageQueueSpec is the user-facing desired state. Kafka is the only
// engine accepted in v1alpha1; engine-specific settings live in Kafka.
type MessageQueueSpec struct {
	// Engine defaults to kafka and is reserved for future engines such as
	// rabbitmq.
	Engine string `json:"engine,omitempty"`

	Kafka KafkaSpec `json:"kafka,omitempty"`

	// Version and Replicas are compatibility shorthands for clients that do
	// not send the nested Kafka block. Nested values take precedence.
	Version  string `json:"version,omitempty"`
	Replicas int32  `json:"replicas,omitempty"`

	Topology  TopologySpec `json:"topology,omitempty"`
	Resources ResourceSpec `json:"resources,omitempty"`
	Storage   StorageSpec  `json:"storage,omitempty"`

	// DeletionPolicy controls Strimzi persistent-claim cleanup. Supported
	// values are Delete (default) and Retain.
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
	Suspend        bool   `json:"suspend,omitempty"`
}

type KafkaSpec struct {
	Version   string          `json:"version,omitempty"`
	Replicas  int32           `json:"replicas,omitempty"`
	Listeners *KafkaListeners `json:"listeners,omitempty"`
}

// KafkaListeners contains listener intent only. The controller owns the
// rendered listener type, port, and address strategy.
type KafkaListeners struct {
	External *ExternalListener `json:"external,omitempty"`
}

type ExternalListener struct {
	Enabled                      bool     `json:"enabled,omitempty"`
	Type                         string   `json:"type,omitempty"`
	PreferredNodePortAddressType string   `json:"preferredNodePortAddressType,omitempty"`
	BootstrapAlternativeNames    []string `json:"bootstrapAlternativeNames,omitempty"`
}

type TopologySpec struct {
	// Mode is currently only combined, where each KafkaNodePool pod has both
	// broker and controller roles and runs KRaft.
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

// MessageQueueStatus reports observed state without embedding credentials or
// other secret material.
type MessageQueueStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	KafkaRef           string             `json:"kafkaRef,omitempty"`
	NodePoolRef        string             `json:"nodePoolRef,omitempty"`
	ClientSecretRef    string             `json:"clientSecretRef,omitempty"`
	Endpoints          []string           `json:"endpoints,omitempty"`
	ExternalEndpoints  []string           `json:"externalEndpoints,omitempty"`
	ReadyReplicas      int32              `json:"readyReplicas,omitempty"`
	Message            string             `json:"message,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=mq
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Engine",type="string",JSONPath=".spec.engine"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type MessageQueue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MessageQueueSpec   `json:"spec,omitempty"`
	Status            MessageQueueStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type MessageQueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MessageQueue `json:"items"`
}

func (in *MessageQueue) DeepCopyInto(out *MessageQueue) {
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Status = *in.Status.DeepCopy()
	if in.Spec.Storage.DeleteClaim != nil {
		v := *in.Spec.Storage.DeleteClaim
		out.Spec.Storage.DeleteClaim = &v
	}
	if in.Spec.Kafka.Listeners != nil {
		out.Spec.Kafka.Listeners = &KafkaListeners{}
		if in.Spec.Kafka.Listeners.External != nil {
			v := *in.Spec.Kafka.Listeners.External
			if in.Spec.Kafka.Listeners.External.BootstrapAlternativeNames != nil {
				v.BootstrapAlternativeNames = append([]string(nil), in.Spec.Kafka.Listeners.External.BootstrapAlternativeNames...)
			}
			out.Spec.Kafka.Listeners.External = &v
		}
	}
}

func (in *MessageQueue) DeepCopy() *MessageQueue {
	if in == nil {
		return nil
	}
	out := new(MessageQueue)
	in.DeepCopyInto(out)
	return out
}

func (in *MessageQueue) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *MessageQueueList) DeepCopyInto(out *MessageQueueList) {
	*out = *in
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]MessageQueue, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *MessageQueueList) DeepCopy() *MessageQueueList {
	if in == nil {
		return nil
	}
	out := new(MessageQueueList)
	in.DeepCopyInto(out)
	return out
}

func (in *MessageQueueList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *MessageQueueStatus) DeepCopy() *MessageQueueStatus {
	if in == nil {
		return nil
	}
	out := new(MessageQueueStatus)
	*out = *in
	if in.Endpoints != nil {
		out.Endpoints = append([]string(nil), in.Endpoints...)
	}
	if in.ExternalEndpoints != nil {
		out.ExternalEndpoints = append([]string(nil), in.ExternalEndpoints...)
	}
	if in.Conditions != nil {
		out.Conditions = append([]metav1.Condition(nil), in.Conditions...)
	}
	return out
}
