package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	v1alpha1 "github.com/labring/messagequeue/controller/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	KafkaAPIVersion         = "kafka.strimzi.io/v1beta2"
	KafkaKind               = "Kafka"
	KafkaNodePoolKind       = "KafkaNodePool"
	KafkaUserKind           = "KafkaUser"
	KafkaNodePoolNameSuffix = "-pool"
	KafkaUserNameSuffix     = "-client"

	labelPartOf    = "app.kubernetes.io/part-of"
	labelManagedBy = "app.kubernetes.io/managed-by"
	labelInstance  = "app.kubernetes.io/instance"
	controllerName = "messagequeue-controller"
)

var (
	kafkaGVK         = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1beta2", Kind: KafkaKind}
	kafkaNodePoolGVK = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1beta2", Kind: KafkaNodePoolKind}
	kafkaUserGVK     = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1beta2", Kind: KafkaUserKind}
)

// MessageQueueReconciler translates a product MessageQueue into Strimzi CRs.
// It intentionally uses unstructured Strimzi objects so the controller can be
// built independently of any particular Strimzi Go module release.
type MessageQueueReconciler struct {
	client.Client
}

// NewReconciler returns a reconciler suitable for a controller-runtime manager
// or direct unit testing with a fake client.
func NewReconciler(c client.Client) *MessageQueueReconciler {
	return &MessageQueueReconciler{Client: c}
}

func (r *MessageQueueReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	mq := &v1alpha1.MessageQueue{}
	if err := r.Get(ctx, req.NamespacedName, mq); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("get MessageQueue %s: %w", req.NamespacedName, err)
	}

	if !mq.DeletionTimestamp.IsZero() {
		status := mq.Status.DeepCopy()
		status.ObservedGeneration = mq.Generation
		status.Phase = v1alpha1.PhaseDeleting
		status.Message = "deletion is managed by Kubernetes owner references"
		setCondition(status, mq.Generation, v1alpha1.ConditionProgressing, metav1.ConditionTrue, "Deleting", status.Message)
		setCondition(status, mq.Generation, v1alpha1.ConditionReady, metav1.ConditionFalse, "Deleting", status.Message)
		_ = r.updateStatus(ctx, mq, status)
		return reconcile.Result{}, nil
	}

	desired, err := normalizeSpec(mq.Spec)
	if err != nil {
		status := mq.Status.DeepCopy()
		status.ObservedGeneration = mq.Generation
		status.Phase = v1alpha1.PhaseFailed
		status.Message = err.Error()
		setCondition(status, mq.Generation, v1alpha1.ConditionReady, metav1.ConditionFalse, "InvalidSpec", err.Error())
		setCondition(status, mq.Generation, v1alpha1.ConditionDegraded, metav1.ConditionTrue, "InvalidSpec", err.Error())
		setCondition(status, mq.Generation, v1alpha1.ConditionProgressing, metav1.ConditionFalse, "InvalidSpec", err.Error())
		setCondition(status, mq.Generation, v1alpha1.ConditionSuspended, metav1.ConditionFalse, "Active", "reconciliation is active")
		if updateErr := r.updateStatus(ctx, mq, status); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{}, nil
	}

	if mq.Spec.Suspend {
		status := mq.Status.DeepCopy()
		status.ObservedGeneration = mq.Generation
		status.Phase = v1alpha1.PhaseSuspended
		status.Message = "reconciliation is suspended by spec.suspend"
		setCondition(status, mq.Generation, v1alpha1.ConditionSuspended, metav1.ConditionTrue, "Suspended", status.Message)
		setCondition(status, mq.Generation, v1alpha1.ConditionReady, metav1.ConditionFalse, "Suspended", status.Message)
		setCondition(status, mq.Generation, v1alpha1.ConditionProgressing, metav1.ConditionFalse, "Suspended", status.Message)
		setCondition(status, mq.Generation, v1alpha1.ConditionDegraded, metav1.ConditionFalse, "Suspended", "")
		if err := r.updateStatus(ctx, mq, status); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	kafka := RenderKafka(mq, desired)
	nodePool := RenderKafkaNodePool(mq, desired)
	kafkaUser := RenderKafkaUser(mq)
	if err := controllerutil.SetControllerReference(mq, kafka, r.Scheme()); err != nil {
		return reconcile.Result{}, fmt.Errorf("set Kafka owner reference: %w", err)
	}
	if err := controllerutil.SetControllerReference(mq, nodePool, r.Scheme()); err != nil {
		return reconcile.Result{}, fmt.Errorf("set KafkaNodePool owner reference: %w", err)
	}
	if err := controllerutil.SetControllerReference(mq, kafkaUser, r.Scheme()); err != nil {
		return reconcile.Result{}, fmt.Errorf("set KafkaUser owner reference: %w", err)
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, kafka, func() error {
		// RenderKafka is deterministic. Replacing labels/spec on every run also
		// repairs drift while preserving server-owned metadata and status.
		applyRenderedObject(kafka, RenderKafka(mq, desired))
		return controllerutil.SetControllerReference(mq, kafka, r.Scheme())
	}); err != nil {
		return reconcile.Result{}, fmt.Errorf("reconcile Kafka: %w", err)
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, nodePool, func() error {
		applyRenderedObject(nodePool, RenderKafkaNodePool(mq, desired))
		return controllerutil.SetControllerReference(mq, nodePool, r.Scheme())
	}); err != nil {
		return reconcile.Result{}, fmt.Errorf("reconcile KafkaNodePool: %w", err)
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, kafkaUser, func() error {
		applyRenderedObject(kafkaUser, RenderKafkaUser(mq))
		return controllerutil.SetControllerReference(mq, kafkaUser, r.Scheme())
	}); err != nil {
		return reconcile.Result{}, fmt.Errorf("reconcile KafkaUser: %w", err)
	}

	observed := &unstructured.Unstructured{}
	observed.SetGroupVersionKind(kafkaGVK)
	ready, readyReplicas, observedMessage := false, int32(0), "waiting for Strimzi to report Ready"
	if err := r.Get(ctx, types.NamespacedName{Name: mq.Name, Namespace: mq.Namespace}, observed); err == nil {
		ready, readyReplicas, observedMessage = kafkaStatus(observed)
		if ready && readyReplicas == 0 {
			readyReplicas = desired.replicas
		}
	} else if !apierrors.IsNotFound(err) {
		return reconcile.Result{}, fmt.Errorf("observe Kafka: %w", err)
	}

	status := mq.Status.DeepCopy()
	status.ObservedGeneration = mq.Generation
	status.KafkaRef = mq.Name
	status.NodePoolRef = nodePool.GetName()
	status.ClientSecretRef = kafkaUser.GetName()
	status.Endpoints = []string{
		fmt.Sprintf("%s-kafka-bootstrap.%s.svc:9092", mq.Name, mq.Namespace),
		fmt.Sprintf("%s-kafka-bootstrap.%s.svc:9093", mq.Name, mq.Namespace),
	}
	status.ReadyReplicas = readyReplicas
	status.Message = observedMessage
	if ready {
		status.Phase = v1alpha1.PhaseReady
		setCondition(status, mq.Generation, v1alpha1.ConditionReady, metav1.ConditionTrue, "KafkaReady", observedMessage)
		setCondition(status, mq.Generation, v1alpha1.ConditionProgressing, metav1.ConditionFalse, "KafkaReady", observedMessage)
		setCondition(status, mq.Generation, v1alpha1.ConditionDegraded, metav1.ConditionFalse, "KafkaReady", "Strimzi reports the Kafka instance is ready")
		setCondition(status, mq.Generation, v1alpha1.ConditionSuspended, metav1.ConditionFalse, "Active", "reconciliation is active")
	} else {
		status.Phase = v1alpha1.PhaseProvisioning
		setCondition(status, mq.Generation, v1alpha1.ConditionReady, metav1.ConditionFalse, "Provisioning", observedMessage)
		setCondition(status, mq.Generation, v1alpha1.ConditionProgressing, metav1.ConditionTrue, "Provisioning", observedMessage)
		setCondition(status, mq.Generation, v1alpha1.ConditionDegraded, metav1.ConditionFalse, "Provisioning", "")
		setCondition(status, mq.Generation, v1alpha1.ConditionSuspended, metav1.ConditionFalse, "Active", "reconciliation is active")
	}
	if err := r.updateStatus(ctx, mq, status); err != nil {
		return reconcile.Result{}, err
	}

	if ready {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
}

// SetupWithManager registers the namespaced primary watch. Strimzi resources
// carry controller owner references, so a deployment can add child watches
// without changing the reconciliation contract.
func (r *MessageQueueReconciler) SetupWithManager(mgr manager.Manager) error {
	kafka := &unstructured.Unstructured{}
	kafka.SetGroupVersionKind(kafkaGVK)
	nodePool := &unstructured.Unstructured{}
	nodePool.SetGroupVersionKind(kafkaNodePoolGVK)
	kafkaUser := &unstructured.Unstructured{}
	kafkaUser.SetGroupVersionKind(kafkaUserGVK)
	return builder.ControllerManagedBy(mgr).
		For(&v1alpha1.MessageQueue{}).
		Owns(kafka).
		Owns(nodePool).
		Owns(kafkaUser).
		Complete(r)
}

type DesiredSpec struct {
	version      string
	replicas     int32
	topology     string
	storageSize  string
	storageClass string
	deleteClaim  bool
	cpu          string
	memory       string
}

// desiredSpec keeps the implementation concise while exposing DesiredSpec to
// package users that want to render resources in admission or integration
// tests.
type desiredSpec = DesiredSpec

// NormalizeSpec applies defaults and validates the product API before it is
// rendered into Strimzi resources.
func NormalizeSpec(spec v1alpha1.MessageQueueSpec) (DesiredSpec, error) {
	return normalizeSpec(spec)
}

func normalizeSpec(spec v1alpha1.MessageQueueSpec) (desiredSpec, error) {
	engine := strings.ToLower(strings.TrimSpace(spec.Engine))
	if engine == "" {
		engine = v1alpha1.EngineKafka
	}
	if engine != v1alpha1.EngineKafka {
		return desiredSpec{}, invalidSpecf("unsupported engine %q: only kafka is supported in v1alpha1", engine)
	}

	version := strings.TrimSpace(spec.Kafka.Version)
	if version == "" {
		version = strings.TrimSpace(spec.Version)
	}
	if version == "" {
		version = v1alpha1.DefaultKafkaVersion
	}
	if !supportedKafkaVersion(version) {
		return desiredSpec{}, invalidSpecf("unsupported Kafka version %q", version)
	}

	replicas := spec.Kafka.Replicas
	if replicas == 0 {
		replicas = spec.Topology.Replicas
	}
	if replicas == 0 {
		replicas = spec.Replicas
	}
	if replicas == 0 {
		replicas = 1
	}
	if replicas < 1 || replicas > 9 {
		return desiredSpec{}, invalidSpecf("Kafka replicas must be between 1 and 9, got %d", replicas)
	}

	topology := strings.ToLower(strings.TrimSpace(spec.Topology.Mode))
	if topology == "" {
		topology = v1alpha1.TopologyCombined
	}
	if topology != v1alpha1.TopologyCombined {
		return desiredSpec{}, invalidSpecf("unsupported Kafka topology %q: only combined KRaft is supported", topology)
	}

	storageSize := strings.TrimSpace(spec.Storage.Size)
	if storageSize == "" {
		storageSize = v1alpha1.DefaultStorageSize
	}
	storageClass := strings.TrimSpace(spec.Storage.ClassName)
	deleteClaim := true
	if spec.Storage.DeleteClaim != nil {
		deleteClaim = *spec.Storage.DeleteClaim
	}
	if strings.EqualFold(spec.DeletionPolicy, "Retain") {
		deleteClaim = false
	} else if spec.DeletionPolicy != "" && !strings.EqualFold(spec.DeletionPolicy, "Delete") {
		return desiredSpec{}, invalidSpecf("unsupported deletionPolicy %q: use Delete or Retain", spec.DeletionPolicy)
	}

	return desiredSpec{version: version, replicas: replicas, topology: topology, storageSize: storageSize,
		storageClass: storageClass, deleteClaim: deleteClaim, cpu: strings.TrimSpace(spec.Resources.CPU), memory: strings.TrimSpace(spec.Resources.Memory)}, nil
}

func supportedKafkaVersion(version string) bool {
	// Strimzi-compatible versions are intentionally explicit to avoid passing
	// arbitrary user input through to the operator.
	switch version {
	case "3.9.0", "4.0.0":
		return true
	default:
		return false
	}
}

func RenderKafka(mq *v1alpha1.MessageQueue, desired desiredSpec) *unstructured.Unstructured {
	replicationFactor := int64(desired.replicas)
	minISR := replicationFactor - 1
	if minISR < 1 {
		minISR = 1
	}
	metadata := objectMetadata(mq, KafkaKind)
	metadata["annotations"] = map[string]interface{}{
		"strimzi.io/node-pools": "enabled",
		"strimzi.io/kraft":      "enabled",
	}
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": KafkaAPIVersion,
		"kind":       KafkaKind,
		"metadata":   metadata,
		"spec": map[string]interface{}{
			"kafka": map[string]interface{}{
				"version": desired.version,
				"listeners": []interface{}{
					map[string]interface{}{"name": "internal", "port": int64(9092), "type": "internal", "tls": false, "authentication": map[string]interface{}{"type": "scram-sha-512"}},
					map[string]interface{}{"name": "tls", "port": int64(9093), "type": "internal", "tls": true, "authentication": map[string]interface{}{"type": "scram-sha-512"}},
				},
				"config": map[string]interface{}{
					"default.replication.factor":               replicationFactor,
					"min.insync.replicas":                      minISR,
					"offsets.topic.replication.factor":         replicationFactor,
					"transaction.state.log.replication.factor": replicationFactor,
					"transaction.state.log.min.isr":            minISR,
				},
				"authorization": map[string]interface{}{"type": "simple"},
			},
			"entityOperator": map[string]interface{}{
				"topicOperator": map[string]interface{}{
					"resources": entityOperatorResources(),
				},
				"userOperator": map[string]interface{}{
					"resources": entityOperatorResources(),
				},
			},
		},
	}}
	if resources := resourceSpec(desired); resources != nil {
		_ = unstructured.SetNestedField(obj.Object, resources, "spec", "kafka", "resources")
	}
	return obj
}

func RenderKafkaNodePool(mq *v1alpha1.MessageQueue, desired desiredSpec) *unstructured.Unstructured {
	metadata := objectMetadata(mq, KafkaNodePoolKind)
	metadata["name"] = mq.Name + KafkaNodePoolNameSuffix
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": KafkaAPIVersion,
		"kind":       KafkaNodePoolKind,
		"metadata":   metadata,
		"spec": map[string]interface{}{
			"replicas": int64(desired.replicas),
			"roles":    []interface{}{"broker", "controller"},
			"storage":  nodePoolStorageSpec(desired),
		},
	}}
	if resources := resourceSpec(desired); resources != nil {
		_ = unstructured.SetNestedField(obj.Object, resources, "spec", "resources")
	}
	return obj
}

// RenderKafkaUser creates the development client identity. Strimzi writes the
// generated password to a same-name Secret; MessageQueue status only exposes
// that Secret reference and never copies credential material.
func RenderKafkaUser(mq *v1alpha1.MessageQueue) *unstructured.Unstructured {
	metadata := objectMetadata(mq, KafkaUserKind)
	metadata["name"] = mq.Name + KafkaUserNameSuffix
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": KafkaAPIVersion,
		"kind":       KafkaUserKind,
		"metadata":   metadata,
		"spec": map[string]interface{}{
			"authentication": map[string]interface{}{"type": "scram-sha-512"},
			"authorization": map[string]interface{}{
				"type": "simple",
				"acls": []interface{}{
					map[string]interface{}{
						"resource":   map[string]interface{}{"type": "topic", "name": "*", "patternType": "literal"},
						"operations": []interface{}{"Create", "Describe", "Read", "Write"},
						"host":       "*",
					},
					map[string]interface{}{
						"resource":   map[string]interface{}{"type": "group", "name": "*", "patternType": "literal"},
						"operations": []interface{}{"Describe", "Read"},
						"host":       "*",
					},
					map[string]interface{}{
						"resource":   map[string]interface{}{"type": "cluster"},
						"operations": []interface{}{"Describe", "IdempotentWrite"},
						"host":       "*",
					},
				},
			},
		},
	}}
}

func objectMetadata(mq *v1alpha1.MessageQueue, kind string) map[string]interface{} {
	labels := map[string]interface{}{
		labelPartOf:                            "messagequeue",
		labelManagedBy:                         controllerName,
		labelInstance:                          mq.Name,
		"messagequeue.sealos.io/engine":        "kafka",
		"messagequeue.sealos.io/resource-kind": strings.ToLower(kind),
	}
	if kind == KafkaNodePoolKind || kind == KafkaUserKind {
		labels["strimzi.io/cluster"] = mq.Name
	}
	return map[string]interface{}{
		"name":      mq.Name,
		"namespace": mq.Namespace,
		"labels":    labels,
	}
}

func nodePoolStorageSpec(desired desiredSpec) map[string]interface{} {
	volume := map[string]interface{}{
		"id":            int64(0),
		"type":          "persistent-claim",
		"size":          desired.storageSize,
		"deleteClaim":   desired.deleteClaim,
		"kraftMetadata": "shared",
	}
	if desired.storageClass != "" {
		volume["class"] = desired.storageClass
	}
	return map[string]interface{}{
		"type":    "jbod",
		"volumes": []interface{}{volume},
	}
}

func resourceSpec(desired desiredSpec) map[string]interface{} {
	if desired.cpu == "" && desired.memory == "" {
		return nil
	}
	requests := map[string]interface{}{}
	limits := map[string]interface{}{}
	if desired.cpu != "" {
		requests["cpu"], limits["cpu"] = desired.cpu, desired.cpu
	}
	if desired.memory != "" {
		requests["memory"], limits["memory"] = desired.memory, desired.memory
	}
	return map[string]interface{}{"requests": requests, "limits": limits}
}

func entityOperatorResources() map[string]interface{} {
	return map[string]interface{}{
		"requests": map[string]interface{}{
			"cpu":    "200m",
			"memory": "256Mi",
		},
		"limits": map[string]interface{}{
			"cpu":    "500m",
			"memory": "512Mi",
		},
	}
}

func applyRenderedObject(dst, src *unstructured.Unstructured) {
	labels, _, _ := unstructured.NestedStringMap(src.Object, "metadata", "labels")
	_ = unstructured.SetNestedStringMap(dst.Object, labels, "metadata", "labels")
	annotations, _, _ := unstructured.NestedStringMap(src.Object, "metadata", "annotations")
	if annotations != nil {
		_ = unstructured.SetNestedStringMap(dst.Object, annotations, "metadata", "annotations")
	}
	_ = unstructured.SetNestedField(dst.Object, src.Object["spec"], "spec")
	dst.SetGroupVersionKind(src.GroupVersionKind())
}

func kafkaStatus(obj *unstructured.Unstructured) (ready bool, readyReplicas int32, message string) {
	message = "waiting for Strimzi to report Ready"
	if value, found, _ := unstructured.NestedInt64(obj.Object, "status", "readyReplicas"); found {
		readyReplicas = int32(value)
	}
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return false, readyReplicas, message
	}
	for _, raw := range conditions {
		condition, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		typeValue, _, _ := unstructured.NestedString(condition, "type")
		statusValue, _, _ := unstructured.NestedString(condition, "status")
		reason, _, _ := unstructured.NestedString(condition, "reason")
		conditionMessage, _, _ := unstructured.NestedString(condition, "message")
		if typeValue == "Ready" {
			if statusValue == "True" {
				if conditionMessage != "" {
					message = conditionMessage
				} else {
					message = "Strimzi reports the Kafka instance is ready"
				}
				return true, readyReplicas, message
			}
			if reason != "" {
				message = reason
			}
			if conditionMessage != "" {
				message = conditionMessage
			}
		}
	}
	return false, readyReplicas, message
}

func (r *MessageQueueReconciler) updateStatus(ctx context.Context, mq *v1alpha1.MessageQueue, desired *v1alpha1.MessageQueueStatus) error {
	if reflect.DeepEqual(mq.Status, *desired) {
		return nil
	}
	mq.Status = *desired
	return r.Status().Update(ctx, mq)
}

func setCondition(status *v1alpha1.MessageQueueStatus, generation int64, typ string, conditionStatus metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	for i := range status.Conditions {
		if status.Conditions[i].Type != typ {
			continue
		}
		old := status.Conditions[i]
		status.Conditions[i].Status = conditionStatus
		status.Conditions[i].Reason = reason
		status.Conditions[i].Message = message
		status.Conditions[i].ObservedGeneration = generation
		if old.Status != conditionStatus || old.Reason != reason || old.Message != message || old.ObservedGeneration != generation {
			status.Conditions[i].LastTransitionTime = now
		}
		return
	}
	status.Conditions = append(status.Conditions, metav1.Condition{Type: typ, Status: conditionStatus, Reason: reason, Message: message, ObservedGeneration: generation, LastTransitionTime: now})
}

// Keep errors.Is available to callers that want to classify invalid specs
// without depending on controller internals.
var ErrInvalidSpec = errors.New("invalid MessageQueue spec")

func invalidSpecf(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrInvalidSpec, fmt.Sprintf(format, args...))
}
