package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strings"
	"time"

	v1alpha1 "github.com/labring/messagequeue/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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
	KafkaAPIVersion                 = "kafka.strimzi.io/v1beta2"
	KafkaKind                       = "Kafka"
	KafkaNodePoolKind               = "KafkaNodePool"
	KafkaUserKind                   = "KafkaUser"
	KafkaNodePoolNameSuffix         = "-pool"
	KafkaUserNameSuffix             = "-client"
	KafkaMetricsConfigMapNameSuffix = "-kafka-metrics"
	CredentialAccessNameSuffix      = "-credential-access"
	kafkaMetricsConfigKey           = "metrics-config.yaml"

	labelPartOf    = "app.kubernetes.io/part-of"
	labelManagedBy = "app.kubernetes.io/managed-by"
	labelInstance  = "app.kubernetes.io/instance"
	controllerName = "messagequeue-controller"
)

var (
	kafkaGVK         = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1beta2", Kind: KafkaKind}
	kafkaNodePoolGVK = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1beta2", Kind: KafkaNodePoolKind}
	kafkaUserGVK     = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1beta2", Kind: KafkaUserKind}
	dnsSubdomainRe   = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
)

// MessageQueueReconciler translates a product MessageQueue into Strimzi CRs.
// It intentionally uses unstructured Strimzi objects so the controller can be
// built independently of any particular Strimzi Go module release.
type MessageQueueReconciler struct {
	client.Client
	BackendServiceAccountName      string
	BackendServiceAccountNamespace string
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
		if err := r.reconcileCredentialAccess(ctx, mq); err != nil {
			return reconcile.Result{}, err
		}
		if err := r.suspendNodePool(ctx, mq); err != nil {
			return reconcile.Result{}, err
		}
		status := mq.Status.DeepCopy()
		status.ObservedGeneration = mq.Generation
		status.Phase = v1alpha1.PhaseSuspended
		status.Message = "reconciliation is suspended by spec.suspend"
		status.Endpoints = nil
		status.ExternalEndpoints = nil
		status.ReadyReplicas = 0
		setCondition(status, mq.Generation, v1alpha1.ConditionSuspended, metav1.ConditionTrue, "Suspended", status.Message)
		setCondition(status, mq.Generation, v1alpha1.ConditionReady, metav1.ConditionFalse, "Suspended", status.Message)
		setCondition(status, mq.Generation, v1alpha1.ConditionProgressing, metav1.ConditionFalse, "Suspended", status.Message)
		setCondition(status, mq.Generation, v1alpha1.ConditionDegraded, metav1.ConditionFalse, "Suspended", "")
		if err := r.updateStatus(ctx, mq, status); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	metricsConfigMap := RenderKafkaMetricsConfigMap(mq)
	kafka := RenderKafka(mq, desired)
	nodePool := RenderKafkaNodePool(mq, desired)
	kafkaUser := RenderKafkaUser(mq)
	if err := controllerutil.SetControllerReference(mq, metricsConfigMap, r.Scheme()); err != nil {
		return reconcile.Result{}, fmt.Errorf("set Kafka metrics ConfigMap owner reference: %w", err)
	}
	if err := controllerutil.SetControllerReference(mq, kafka, r.Scheme()); err != nil {
		return reconcile.Result{}, fmt.Errorf("set Kafka owner reference: %w", err)
	}
	if err := controllerutil.SetControllerReference(mq, nodePool, r.Scheme()); err != nil {
		return reconcile.Result{}, fmt.Errorf("set KafkaNodePool owner reference: %w", err)
	}
	if err := controllerutil.SetControllerReference(mq, kafkaUser, r.Scheme()); err != nil {
		return reconcile.Result{}, fmt.Errorf("set KafkaUser owner reference: %w", err)
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, metricsConfigMap, func() error {
		rendered := RenderKafkaMetricsConfigMap(mq)
		metricsConfigMap.Labels = rendered.Labels
		metricsConfigMap.Annotations = rendered.Annotations
		metricsConfigMap.Data = rendered.Data
		metricsConfigMap.BinaryData = rendered.BinaryData
		metricsConfigMap.Immutable = rendered.Immutable
		return controllerutil.SetControllerReference(mq, metricsConfigMap, r.Scheme())
	}); err != nil {
		return reconcile.Result{}, fmt.Errorf("reconcile Kafka metrics ConfigMap: %w", err)
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
	if err := r.reconcileCredentialAccess(ctx, mq); err != nil {
		return reconcile.Result{}, err
	}

	observed := &unstructured.Unstructured{}
	observed.SetGroupVersionKind(kafkaGVK)
	ready, readyReplicas, observedMessage := false, int32(0), "waiting for Strimzi to report Ready"
	externalEndpoints := []string(nil)
	if err := r.Get(ctx, types.NamespacedName{Name: mq.Name, Namespace: mq.Namespace}, observed); err == nil {
		ready, readyReplicas, observedMessage = kafkaStatus(observed)
		if ready && readyReplicas == 0 {
			readyReplicas = desired.replicas
		}
		externalEndpoints = kafkaExternalEndpoints(observed)
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
	status.ExternalEndpoints = externalEndpoints
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
		Owns(&corev1.ConfigMap{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
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
	if err := validateExternalListener(spec.Kafka.Listeners); err != nil {
		return desiredSpec{}, err
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
	listeners := []interface{}{
		map[string]interface{}{"name": "internal", "port": int64(9092), "type": "internal", "tls": false, "authentication": map[string]interface{}{"type": "scram-sha-512"}},
		map[string]interface{}{"name": "tls", "port": int64(9093), "type": "internal", "tls": true, "authentication": map[string]interface{}{"type": "scram-sha-512"}},
	}
	if externalListener, ok := renderExternalKafkaListener(mq); ok {
		listeners = append(listeners, externalListener)
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
				"version":   desired.version,
				"listeners": listeners,
				"config": map[string]interface{}{
					"default.replication.factor":               replicationFactor,
					"min.insync.replicas":                      minISR,
					"offsets.topic.replication.factor":         replicationFactor,
					"transaction.state.log.replication.factor": replicationFactor,
					"transaction.state.log.min.isr":            minISR,
				},
				"authorization": map[string]interface{}{"type": "simple"},
				"metricsConfig": map[string]interface{}{
					"type": "jmxPrometheusExporter",
					"valueFrom": map[string]interface{}{
						"configMapKeyRef": map[string]interface{}{
							"name": metricsConfigMapName(mq),
							"key":  kafkaMetricsConfigKey,
						},
					},
				},
				"template": managedPodTemplate(),
			},
			"kafkaExporter": map[string]interface{}{
				"topicRegex": ".*",
				"groupRegex": ".*",
				"resources":  kafkaExporterResources(),
				"template":   managedPodTemplate(),
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
			"template": managedPodTemplate(),
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

func RenderCredentialAccessRole(mq *v1alpha1.MessageQueue) *rbacv1.Role {
	return &rbacv1.Role{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "Role",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      credentialAccessName(mq),
			Namespace: mq.Namespace,
			Labels:    typedLabels(objectLabels(mq, "credential-access")),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups:     []string{""},
				Resources:     []string{"secrets"},
				ResourceNames: []string{mq.Name + KafkaUserNameSuffix, mq.Name + "-cluster-ca-cert"},
				Verbs:         []string{"get"},
			},
		},
	}
}

func RenderCredentialAccessRoleBinding(mq *v1alpha1.MessageQueue, serviceAccountName, serviceAccountNamespace string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "RoleBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      credentialAccessName(mq),
			Namespace: mq.Namespace,
			Labels:    typedLabels(objectLabels(mq, "credential-access")),
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      serviceAccountName,
				Namespace: serviceAccountNamespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     credentialAccessName(mq),
		},
	}
}

func renderExternalKafkaListener(mq *v1alpha1.MessageQueue) (map[string]interface{}, bool) {
	if mq.Spec.Kafka.Listeners == nil || mq.Spec.Kafka.Listeners.External == nil || !mq.Spec.Kafka.Listeners.External.Enabled {
		return nil, false
	}
	listenerType := strings.ToLower(strings.TrimSpace(mq.Spec.Kafka.Listeners.External.Type))
	if listenerType == "" {
		listenerType = "nodeport"
	}
	if listenerType != "nodeport" {
		listenerType = "nodeport"
	}
	addressType := strings.TrimSpace(mq.Spec.Kafka.Listeners.External.PreferredNodePortAddressType)
	if addressType == "" {
		addressType = "InternalIP"
	}
	listener := map[string]interface{}{
		"name": "external",
		"port": int64(9094),
		"type": listenerType,
		"tls":  true,
		"authentication": map[string]interface{}{
			"type": "scram-sha-512",
		},
		"configuration": map[string]interface{}{
			"preferredNodePortAddressType": addressType,
		},
	}
	if len(mq.Spec.Kafka.Listeners.External.BootstrapAlternativeNames) > 0 {
		alternativeNames := make([]interface{}, 0, len(mq.Spec.Kafka.Listeners.External.BootstrapAlternativeNames))
		for _, name := range mq.Spec.Kafka.Listeners.External.BootstrapAlternativeNames {
			if trimmed := strings.TrimSpace(name); trimmed != "" {
				alternativeNames = append(alternativeNames, trimmed)
			}
		}
		if len(alternativeNames) > 0 {
			_ = unstructured.SetNestedSlice(listener, alternativeNames, "configuration", "bootstrap", "alternativeNames")
		}
	}
	return listener, true
}

func kafkaExternalEndpoints(obj *unstructured.Unstructured) []string {
	return kafkaListenerEndpoints(obj, "external")
}

func kafkaListenerEndpoints(obj *unstructured.Unstructured, name string) []string {
	listeners, found, _ := unstructured.NestedSlice(obj.Object, "status", "listeners")
	if !found {
		return nil
	}
	endpoints := []string{}
	seen := map[string]bool{}
	for _, raw := range listeners {
		listener, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		listenerName, _, _ := unstructured.NestedString(listener, "name")
		if listenerName != name {
			continue
		}
		if bootstrap, _, _ := unstructured.NestedString(listener, "bootstrapServers"); bootstrap != "" && !seen[bootstrap] {
			seen[bootstrap] = true
			endpoints = append(endpoints, bootstrap)
		}
		addresses, found, _ := unstructured.NestedSlice(listener, "addresses")
		if !found {
			continue
		}
		for _, rawAddress := range addresses {
			address, ok := rawAddress.(map[string]interface{})
			if !ok {
				continue
			}
			host, _, _ := unstructured.NestedString(address, "host")
			port, foundPort, _ := unstructured.NestedInt64(address, "port")
			if host == "" || !foundPort || port <= 0 {
				continue
			}
			endpoint := fmt.Sprintf("%s:%d", host, port)
			if seen[endpoint] {
				continue
			}
			seen[endpoint] = true
			endpoints = append(endpoints, endpoint)
		}
	}
	return endpoints
}

func objectMetadata(mq *v1alpha1.MessageQueue, kind string) map[string]interface{} {
	labels := objectLabels(mq, strings.ToLower(kind))
	if kind == KafkaNodePoolKind || kind == KafkaUserKind {
		labels["strimzi.io/cluster"] = mq.Name
	}
	return map[string]interface{}{
		"name":      mq.Name,
		"namespace": mq.Namespace,
		"labels":    labels,
	}
}

func objectLabels(mq *v1alpha1.MessageQueue, resourceKind string) map[string]interface{} {
	return map[string]interface{}{
		labelPartOf:                            "messagequeue",
		labelManagedBy:                         controllerName,
		labelInstance:                          mq.Name,
		"messagequeue.sealos.io/engine":        "kafka",
		"messagequeue.sealos.io/resource-kind": resourceKind,
	}
}

func typedLabels(labels map[string]interface{}) map[string]string {
	out := make(map[string]string, len(labels))
	for key, value := range labels {
		out[key] = fmt.Sprint(value)
	}
	return out
}

func credentialAccessName(mq *v1alpha1.MessageQueue) string {
	return mq.Name + CredentialAccessNameSuffix
}

func (r *MessageQueueReconciler) backendServiceAccountName() string {
	if name := strings.TrimSpace(r.BackendServiceAccountName); name != "" {
		return name
	}
	return "messagequeue-backend"
}

func (r *MessageQueueReconciler) backendServiceAccountNamespace(mq *v1alpha1.MessageQueue) string {
	if namespace := strings.TrimSpace(r.BackendServiceAccountNamespace); namespace != "" {
		return namespace
	}
	return mq.Namespace
}

func (r *MessageQueueReconciler) reconcileCredentialAccess(ctx context.Context, mq *v1alpha1.MessageQueue) error {
	credentialRole := RenderCredentialAccessRole(mq)
	credentialRoleBinding := RenderCredentialAccessRoleBinding(mq, r.backendServiceAccountName(), r.backendServiceAccountNamespace(mq))
	if err := controllerutil.SetControllerReference(mq, credentialRole, r.Scheme()); err != nil {
		return fmt.Errorf("set credential Role owner reference: %w", err)
	}
	if err := controllerutil.SetControllerReference(mq, credentialRoleBinding, r.Scheme()); err != nil {
		return fmt.Errorf("set credential RoleBinding owner reference: %w", err)
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, credentialRole, func() error {
		rendered := RenderCredentialAccessRole(mq)
		credentialRole.Labels = rendered.Labels
		credentialRole.Rules = rendered.Rules
		return controllerutil.SetControllerReference(mq, credentialRole, r.Scheme())
	}); err != nil {
		return fmt.Errorf("reconcile credential Role: %w", err)
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, credentialRoleBinding, func() error {
		rendered := RenderCredentialAccessRoleBinding(mq, r.backendServiceAccountName(), r.backendServiceAccountNamespace(mq))
		credentialRoleBinding.Labels = rendered.Labels
		credentialRoleBinding.Subjects = rendered.Subjects
		credentialRoleBinding.RoleRef = rendered.RoleRef
		return controllerutil.SetControllerReference(mq, credentialRoleBinding, r.Scheme())
	}); err != nil {
		return fmt.Errorf("reconcile credential RoleBinding: %w", err)
	}
	return nil
}

func (r *MessageQueueReconciler) suspendNodePool(ctx context.Context, mq *v1alpha1.MessageQueue) error {
	nodePool := &unstructured.Unstructured{}
	nodePool.SetGroupVersionKind(kafkaNodePoolGVK)
	key := types.NamespacedName{Name: mq.Name + KafkaNodePoolNameSuffix, Namespace: mq.Namespace}
	if err := r.Get(ctx, key, nodePool); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get KafkaNodePool for suspension: %w", err)
	}
	if !isControlledByMessageQueue(mq, nodePool) {
		return fmt.Errorf("refuse to suspend unmanaged KafkaNodePool %s/%s", nodePool.GetNamespace(), nodePool.GetName())
	}
	replicas, found, err := unstructured.NestedInt64(nodePool.Object, "spec", "replicas")
	if err != nil {
		return fmt.Errorf("read KafkaNodePool replicas for suspension: %w", err)
	}
	if found && replicas == 0 {
		return nil
	}
	if err := unstructured.SetNestedField(nodePool.Object, int64(0), "spec", "replicas"); err != nil {
		return fmt.Errorf("set KafkaNodePool replicas for suspension: %w", err)
	}
	if err := r.Update(ctx, nodePool); err != nil {
		return fmt.Errorf("scale KafkaNodePool down for suspension: %w", err)
	}
	return nil
}

func isControlledByMessageQueue(mq *v1alpha1.MessageQueue, obj client.Object) bool {
	for _, owner := range obj.GetOwnerReferences() {
		if owner.APIVersion == v1alpha1.GroupVersion.String() &&
			owner.Kind == "MessageQueue" &&
			owner.Name == mq.Name &&
			owner.UID == mq.UID {
			return true
		}
	}
	return false
}

func validateExternalListener(listeners *v1alpha1.KafkaListeners) error {
	if listeners == nil || listeners.External == nil || !listeners.External.Enabled {
		return nil
	}
	listenerType := strings.ToLower(strings.TrimSpace(listeners.External.Type))
	if listenerType == "" {
		listenerType = "nodeport"
	}
	if listenerType != "nodeport" {
		return invalidSpecf("external listener type must be nodeport")
	}
	addressType := strings.TrimSpace(listeners.External.PreferredNodePortAddressType)
	if addressType == "" {
		addressType = "InternalIP"
	}
	validAddressTypes := map[string]bool{
		"ExternalDNS": true,
		"ExternalIP":  true,
		"Hostname":    true,
		"InternalDNS": true,
		"InternalIP":  true,
	}
	if !validAddressTypes[addressType] {
		return invalidSpecf("external node address type %q is invalid", addressType)
	}
	if len(listeners.External.BootstrapAlternativeNames) > 16 {
		return invalidSpecf("external bootstrap alternative names must contain at most 16 entries")
	}
	for _, rawName := range listeners.External.BootstrapAlternativeNames {
		name := strings.TrimSpace(rawName)
		if name == "" || len(name) > 253 || (net.ParseIP(name) == nil && !dnsSubdomainRe.MatchString(strings.ToLower(name))) {
			return invalidSpecf("external bootstrap alternative names must be IP addresses or DNS names")
		}
	}
	return nil
}

func managedPodTemplate() map[string]interface{} {
	return map[string]interface{}{
		"pod": map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": map[string]interface{}{
					"messagequeue.sealos.io/managed": "true",
					"messagequeue.sealos.io/engine":  "kafka",
				},
			},
		},
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
