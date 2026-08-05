package controller

import (
	"context"
	"strings"
	"testing"

	v1alpha1 "github.com/labring/messagequeue/controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rbacv1 scheme: %v", err)
	}
	if err := v1alpha1.SchemeBuilder(scheme); err != nil {
		t.Fatalf("add MessageQueue scheme: %v", err)
	}
	return scheme
}

func TestNormalizeSpecValidation(t *testing.T) {
	if _, err := normalizeSpec(v1alpha1.MessageQueueSpec{Engine: "rabbitmq"}); err == nil {
		t.Fatal("expected unsupported engine error")
	}
	if _, err := normalizeSpec(v1alpha1.MessageQueueSpec{Kafka: v1alpha1.KafkaSpec{Version: "3.7.0"}}); err == nil {
		t.Fatal("expected unsupported Kafka version error")
	}
	if _, err := normalizeSpec(v1alpha1.MessageQueueSpec{Topology: v1alpha1.TopologySpec{Mode: "zookeeper"}}); err == nil {
		t.Fatal("expected unsupported topology error")
	}
	got, err := normalizeSpec(v1alpha1.MessageQueueSpec{})
	if err != nil {
		t.Fatalf("default spec rejected: %v", err)
	}
	if got.version != v1alpha1.DefaultKafkaVersion || got.replicas != 1 || got.storageSize != v1alpha1.DefaultStorageSize {
		t.Fatalf("unexpected defaults: %+v", got)
	}
}

func TestRenderKRaftResources(t *testing.T) {
	mq := &v1alpha1.MessageQueue{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns-demo"}}
	desired, err := normalizeSpec(v1alpha1.MessageQueueSpec{
		Kafka:     v1alpha1.KafkaSpec{Version: "3.9.0", Replicas: 3},
		Storage:   v1alpha1.StorageSpec{Size: "20Gi", ClassName: "fast"},
		Resources: v1alpha1.ResourceSpec{CPU: "500m", Memory: "1Gi"},
	})
	if err != nil {
		t.Fatalf("normalize spec: %v", err)
	}
	kafka := RenderKafka(mq, desired)
	if kafka.GroupVersionKind() != kafkaGVK {
		t.Fatalf("unexpected Kafka GVK: %s", kafka.GroupVersionKind())
	}
	if got, _, _ := unstructured.NestedString(kafka.Object, "spec", "kafka", "version"); got != "3.9.0" {
		t.Fatalf("unexpected Kafka version: %q", got)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(kafka.Object, "spec", "kafka", "replicas"); found {
		t.Fatal("Kafka.spec.kafka.replicas must be omitted when node pools are enabled")
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(kafka.Object, "spec", "kafka", "storage"); found {
		t.Fatal("Kafka.spec.kafka.storage must be omitted when node pools are enabled")
	}
	if got, found := kafka.GetAnnotations()["strimzi.io/kraft"]; !found || got != "enabled" {
		t.Fatalf("KRaft annotation missing: %v", kafka.GetAnnotations())
	}
	listeners, found, err := unstructured.NestedSlice(kafka.Object, "spec", "kafka", "listeners")
	if err != nil || !found || len(listeners) != 2 {
		t.Fatalf("expected two authenticated listeners, found=%v err=%v listeners=%v", found, err, listeners)
	}
	for _, raw := range listeners {
		listener := raw.(map[string]interface{})
		auth := listener["authentication"].(map[string]interface{})
		if auth["type"] != "scram-sha-512" {
			t.Fatalf("listener is missing SCRAM authentication: %v", listener)
		}
	}
	assertManagedPodTemplate(t, kafka.Object, "spec", "kafka", "template")
	for _, operator := range []string{"topicOperator", "userOperator"} {
		requests, found, err := unstructured.NestedStringMap(kafka.Object, "spec", "entityOperator", operator, "resources", "requests")
		if err != nil || !found {
			t.Fatalf("%s resource requests missing: found=%v err=%v", operator, found, err)
		}
		if requests["cpu"] != "200m" || requests["memory"] != "256Mi" {
			t.Fatalf("unexpected %s resource requests: %v", operator, requests)
		}
		limits, found, err := unstructured.NestedStringMap(kafka.Object, "spec", "entityOperator", operator, "resources", "limits")
		if err != nil || !found {
			t.Fatalf("%s resource limits missing: found=%v err=%v", operator, found, err)
		}
		if limits["cpu"] != "500m" || limits["memory"] != "512Mi" {
			t.Fatalf("unexpected %s resource limits: %v", operator, limits)
		}
	}
	nodePool := RenderKafkaNodePool(mq, desired)
	if nodePool.GetName() != "orders"+KafkaNodePoolNameSuffix {
		t.Fatalf("unexpected node pool name: %s", nodePool.GetName())
	}
	roles, found, err := unstructured.NestedStringSlice(nodePool.Object, "spec", "roles")
	if err != nil || !found || len(roles) != 2 {
		t.Fatalf("expected broker/controller roles, found=%v err=%v roles=%v", found, err, roles)
	}
	if roles[0] != "broker" || roles[1] != "controller" {
		t.Fatalf("unexpected KRaft roles: %v", roles)
	}
	storage, found, err := unstructured.NestedMap(nodePool.Object, "spec", "storage")
	if err != nil || !found || storage["type"] != "jbod" {
		t.Fatalf("expected JBOD node pool storage, found=%v err=%v storage=%v", found, err, storage)
	}
	volumes, found, err := unstructured.NestedSlice(nodePool.Object, "spec", "storage", "volumes")
	if err != nil || !found || len(volumes) != 1 {
		t.Fatalf("expected one JBOD volume, found=%v err=%v volumes=%v", found, err, volumes)
	}
	volume, ok := volumes[0].(map[string]interface{})
	if !ok || volume["kraftMetadata"] != "shared" {
		t.Fatalf("expected shared KRaft metadata volume, got %v", volumes[0])
	}
	if nodePool.GetLabels()["strimzi.io/cluster"] != mq.Name {
		t.Fatalf("node pool cluster label missing: %v", nodePool.GetLabels())
	}
	assertManagedPodTemplate(t, nodePool.Object, "spec", "template")
	user := RenderKafkaUser(mq)
	if user.GetName() != "orders"+KafkaUserNameSuffix || user.GetLabels()["strimzi.io/cluster"] != mq.Name {
		t.Fatalf("unexpected KafkaUser identity: name=%s labels=%v", user.GetName(), user.GetLabels())
	}
	if authType, _, _ := unstructured.NestedString(user.Object, "spec", "authentication", "type"); authType != "scram-sha-512" {
		t.Fatalf("unexpected KafkaUser authentication: %q", authType)
	}
	acls, found, err := unstructured.NestedSlice(user.Object, "spec", "authorization", "acls")
	if err != nil || !found || len(acls) != 3 {
		t.Fatalf("expected topic/group/cluster ACLs, found=%v err=%v acls=%v", found, err, acls)
	}
}

func TestRenderKafkaIncludesExternalListenerAndMetricsConfig(t *testing.T) {
	mq := &v1alpha1.MessageQueue{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns-demo"},
		Spec: v1alpha1.MessageQueueSpec{
			Kafka: v1alpha1.KafkaSpec{
				Version:  "3.9.0",
				Replicas: 3,
				Listeners: &v1alpha1.KafkaListeners{
					External: &v1alpha1.ExternalListener{
						Enabled:                      true,
						PreferredNodePortAddressType: "InternalIP",
						BootstrapAlternativeNames:    []string{"192.168.0.62"},
					},
				},
			},
		},
	}
	desired, err := normalizeSpec(mq.Spec)
	if err != nil {
		t.Fatalf("normalize spec: %v", err)
	}
	kafka := RenderKafka(mq, desired)
	listeners, found, err := unstructured.NestedSlice(kafka.Object, "spec", "kafka", "listeners")
	if err != nil || !found || len(listeners) != 3 {
		t.Fatalf("expected external listener to be rendered, found=%v err=%v listeners=%v", found, err, listeners)
	}
	external := listeners[2].(map[string]interface{})
	if external["type"] != "nodeport" || external["port"] != int64(9094) {
		t.Fatalf("unexpected external listener: %#v", external)
	}
	alternativeNames, found, err := unstructured.NestedStringSlice(external, "configuration", "bootstrap", "alternativeNames")
	if err != nil || !found || len(alternativeNames) != 1 || alternativeNames[0] != "192.168.0.62" {
		t.Fatalf("external listener bootstrap alternative names missing: found=%v err=%v names=%v", found, err, alternativeNames)
	}
	if cfg, found, err := unstructured.NestedMap(kafka.Object, "spec", "kafka", "metricsConfig"); err != nil || !found {
		t.Fatalf("metricsConfig missing: found=%v err=%v", found, err)
	} else {
		valueFrom, found, _ := unstructured.NestedMap(cfg, "valueFrom")
		if !found {
			t.Fatal("metricsConfig.valueFrom missing")
		}
		ref, found, _ := unstructured.NestedMap(valueFrom, "configMapKeyRef")
		if !found || ref["name"] != metricsConfigMapName(mq) || ref["key"] != kafkaMetricsConfigKey {
			t.Fatalf("metrics config map ref = %#v", ref)
		}
	}
	if exporter, found, err := unstructured.NestedMap(kafka.Object, "spec", "kafkaExporter"); err != nil || !found {
		t.Fatalf("kafkaExporter missing: found=%v err=%v", found, err)
	} else if exporter["topicRegex"] != ".*" || exporter["groupRegex"] != ".*" {
		t.Fatalf("unexpected kafkaExporter config: %#v", exporter)
	}
	assertManagedPodTemplate(t, kafka.Object, "spec", "kafkaExporter", "template")
}

func assertManagedPodTemplate(t *testing.T, object map[string]interface{}, fields ...string) {
	t.Helper()
	labels, found, err := unstructured.NestedStringMap(object, append(fields, "pod", "metadata", "labels")...)
	if err != nil || !found {
		t.Fatalf("managed pod template labels missing at %v: found=%v err=%v", fields, found, err)
	}
	if labels["messagequeue.sealos.io/managed"] != "true" || labels["messagequeue.sealos.io/engine"] != "kafka" {
		t.Fatalf("unexpected managed pod template labels at %v: %v", fields, labels)
	}
}

func TestReconcileCreatesOwnedResourcesIdempotently(t *testing.T) {
	scheme := testScheme(t)
	mq := &v1alpha1.MessageQueue{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "MessageQueue"},
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns-demo", Generation: 1},
		Spec:       v1alpha1.MessageQueueSpec{Kafka: v1alpha1.KafkaSpec{Replicas: 1}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MessageQueue{}).WithObjects(mq).Build()
	r := NewReconciler(c)
	req := reconcileRequest(mq)
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	kafka := &unstructured.Unstructured{}
	kafka.SetGroupVersionKind(kafkaGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Name: "orders", Namespace: "ns-demo"}, kafka); err != nil {
		t.Fatalf("get Kafka: %v", err)
	}
	if len(kafka.GetOwnerReferences()) != 1 || kafka.GetOwnerReferences()[0].Name != mq.Name {
		t.Fatalf("Kafka owner reference missing: %+v", kafka.GetOwnerReferences())
	}
	nodePool := &unstructured.Unstructured{}
	nodePool.SetGroupVersionKind(kafkaNodePoolGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Name: "orders" + KafkaNodePoolNameSuffix, Namespace: "ns-demo"}, nodePool); err != nil {
		t.Fatalf("get KafkaNodePool: %v", err)
	}
	user := &unstructured.Unstructured{}
	user.SetGroupVersionKind(kafkaUserGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Name: "orders" + KafkaUserNameSuffix, Namespace: "ns-demo"}, user); err != nil {
		t.Fatalf("get KafkaUser: %v", err)
	}
	if len(user.GetOwnerReferences()) != 1 || user.GetOwnerReferences()[0].Name != mq.Name {
		t.Fatalf("KafkaUser owner reference missing: %+v", user.GetOwnerReferences())
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1beta2", Kind: "KafkaList"})
	if err := c.List(context.Background(), list, client.InNamespace("ns-demo")); err != nil {
		t.Fatalf("list Kafka: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected one Kafka after idempotent reconciles, got %d", len(list.Items))
	}
	metricsConfigMap := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: metricsConfigMapName(mq), Namespace: "ns-demo"}, metricsConfigMap); err != nil {
		t.Fatalf("get Kafka metrics ConfigMap: %v", err)
	}
	role := &rbacv1.Role{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: credentialAccessName(mq), Namespace: "ns-demo"}, role); err != nil {
		t.Fatalf("get credential Role: %v", err)
	}
	if len(role.Rules) != 1 || len(role.Rules[0].ResourceNames) != 2 {
		t.Fatalf("credential Role should be limited to derived Secret names: %#v", role.Rules)
	}
	for _, forbidden := range []string{"other-secret", "orders"} {
		for _, resourceName := range role.Rules[0].ResourceNames {
			if resourceName == forbidden {
				t.Fatalf("credential Role granted unexpected Secret name %q", forbidden)
			}
		}
	}
	roleBinding := &rbacv1.RoleBinding{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: credentialAccessName(mq), Namespace: "ns-demo"}, roleBinding); err != nil {
		t.Fatalf("get credential RoleBinding: %v", err)
	}
	if len(roleBinding.Subjects) != 1 || roleBinding.Subjects[0].Kind != rbacv1.ServiceAccountKind {
		t.Fatalf("credential RoleBinding should bind the backend ServiceAccount: %#v", roleBinding.Subjects)
	}
	stored := &v1alpha1.MessageQueue{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mq), stored); err != nil {
		t.Fatalf("get MessageQueue: %v", err)
	}
	if stored.Status.Phase != v1alpha1.PhaseProvisioning {
		t.Fatalf("expected provisioning status, got %+v", stored.Status)
	}
	if stored.Status.ClientSecretRef != "orders"+KafkaUserNameSuffix {
		t.Fatalf("unexpected client Secret reference: %q", stored.Status.ClientSecretRef)
	}
}

func TestKafkaReadyStatus(t *testing.T) {
	scheme := testScheme(t)
	mq := &v1alpha1.MessageQueue{ObjectMeta: metav1.ObjectMeta{Name: "ready", Namespace: "ns-demo", Generation: 1}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MessageQueue{}).WithObjects(mq).Build()
	r := NewReconciler(c)
	if _, err := r.Reconcile(context.Background(), reconcileRequest(mq)); err != nil {
		t.Fatalf("provision reconcile: %v", err)
	}
	kafka := &unstructured.Unstructured{}
	kafka.SetGroupVersionKind(kafkaGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Name: "ready", Namespace: "ns-demo"}, kafka); err != nil {
		t.Fatalf("get Kafka: %v", err)
	}
	_ = unstructured.SetNestedField(kafka.Object, []interface{}{map[string]interface{}{"type": "Ready", "status": "True", "reason": "KafkaIsReady"}}, "status", "conditions")
	_ = unstructured.SetNestedField(kafka.Object, int64(1), "status", "readyReplicas")
	if err := c.Update(context.Background(), kafka); err != nil {
		t.Fatalf("update Kafka status: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), reconcileRequest(mq)); err != nil {
		t.Fatalf("ready reconcile: %v", err)
	}
	stored := &v1alpha1.MessageQueue{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mq), stored); err != nil {
		t.Fatalf("get MessageQueue: %v", err)
	}
	if stored.Status.Phase != v1alpha1.PhaseReady || stored.Status.ReadyReplicas != 1 {
		t.Fatalf("unexpected ready status: %+v", stored.Status)
	}
}

func TestKafkaReadyStatusExportsExternalEndpoints(t *testing.T) {
	scheme := testScheme(t)
	mq := &v1alpha1.MessageQueue{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "MessageQueue"},
		ObjectMeta: metav1.ObjectMeta{Name: "ready", Namespace: "ns-demo", Generation: 1},
		Spec: v1alpha1.MessageQueueSpec{
			Kafka: v1alpha1.KafkaSpec{
				Replicas:  1,
				Listeners: &v1alpha1.KafkaListeners{External: &v1alpha1.ExternalListener{Enabled: true}},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MessageQueue{}).WithObjects(mq).Build()
	r := NewReconciler(c)
	if _, err := r.Reconcile(context.Background(), reconcileRequest(mq)); err != nil {
		t.Fatalf("provision reconcile: %v", err)
	}
	kafka := &unstructured.Unstructured{}
	kafka.SetGroupVersionKind(kafkaGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Name: "ready", Namespace: "ns-demo"}, kafka); err != nil {
		t.Fatalf("get Kafka: %v", err)
	}
	_ = unstructured.SetNestedField(kafka.Object, []interface{}{
		map[string]interface{}{
			"name":             "external",
			"bootstrapServers": "192.168.0.62:31234",
			"addresses": []interface{}{
				map[string]interface{}{"host": "192.168.0.62", "port": int64(31234)},
			},
		},
	}, "status", "listeners")
	_ = unstructured.SetNestedField(kafka.Object, []interface{}{map[string]interface{}{"type": "Ready", "status": "True", "reason": "KafkaIsReady"}}, "status", "conditions")
	_ = unstructured.SetNestedField(kafka.Object, int64(1), "status", "readyReplicas")
	if err := c.Update(context.Background(), kafka); err != nil {
		t.Fatalf("update Kafka status: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), reconcileRequest(mq)); err != nil {
		t.Fatalf("ready reconcile: %v", err)
	}
	stored := &v1alpha1.MessageQueue{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mq), stored); err != nil {
		t.Fatalf("get MessageQueue: %v", err)
	}
	if len(stored.Status.ExternalEndpoints) == 0 || stored.Status.ExternalEndpoints[0] != "192.168.0.62:31234" {
		t.Fatalf("unexpected external endpoints: %#v", stored.Status.ExternalEndpoints)
	}
}

func TestReconcileTerminalPreconditions(t *testing.T) {
	tests := []struct {
		name  string
		spec  v1alpha1.MessageQueueSpec
		phase string
	}{
		{name: "invalid", spec: v1alpha1.MessageQueueSpec{Kafka: v1alpha1.KafkaSpec{Version: "3.7.0"}}, phase: v1alpha1.PhaseFailed},
		{name: "suspended", spec: v1alpha1.MessageQueueSpec{Suspend: true}, phase: v1alpha1.PhaseSuspended},
		{name: "invalid-external-listener", spec: v1alpha1.MessageQueueSpec{Kafka: v1alpha1.KafkaSpec{Listeners: &v1alpha1.KafkaListeners{External: &v1alpha1.ExternalListener{Enabled: true, Type: "loadbalancer"}}}}, phase: v1alpha1.PhaseFailed},
		{name: "invalid-external-san", spec: v1alpha1.MessageQueueSpec{Kafka: v1alpha1.KafkaSpec{Listeners: &v1alpha1.KafkaListeners{External: &v1alpha1.ExternalListener{Enabled: true, BootstrapAlternativeNames: []string{"bad name"}}}}}, phase: v1alpha1.PhaseFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := testScheme(t)
			mq := &v1alpha1.MessageQueue{ObjectMeta: metav1.ObjectMeta{Name: tt.name, Namespace: "ns-demo", Generation: 1}, Spec: tt.spec}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MessageQueue{}).WithObjects(mq).Build()
			if _, err := NewReconciler(c).Reconcile(context.Background(), reconcileRequest(mq)); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			stored := &v1alpha1.MessageQueue{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(mq), stored); err != nil {
				t.Fatalf("get MessageQueue: %v", err)
			}
			if stored.Status.Phase != tt.phase {
				t.Fatalf("expected %s phase, got %+v", tt.phase, stored.Status)
			}
			kafka := &unstructured.Unstructured{}
			kafka.SetGroupVersionKind(kafkaGVK)
			err := c.Get(context.Background(), types.NamespacedName{Name: mq.Name, Namespace: mq.Namespace}, kafka)
			if !apierrors.IsNotFound(err) {
				t.Fatalf("precondition should not create Kafka, got err=%v object=%v", err, kafka.Object)
			}
		})
	}
}

func TestReconcileSuspensionScalesExistingNodePoolToZero(t *testing.T) {
	scheme := testScheme(t)
	mq := &v1alpha1.MessageQueue{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "MessageQueue"},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "orders",
			Namespace:  "ns-demo",
			Generation: 2,
			UID:        types.UID("orders-uid"),
		},
		Spec: v1alpha1.MessageQueueSpec{
			Suspend: true,
			Kafka:   v1alpha1.KafkaSpec{Replicas: 3},
		},
		Status: v1alpha1.MessageQueueStatus{
			ReadyReplicas:     3,
			Endpoints:         []string{"orders-kafka-bootstrap.ns-demo.svc:9093"},
			ExternalEndpoints: []string{"192.168.0.62:31234"},
		},
	}
	nodePool := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": KafkaAPIVersion,
		"kind":       KafkaNodePoolKind,
		"metadata": map[string]interface{}{
			"name":      "orders" + KafkaNodePoolNameSuffix,
			"namespace": "ns-demo",
			"labels": map[string]interface{}{
				"strimzi.io/cluster": "orders",
			},
		},
		"spec": map[string]interface{}{
			"replicas": int64(3),
		},
	}}
	nodePool.SetGroupVersionKind(kafkaNodePoolGVK)
	nodePool.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(mq, v1alpha1.GroupVersion.WithKind("MessageQueue"))})

	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MessageQueue{}).WithObjects(mq, nodePool).Build()
	r := NewReconciler(c)
	if _, err := r.Reconcile(context.Background(), reconcileRequest(mq)); err != nil {
		t.Fatalf("suspend reconcile: %v", err)
	}
	storedNodePool := &unstructured.Unstructured{}
	storedNodePool.SetGroupVersionKind(kafkaNodePoolGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Name: "orders" + KafkaNodePoolNameSuffix, Namespace: "ns-demo"}, storedNodePool); err != nil {
		t.Fatalf("get KafkaNodePool: %v", err)
	}
	replicas, found, err := unstructured.NestedInt64(storedNodePool.Object, "spec", "replicas")
	if err != nil || !found || replicas != 0 {
		t.Fatalf("suspended MessageQueue must scale KafkaNodePool to 0, found=%v err=%v replicas=%d", found, err, replicas)
	}
	stored := &v1alpha1.MessageQueue{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mq), stored); err != nil {
		t.Fatalf("get MessageQueue: %v", err)
	}
	if stored.Status.Phase != v1alpha1.PhaseSuspended || stored.Status.ReadyReplicas != 0 {
		t.Fatalf("unexpected suspended status: %+v", stored.Status)
	}
	if len(stored.Status.Endpoints) != 0 || len(stored.Status.ExternalEndpoints) != 0 {
		t.Fatalf("suspended status must clear connection endpoints: %+v", stored.Status)
	}
	role := &rbacv1.Role{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: credentialAccessName(mq), Namespace: "ns-demo"}, role); err != nil {
		t.Fatalf("suspend must still reconcile credential Role: %v", err)
	}
	roleBinding := &rbacv1.RoleBinding{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: credentialAccessName(mq), Namespace: "ns-demo"}, roleBinding); err != nil {
		t.Fatalf("suspend must still reconcile credential RoleBinding: %v", err)
	}
}

func TestReconcileSuspensionRefusesUnmanagedNodePool(t *testing.T) {
	scheme := testScheme(t)
	mq := &v1alpha1.MessageQueue{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "MessageQueue"},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "orders",
			Namespace:  "ns-demo",
			Generation: 2,
			UID:        types.UID("orders-uid"),
		},
		Spec: v1alpha1.MessageQueueSpec{
			Suspend: true,
		},
	}
	nodePool := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": KafkaAPIVersion,
		"kind":       KafkaNodePoolKind,
		"metadata": map[string]interface{}{
			"name":      "orders" + KafkaNodePoolNameSuffix,
			"namespace": "ns-demo",
		},
		"spec": map[string]interface{}{
			"replicas": int64(3),
		},
	}}
	nodePool.SetGroupVersionKind(kafkaNodePoolGVK)

	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MessageQueue{}).WithObjects(mq, nodePool).Build()
	_, err := NewReconciler(c).Reconcile(context.Background(), reconcileRequest(mq))
	if err == nil || !strings.Contains(err.Error(), "unmanaged KafkaNodePool") {
		t.Fatalf("expected unmanaged NodePool error, got %v", err)
	}
	storedNodePool := &unstructured.Unstructured{}
	storedNodePool.SetGroupVersionKind(kafkaNodePoolGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Name: "orders" + KafkaNodePoolNameSuffix, Namespace: "ns-demo"}, storedNodePool); err != nil {
		t.Fatalf("get KafkaNodePool: %v", err)
	}
	replicas, found, err := unstructured.NestedInt64(storedNodePool.Object, "spec", "replicas")
	if err != nil || !found || replicas != 3 {
		t.Fatalf("unmanaged KafkaNodePool must not be scaled, found=%v err=%v replicas=%d", found, err, replicas)
	}
}

func reconcileRequest(mq *v1alpha1.MessageQueue) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: mq.Name, Namespace: mq.Namespace}}
}
