// Package v1alpha1 contains the MessageQueue API.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	GroupVersion = schema.GroupVersion{Group: "messagequeue.sealos.io", Version: "v1alpha1"}

	SchemeBuilder = func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(GroupVersion, &MessageQueue{}, &MessageQueueList{})
		metav1.AddToGroupVersion(scheme, GroupVersion)
		return nil
	}
)

// AddToScheme registers MessageQueue API types with a Kubernetes scheme.
func AddToScheme(scheme *runtime.Scheme) error {
	return SchemeBuilder(scheme)
}
