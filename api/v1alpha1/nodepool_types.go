/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// NodePoolSpec defines the desired configuration of  a NodePool
type NodePoolSpec struct {
	// Image is informational only; this controller does not provision nodes.
	// +kubebuilder:validation:MinLength=1
	// +required
	Image string `json:"image"`

	// Labels are applied to member nodes.
	// The key nodes.example.com/nodepool is reserved for membership.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Taints are identified by the combination of key and effect.
	// +listType=map
	// +listMapKey=key
	// +listMapKey=effect
	// +optional
	Taints []corev1.Taint `json:"taints,omitempty"`

	// UnreadyPolicy controls how persistently unready nodes are handled.
	// +required
	UnreadyPolicy UnreadyPolicy `json:"unreadyPolicy"`
}

// UnreadyPolicy defines the waiting period, action, and removal limit.
type UnreadyPolicy struct {
	// GracePeriod accepts whole seconds, minutes, or hours.
	// Examples: "0s", "30s", "10m", "2h".
	// Compound and fractional durations are not accepted.
	// +kubebuilder:validation:Pattern="^[0-9]{1,6}(s|m|h)$"
	// +required
	GracePeriod string `json:"gracePeriod"`

	// Action is explicitly selected; there is no default.
	// +kubebuilder:validation:Enum=Remove;Cordon;Ignore
	// +required
	Action string `json:"action"`

	// MaxConcurrentRemovals limits concurrent removals for this pool.
	// It remains required when Action is Cordon or Ignore, but is unused.
	// +kubebuilder:validation:Minimum=1
	// +required
	MaxConcurrentRemovals int32 `json:"maxConcurrentRemovals"`

	// DrainTimeout bounds attempts to evict Pods after a removal slot is reserved.
	// On expiry, remaining Pods block removal; they are never force-deleted.
	// +kubebuilder:default="5m"
	// +kubebuilder:validation:Pattern="^[1-9][0-9]{0,5}(s|m|h)$"
	// +optional
	DrainTimeout string `json:"drainTimeout,omitempty"`
}

// NodePoolStatus records the controller's latest observations.
type NodePoolStatus struct {
	// ReadyNodes is the number of member nodes observed as ready.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ReadyNodes int32 `json:"readyNodes,omitempty"`

	// Nodes records member-node lifecycle state, keyed by Node UID.
	// +listType=map
	// +listMapKey=uid
	// +optional
	Nodes []NodePoolNodeStatus `json:"nodes,omitempty"`

	// PendingSettingsCleanup retains known former members while settings
	// reconciliation is incomplete. These references do not count as members.
	// +listType=map
	// +listMapKey=uid
	// +optional
	PendingSettingsCleanup []NodePoolNodeReference `json:"pendingSettingsCleanup,omitempty"`

	// ObservedGeneration is the spec generation processed by the controller.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions report standard Ready and Progressing conditions.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// NodePoolNodeReference identifies a particular Node, including replacements.
type NodePoolNodeReference struct {
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`

	// +kubebuilder:validation:MinLength=1
	// +required
	UID string `json:"uid"`
}

// NodePoolNodeStatus records durable state for one specific Node object.
type NodePoolNodeStatus struct {
	// Name is the Kubernetes Node name.
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`

	// UID is the Node object's metadata.uid, not its name.
	// This additional field distinguishes recreated nodes.
	// +kubebuilder:validation:MinLength=1
	// +required
	UID string `json:"uid"`

	// Phase is the controller's recorded lifecycle phase.
	// +kubebuilder:validation:Enum=Ready;NotReady;Draining;Removing
	// +required
	Phase string `json:"phase"`

	// NotReadySince starts the current observed unready interval.
	// It is absent when no such interval is being tracked.
	// +optional
	NotReadySince *metav1.Time `json:"notReadySince,omitempty"`

	// ReadyTransitionTime is the last observed Ready-condition transition.
	// A change may indicate recovery that occurred between reconciles.
	// +optional
	ReadyTransitionTime *metav1.Time `json:"readyTransitionTime,omitempty"`

	// DrainStartedAt persists the removal reservation and timeout across restarts.
	// +optional
	DrainStartedAt *metav1.Time `json:"drainStartedAt,omitempty"`

	// Reason is a machine-readable explanation of the current phase.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message explains progress or the intervention needed to unblock it.
	// +optional
	Message string `json:"message,omitempty"`

	// Blocked means the controller will not bypass the reported safety constraint.
	// +optional
	Blocked bool `json:"blocked,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// NodePool is the Schema for the nodepools API
type NodePool struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of NodePool
	// +required
	Spec NodePoolSpec `json:"spec"`

	// status defines the observed state of NodePool
	// +optional
	Status NodePoolStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// NodePoolList contains a list of NodePool
type NodePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []NodePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &NodePool{}, &NodePoolList{})
		return nil
	})
}
