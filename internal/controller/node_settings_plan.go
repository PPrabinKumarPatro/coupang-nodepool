package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const nodeSettingsAnnotation = "nodes.example.com/managed-settings"

// The Node carries this receipt even after membership changes. Pool status
// tracks lifecycle progress; this record retains values needed for safe cleanup.
type nodeSettingsOwnership struct {
	PoolUID types.UID         `json:"poolUID"`
	Labels  map[string]string `json:"labels,omitempty"`
	Taints  []corev1.Taint    `json:"taints,omitempty"`
}

// planNodeSettings returns an independent Node copy.
// It makes no Kubernetes API calls.
// Empty desired settings clean up this pool UID's ownership.
// Comparing desired, live and last-written values avoids treating every
// matching label or taint as something this controller owns.
func planNodeSettings(
	node *corev1.Node,
	poolUID types.
		UID,
	desiredLabels map[string]string,
	desiredTaints []corev1.Taint,
) (*corev1.Node, error) {
	if node == nil || poolUID == "" {
		return nil, fmt.Errorf("node and nonempty pool UID are required")
	}
	if err := nodeSettingsValidateLabels(desiredLabels); err != nil {
		return nil, fmt.Errorf("desired labels: %w", err)
	}
	wanted, err := nodeSettingsTaintMap(desiredTaints, false)
	if err != nil {
		return nil, fmt.Errorf("desired taints: %w", err)
	}
	// Empty desired settings still allow owned cleanup after a worker is promoted
	// to a protected role; the caller separately enforces readiness for that cleanup.
	if protectedNode(node) && len(desiredLabels)+len(desiredTaints) != 0 {
		return nil, fmt.Errorf("protected Node %q cannot receive NodePool desired settings", node.Name)
	}

	var old nodeSettingsOwnership
	if raw, found := node.Annotations[nodeSettingsAnnotation]; found {
		if err := nodeSettingsDecode(raw, &old); err != nil {
			return nil, fmt.Errorf("ownership annotation: %w", err)
		}
		// Pool names can be reused; only the original UID may clean this record.
		if old.PoolUID != poolUID {
			return nil, fmt.Errorf("ownership annotation has a different or missing pool UID")
		}
	}
	if err := nodeSettingsValidateLabels(old.Labels); err != nil {
		// Do not let a legacy receipt remove reserved membership or role labels.
		return nil, fmt.Errorf("owned labels require manual repair: %w", err)
	}
	ownedTaints, err := nodeSettingsTaintMap(old.Taints, false)
	if err != nil {
		return nil, fmt.Errorf("owned taints: %w", err)
	}
	current, err := nodeSettingsTaintMap(node.Spec.Taints, true)
	if err != nil {
		return nil, fmt.Errorf("current taints: %w", err)
	}

	out := node.DeepCopy()
	next := nodeSettingsOwnership{
		PoolUID: poolUID,
		Labels:  make(map[string]string),
	}
	for _, key := range nodeSettingsKeys(old.Labels, desiredLabels) {
		previous, owned := old.Labels[key]
		value, exists := out.Labels[key]
		want, needed := desiredLabels[key]

		// Do not undo external edits: report a conflict if still desired,
		// otherwise preserve the value and omit it from the next ownership record.
		if owned && exists && value != previous {
			if needed {
				return nil, fmt.Errorf("owned label %q changed externally", key)
			}
			continue
		}
		if !needed {
			if owned {
				delete(out.Labels, key)
			}
			continue
		}
		// A preexisting matching value remains unowned, so cleanup preserves it.
		if exists && !owned {
			if value != want {
				return nil, fmt.Errorf("unowned label %q conflicts", key)
			}
			continue
		}
		if out.Labels == nil {
			out.Labels = make(map[string]string)
		}
		out.Labels[key] = want
		next.Labels[key] = want
	}

	nextTaints := make(map[string]corev1.Taint)
	updates := make(map[string]corev1.Taint)
	removals := make(map[string]bool)
	for _, id := range nodeSettingsKeys(ownedTaints, wanted) {
		previous, owned := ownedTaints[id]
		value, exists := current[id]
		want, needed := wanted[id]

		// A timeAdded value also counts as an external change: this planner
		// never sets it, even when managing a NoExecute taint.
		if owned && exists &&
			(value.Value != previous.Value || value.TimeAdded != nil) {
			if needed {
				return nil, fmt.Errorf("owned taint %q/%s changed externally",
					value.Key, value.Effect)
			}
			continue
		}
		if !needed {
			if owned && exists {
				removals[id] = true
			}
			continue
		}
		if exists && !owned {
			if value.Value != want.Value {
				return nil, fmt.Errorf("unowned taint %q/%s conflicts",
					want.Key, want.Effect)
			}
			continue
		}
		if !exists || value.Value != want.Value {
			updates[id] = want
		}
		nextTaints[id] = want
	}

	if len(updates)+len(removals) != 0 {
		taints := make([]corev1.Taint, 0, len(out.Spec.Taints)+len(updates))
		for _, value := range out.Spec.Taints {
			id := nodeSettingsTaintID(value)
			if removals[id] {
				continue
			}
			if replacement, found := updates[id]; found {
				value = replacement
			}
			taints = append(taints, value)
		}
		for _, id := range nodeSettingsKeys(updates, nil) {
			if _, exists := current[id]; !exists {
				taints = append(taints, updates[id])
			}
		}
		out.Spec.Taints = taints
	}

	if len(next.Labels)+len(nextTaints) == 0 {
		// With no owned settings left, release the receipt so another pool can claim.
		delete(out.Annotations, nodeSettingsAnnotation)
	} else if !maps.Equal(old.Labels, next.Labels) ||
		!maps.Equal(ownedTaints, nextTaints) {
		// Keep the annotation unchanged on no-op passes, including its JSON encoding.
		for _, id := range nodeSettingsKeys(nextTaints, nil) {
			next.Taints = append(next.Taints, nextTaints[id])
		}
		encoded, err := json.Marshal(next)
		if err != nil {
			return nil, fmt.Errorf("encode ownership: %w", err)
		}
		if out.Annotations == nil {
			out.Annotations = make(map[string]string)
		}
		out.Annotations[nodeSettingsAnnotation] = string(encoded)
	}
	return out, nil
}

func nodeSettingsValidateLabels(labels map[string]string) error {
	for _, key := range nodeSettingsKeys(labels, nil) {
		if key == membershipLabel {
			return fmt.Errorf("membership label %q is reserved", key)
		}
		if key == controlPlaneRoleLabel || key == legacyMasterRoleLabel {
			return fmt.Errorf("role label %q is reserved for cluster administration", key)
		}
		if problems := validation.IsQualifiedName(key); len(problems) != 0 {
			return fmt.Errorf("key %q: %v", key, problems)
		}
		if problems := validation.IsValidLabelValue(labels[key]); len(problems) != 0 {
			return fmt.Errorf("value for %q: %v", key, problems)
		}
	}
	return nil
}

func nodeSettingsTaintMap(
	taints []corev1.Taint, allowTimeAdded bool,
) (map[string]corev1.Taint, error) {
	result := make(map[string]corev1.Taint, len(taints))
	for _, taint := range taints {
		if problems := validation.IsQualifiedName(taint.Key); len(problems) != 0 {
			return nil, fmt.Errorf("key %q: %v", taint.Key, problems)
		}
		if problems := validation.IsValidLabelValue(taint.Value); len(problems) != 0 {
			return nil, fmt.Errorf("value for %q: %v", taint.Key, problems)
		}
		switch taint.Effect {
		case corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule,
			corev1.TaintEffectNoExecute:
		default:
			return nil, fmt.Errorf("taint %q has invalid effect %q",
				taint.Key, taint.Effect)
		}
		if !allowTimeAdded && taint.TimeAdded != nil {
			return nil, fmt.Errorf("taint %q: timeAdded is unsupported", taint.Key)
		}
		id := nodeSettingsTaintID(taint)
		if _, exists := result[id]; exists {
			return nil, fmt.Errorf("duplicate taint %q/%s", taint.Key, taint.Effect)
		}
		result[id] = taint
	}
	return result, nil
}

func nodeSettingsTaintID(taint corev1.Taint) string {
	return taint.Key + "\x00" + string(taint.Effect)
}

func nodeSettingsKeys[V any](a, b map[string]V) []string {
	keys := make(map[string]struct{}, len(a)+len(b))
	for key := range a {
		keys[key] = struct{}{}
	}
	for key := range b {
		keys[key] = struct{}{}
	}
	return slices.Sorted(maps.Keys(keys))
}

// Reject duplicate keys, unknown fields, trailing JSON and deep nesting.
// encoding/json normally accepts duplicate keys; ambiguous ownership must
// not decide which live settings the controller is allowed to remove.
func nodeSettingsDecode(raw string, record *nodeSettingsOwnership) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 8 {
			return fmt.Errorf("ownership JSON nesting exceeds limit")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, container := token.(json.Delim)
		if !container {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				token, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := token.(string)
				if !ok || seen[key] {
					return fmt.Errorf("invalid or duplicate JSON object key")
				}
				seen[key] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
		_, err = decoder.Token()
		return err
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("trailing or invalid ownership JSON")
	}
	decoder = json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(record)
}
