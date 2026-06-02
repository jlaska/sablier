package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// redisStop pauses the redis-operator reconciliation loop and scales the owned
// StatefulSet to zero. The two-step sequence (annotate first, then scale) prevents
// the operator from immediately restoring the replica count.
func (p *Provider) redisStop(ctx context.Context, config ParsedName) error {
	if p.dynamic == nil {
		return fmt.Errorf("redis support requires a dynamic client, none configured")
	}

	if err := p.redisSetSkipReconcile(ctx, config, true); err != nil {
		return err
	}

	return p.scaleRedisStatefulSet(ctx, config, 0)
}

// redisStart scales the owned StatefulSet back to one replica and then re-enables
// the redis-operator reconciliation loop.
func (p *Provider) redisStart(ctx context.Context, config ParsedName) error {
	if p.dynamic == nil {
		return fmt.Errorf("redis support requires a dynamic client, none configured")
	}

	if err := p.scaleRedisStatefulSet(ctx, config, 1); err != nil {
		return err
	}

	return p.redisSetSkipReconcile(ctx, config, false)
}

// redisSetSkipReconcile sets or removes the skip-reconcile annotation on the Redis CR.
// When skip is true the annotation is set to "true"; when false the annotation key is
// removed via a JSON merge patch null value.
func (p *Provider) redisSetSkipReconcile(ctx context.Context, config ParsedName, skip bool) error {
	var patch []byte
	var err error

	if skip {
		patch, err = json.Marshal(map[string]any{
			"metadata": map[string]any{
				"annotations": map[string]any{
					redisSkipReconcileAnnotation: "true",
				},
			},
		})
	} else {
		// A JSON merge patch null value removes the key entirely.
		patch, err = json.Marshal(map[string]any{
			"metadata": map[string]any{
				"annotations": map[string]any{
					redisSkipReconcileAnnotation: nil,
				},
			},
		})
	}
	if err != nil {
		return fmt.Errorf("cannot marshal skip-reconcile patch for redis %s/%s: %w", config.Namespace, config.Name, err)
	}

	_, err = p.dynamic.Resource(redisGVR).Namespace(config.Namespace).Patch(
		ctx, config.Name, types.MergePatchType, patch, metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("cannot set skip-reconcile=%v on redis %s/%s: %w", skip, config.Namespace, config.Name, err)
	}

	return nil
}

// scaleRedisStatefulSet scales the StatefulSet owned by the Redis CR to the given
// replica count. The StatefulSet name matches the Redis CR name (operator convention).
func (p *Provider) scaleRedisStatefulSet(ctx context.Context, config ParsedName, replicas int32) error {
	sts := p.Client.AppsV1().StatefulSets(config.Namespace)
	s, err := sts.GetScale(ctx, config.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("cannot get scale for redis statefulset %s/%s: %w", config.Namespace, config.Name, err)
	}

	s.Spec.Replicas = replicas
	_, err = sts.UpdateScale(ctx, config.Name, s, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("cannot scale redis statefulset %s/%s to %d: %w", config.Namespace, config.Name, replicas, err)
	}

	return nil
}
