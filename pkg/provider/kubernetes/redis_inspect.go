package kubernetes

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/sablierapp/sablier/pkg/sablier"
)

// RedisInspect reports the state of an OT-CONTAINER-KIT Redis instance. Status is
// derived from the skip-reconcile annotation and the replica count of the owned
// StatefulSet (which shares the same name as the Redis CR):
//   - skip-reconcile == "true" AND readyReplicas == 0  -> Stopped
//   - readyReplicas >= 1                                -> Ready
//   - otherwise                                        -> Starting
func (p *Provider) RedisInspect(ctx context.Context, config ParsedName) (sablier.InstanceInfo, error) {
	if p.dynamic == nil {
		return sablier.InstanceInfo{}, fmt.Errorf("redis support requires a dynamic client, none configured")
	}

	u, err := p.dynamic.Resource(redisGVR).Namespace(config.Namespace).Get(ctx, config.Name, metav1.GetOptions{})
	if err != nil {
		return sablier.InstanceInfo{}, fmt.Errorf("error getting redis %s/%s: %w", config.Namespace, config.Name, err)
	}

	sts, err := p.Client.AppsV1().StatefulSets(config.Namespace).Get(ctx, config.Name, metav1.GetOptions{})
	if err != nil {
		return sablier.InstanceInfo{}, fmt.Errorf("error getting statefulset for redis %s/%s: %w", config.Namespace, config.Name, err)
	}

	skipReconcile := u.GetAnnotations()[redisSkipReconcileAnnotation] == "true"
	readyReplicas := sts.Status.ReadyReplicas

	var status sablier.InstanceStatus
	switch {
	case skipReconcile && readyReplicas == 0:
		status = sablier.InstanceStatusStopped
	case readyReplicas >= 1:
		status = sablier.InstanceStatusReady
	default:
		status = sablier.InstanceStatusStarting
	}

	p.l.DebugContext(ctx, "redis inspected",
		"redis", config.Name, "namespace", config.Namespace,
		"skipReconcile", skipReconcile, "readyReplicas", readyReplicas,
	)

	labels := u.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	// Derive the container image from the Redis CR spec.
	image, _, _ := unstructured.NestedString(u.Object, "spec", "kubernetesConfig", "image")

	info := sablier.InstanceInfo{
		Name:            config.Original,
		CurrentReplicas: readyReplicas,
		DesiredReplicas: config.Replicas,
		Status:          status,
		Provider:        sablier.ProviderKubernetes,
	}
	sablier.PopulateEnabledAndGroup(&info, labels)
	info.Kubernetes = &sablier.KubernetesWorkloadInfo{
		Namespace: config.Namespace,
		Kind:      KindRedis,
		Image:     image,
		Labels:    labels,
	}

	return info, nil
}
