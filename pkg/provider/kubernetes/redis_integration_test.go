package kubernetes_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/neilotoole/slogt"
	"github.com/sablierapp/sablier/pkg/config"
	"github.com/sablierapp/sablier/pkg/provider/kubernetes"
	"github.com/sablierapp/sablier/pkg/sablier"
	"gotest.tools/v3/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	crdGVRForRedis = schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}
	redisIntegrationGVR = schema.GroupVersionResource{
		Group:    "redis.redis.opstreelabs.in",
		Version:  "v1beta2",
		Resource: "redis",
	}
	redisCRDOnce sync.Once
)

// ensureRedisCRD installs a minimal Redis CRD into the shared cluster (once) so
// the API server accepts Redis resources. It does not run the redis-operator: these
// tests verify Sablier's interactions with the Redis API (annotation, StatefulSet
// scaling), not Redis itself.
func ensureRedisCRD(ctx context.Context, t *testing.T, kind *kindContainer) {
	t.Helper()
	redisCRDOnce.Do(func() {
		crd := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata":   map[string]any{"name": "redis.redis.redis.opstreelabs.in"},
			"spec": map[string]any{
				"group": "redis.redis.opstreelabs.in",
				"scope": "Namespaced",
				"names": map[string]any{
					"plural":   "redis",
					"singular": "redis",
					"kind":     "Redis",
					"listKind": "RedisList",
				},
				"versions": []any{
					map[string]any{
						"name":    "v1beta2",
						"served":  true,
						"storage": true,
						"schema": map[string]any{
							"openAPIV3Schema": map[string]any{
								"type":                                 "object",
								"x-kubernetes-preserve-unknown-fields": true,
							},
						},
					},
				},
			},
		}}

		_, err := kind.dynamic.Resource(crdGVRForRedis).Create(ctx, crd, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("failed to create redis CRD: %v", err)
		}

		// Wait until the API server serves the new resource type.
		assert.NilError(t, waitForCRD(ctx, kind, redisIntegrationGVR))
	})
}

// createRedisAndStatefulSet creates a Redis CR and its companion StatefulSet in the
// test cluster. The StatefulSet simulates what the redis-operator would create.
func createRedisAndStatefulSet(ctx context.Context, t *testing.T, kind *kindContainer, name string, labels map[string]string, readyReplicas int32) {
	t.Helper()
	one := int32(1)

	// Create the Redis CR.
	redis := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "redis.redis.opstreelabs.in/v1beta2",
		"kind":       "Redis",
		"metadata": map[string]any{
			"name":      name,
			"namespace": "default",
			"labels":    toStringMap(labels),
		},
		"spec": map[string]any{
			"kubernetesConfig": map[string]any{
				"image": "quay.io/opstree/redis:v8.0.3",
			},
		},
	}}
	_, err := kind.dynamic.Resource(redisIntegrationGVR).Namespace("default").Create(ctx, redis, metav1.CreateOptions{})
	assert.NilError(t, err)

	// Create a companion StatefulSet matching the redis-operator naming convention.
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "redis",
						Image: "quay.io/opstree/redis:v8.0.3",
					}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: readyReplicas},
	}
	_, err = kind.client.AppsV1().StatefulSets("default").Create(ctx, sts, metav1.CreateOptions{})
	assert.NilError(t, err)

	t.Cleanup(func() {
		_ = kind.dynamic.Resource(redisIntegrationGVR).Namespace("default").Delete(context.Background(), name, metav1.DeleteOptions{})
		_ = kind.client.AppsV1().StatefulSets("default").Delete(context.Background(), name, metav1.DeleteOptions{})
	})
}

// redisSkipAnnotation retrieves the skip-reconcile annotation from the Redis CR.
func redisSkipAnnotation(ctx context.Context, t *testing.T, kind *kindContainer, name string) string {
	t.Helper()
	u, err := kind.dynamic.Resource(redisIntegrationGVR).Namespace("default").Get(ctx, name, metav1.GetOptions{})
	assert.NilError(t, err)
	return u.GetAnnotations()["redis.opstreelabs.in/skip-reconcile"]
}

// waitForCRD polls until the API server accepts list requests for the given GVR.
func waitForCRD(ctx context.Context, kind *kindContainer, gvr schema.GroupVersionResource) error {
	for {
		_, err := kind.dynamic.Resource(gvr).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{Limit: 1})
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("timed out waiting for CRD %s: %w", gvr.Resource, ctx.Err())
		}
	}
}

func TestKubernetesProvider_Redis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ctx := t.Context()
	kind := sharedKinD
	ensureRedisCRD(ctx, t, kind)

	p, err := kubernetes.New(ctx, kind.client, kind.dynamic, slogt.New(t), config.NewProviderConfig().Kubernetes)
	assert.NilError(t, err)

	name := "redis-" + generateRandomName()
	createRedisAndStatefulSet(ctx, t, kind, name, map[string]string{
		"sablier.enable": "true",
		"sablier.group":  "romm",
	}, 1)

	instanceName := kubernetes.RedisName("default", name, kubernetes.ParseOptions{Delimiter: "_"}).Original

	t.Run("inspect reports ready", func(t *testing.T) {
		info, err := p.InstanceInspect(ctx, instanceName)
		assert.NilError(t, err)
		assert.Equal(t, info.Status, sablier.InstanceStatusReady)
		assert.Equal(t, info.Kubernetes.Kind, kubernetes.KindRedis)
	})

	t.Run("stop sets skip-reconcile and scales statefulset to 0", func(t *testing.T) {
		assert.NilError(t, p.InstanceStop(ctx, instanceName))
		assert.Equal(t, redisSkipAnnotation(ctx, t, kind, name), "true")

		sts, err := kind.client.AppsV1().StatefulSets("default").Get(ctx, name, metav1.GetOptions{})
		assert.NilError(t, err)
		assert.Equal(t, *sts.Spec.Replicas, int32(0))
	})

	t.Run("inspect reports stopped after stop", func(t *testing.T) {
		info, err := p.InstanceInspect(ctx, instanceName)
		assert.NilError(t, err)
		assert.Equal(t, info.Status, sablier.InstanceStatusStopped)
	})

	t.Run("start scales statefulset to 1 and removes skip-reconcile", func(t *testing.T) {
		assert.NilError(t, p.InstanceStart(ctx, instanceName))

		sts, err := kind.client.AppsV1().StatefulSets("default").Get(ctx, name, metav1.GetOptions{})
		assert.NilError(t, err)
		assert.Equal(t, *sts.Spec.Replicas, int32(1))
		assert.Equal(t, redisSkipAnnotation(ctx, t, kind, name), "")
	})

	t.Run("list discovers the redis instance", func(t *testing.T) {
		instances, err := p.RedisList(ctx)
		assert.NilError(t, err)
		found := false
		for _, i := range instances {
			if i.Name == instanceName {
				found = true
				assert.DeepEqual(t, i.Groups, []string{"romm"})
			}
		}
		assert.Assert(t, found, fmt.Sprintf("expected to find %s in redis list", instanceName))
	})
}
