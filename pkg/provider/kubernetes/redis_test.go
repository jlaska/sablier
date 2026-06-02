package kubernetes

import (
	"context"
	"testing"

	"github.com/neilotoole/slogt"
	"github.com/sablierapp/sablier/pkg/sablier"
	"go.opentelemetry.io/otel"
	"gotest.tools/v3/assert"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// newRedisObj builds an unstructured Redis CR for tests.
func newRedisObj(namespace, name string, labels, annotations map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("redis.redis.opstreelabs.in/v1beta2")
	u.SetKind("Redis")
	u.SetNamespace(namespace)
	u.SetName(name)
	if labels != nil {
		u.SetLabels(labels)
	}
	if annotations != nil {
		u.SetAnnotations(annotations)
	}
	_ = unstructured.SetNestedField(u.Object, "quay.io/opstree/redis:v8.0.3", "spec", "kubernetesConfig", "image")
	return u
}

// newRedisStatefulSet builds a fake StatefulSet for the Redis CR (matching name convention).
func newRedisStatefulSet(namespace, name string, readyReplicas int32) *appsv1.StatefulSet {
	one := int32(1)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &one,
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: readyReplicas,
		},
	}
}

// newFakeRedisProvider creates a Provider with fake dynamic (for Redis CRs) and typed
// (for StatefulSets) clients. It works around two limitations of the fake clients:
//
//  1. The dynamic tracker's Add() uses UnsafeGuessKindToResource, which maps Kind="Redis"
//     to resource "redises" (double-s) rather than the actual CRD resource "redis".
//     We bypass this by using tracker.Create() with the explicit GVR.
//  2. The typed client's GetScale() panics unless a Scale object is in the tracker.
//     We inject a PrependReactor to return a synthetic Scale for each StatefulSet.
func newFakeRedisProvider(t *testing.T, redisObjs []*unstructured.Unstructured, stsSets []*appsv1.StatefulSet) *Provider {
	t.Helper()

	// --- Dynamic client for Redis CRs ---
	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{redisGVR: "RedisList"},
		// Do NOT pass objects here; Add() guesses "redises" from Kind="Redis".
		// We use tracker.Create() below to store under the explicit GVR "redis".
	)
	for _, obj := range redisObjs {
		if err := dyn.Tracker().Create(redisGVR, obj, obj.GetNamespace()); err != nil {
			t.Fatalf("failed to add redis obj to tracker: %v", err)
		}
	}

	// --- Typed client for StatefulSets ---
	// Build a map of name → replicas so the Scale reactor can respond correctly.
	stsReplicas := make(map[string]int32, len(stsSets))
	stsObjs := make([]runtime.Object, len(stsSets))
	for i, sts := range stsSets {
		stsObjs[i] = sts
		stsReplicas[sts.Name] = *sts.Spec.Replicas
	}
	client := k8sfake.NewSimpleClientset(stsObjs...)

	// The fake typed client's GetScale/UpdateScale operate on an autoscalingv1.Scale
	// subresource. Without a reactor, the default ObjectReaction returns the StatefulSet
	// itself and the type assertion to *Scale panics. We intercept scale actions here.
	client.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "scale" {
			return false, nil, nil
		}
		name := action.(k8stesting.GetAction).GetName()
		replicas := stsReplicas[name]
		return true, &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: action.GetNamespace()},
			Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
		}, nil
	})
	client.PrependReactor("update", "statefulsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "scale" {
			return false, nil, nil
		}
		scale := action.(k8stesting.UpdateAction).GetObject().(*autoscalingv1.Scale)
		stsReplicas[scale.Name] = scale.Spec.Replicas
		return true, scale, nil
	})

	return &Provider{
		Client:    client,
		dynamic:   dyn,
		delimiter: "_",
		l:         slogt.New(t),
		tracer:    otel.Tracer("test"),
	}
}

func TestProvider_RedisInspect(t *testing.T) {
	t.Parallel()

	enabled := map[string]string{"sablier.enable": "true", "sablier.group": "romm"}

	tests := []struct {
		name          string
		labels        map[string]string
		annotations   map[string]string
		readyReplicas int32
		wantStatus    sablier.InstanceStatus
	}{
		{
			name:          "ready when statefulset has ready replicas",
			labels:        enabled,
			readyReplicas: 1,
			wantStatus:    sablier.InstanceStatusReady,
		},
		{
			name:          "starting when statefulset has no ready replicas but no skip-reconcile",
			labels:        enabled,
			readyReplicas: 0,
			wantStatus:    sablier.InstanceStatusStarting,
		},
		{
			name:          "stopped when skip-reconcile is set and no ready replicas",
			labels:        enabled,
			annotations:   map[string]string{redisSkipReconcileAnnotation: "true"},
			readyReplicas: 0,
			wantStatus:    sablier.InstanceStatusStopped,
		},
		{
			name:          "ready when skip-reconcile is set but replicas are still draining",
			labels:        enabled,
			annotations:   map[string]string{redisSkipReconcileAnnotation: "true"},
			readyReplicas: 1,
			wantStatus:    sablier.InstanceStatusReady,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			redisObj := newRedisObj("default", "my-redis", tc.labels, tc.annotations)
			sts := newRedisStatefulSet("default", "my-redis", tc.readyReplicas)
			p := newFakeRedisProvider(t, []*unstructured.Unstructured{redisObj}, []*appsv1.StatefulSet{sts})

			parsed := RedisName("default", "my-redis", ParseOptions{Delimiter: "_"})
			info, err := p.RedisInspect(context.Background(), parsed)
			assert.NilError(t, err)
			assert.Equal(t, info.Status, tc.wantStatus)
			assert.Equal(t, info.Provider, sablier.ProviderKubernetes)
			assert.Assert(t, info.Kubernetes != nil)
			assert.Equal(t, info.Kubernetes.Kind, KindRedis)
			assert.Equal(t, info.Kubernetes.Image, "quay.io/opstree/redis:v8.0.3")
		})
	}
}

func TestProvider_RedisScale(t *testing.T) {
	t.Parallel()

	getAnnotation := func(t *testing.T, p *Provider, namespace, name string) string {
		t.Helper()
		u, err := p.dynamic.Resource(redisGVR).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
		assert.NilError(t, err)
		return u.GetAnnotations()[redisSkipReconcileAnnotation]
	}

	getStsReplicas := func(t *testing.T, p *Provider, namespace, name string) int32 {
		t.Helper()
		s, err := p.Client.AppsV1().StatefulSets(namespace).GetScale(context.Background(), name, metav1.GetOptions{})
		assert.NilError(t, err)
		return s.Spec.Replicas
	}

	t.Run("stop sets skip-reconcile and scales to 0", func(t *testing.T) {
		t.Parallel()
		redisObj := newRedisObj("default", "my-redis", nil, nil)
		sts := newRedisStatefulSet("default", "my-redis", 1)
		p := newFakeRedisProvider(t, []*unstructured.Unstructured{redisObj}, []*appsv1.StatefulSet{sts})

		parsed := RedisName("default", "my-redis", ParseOptions{Delimiter: "_"})
		assert.NilError(t, p.InstanceStop(context.Background(), parsed.Original))
		assert.Equal(t, getAnnotation(t, p, "default", "my-redis"), "true")
		assert.Equal(t, getStsReplicas(t, p, "default", "my-redis"), int32(0))
	})

	t.Run("start scales to 1 and removes skip-reconcile annotation", func(t *testing.T) {
		t.Parallel()
		redisObj := newRedisObj("default", "my-redis", nil, map[string]string{redisSkipReconcileAnnotation: "true"})
		sts := newRedisStatefulSet("default", "my-redis", 0)
		p := newFakeRedisProvider(t, []*unstructured.Unstructured{redisObj}, []*appsv1.StatefulSet{sts})

		parsed := RedisName("default", "my-redis", ParseOptions{Delimiter: "_"})
		assert.NilError(t, p.InstanceStart(context.Background(), parsed.Original))
		assert.Equal(t, getStsReplicas(t, p, "default", "my-redis"), int32(1))
		// Null-patch removes the annotation key entirely.
		assert.Equal(t, getAnnotation(t, p, "default", "my-redis"), "")
	})
}

func TestProvider_RedisListAndGroups(t *testing.T) {
	t.Parallel()

	enabledRomm := newRedisObj("default", "romm-redis",
		map[string]string{"sablier.enable": "true", "sablier.group": "romm"}, nil)
	enabledDefaultGroup := newRedisObj("default", "solo-redis",
		map[string]string{"sablier.enable": "true"}, nil)
	disabled := newRedisObj("default", "ignored-redis", nil, nil)

	stsRomm := newRedisStatefulSet("default", "romm-redis", 1)
	stsSolo := newRedisStatefulSet("default", "solo-redis", 1)
	stsIgnored := newRedisStatefulSet("default", "ignored-redis", 1)

	p := newFakeRedisProvider(t,
		[]*unstructured.Unstructured{enabledRomm, enabledDefaultGroup, disabled},
		[]*appsv1.StatefulSet{stsRomm, stsSolo, stsIgnored},
	)

	instances, err := p.RedisList(context.Background())
	assert.NilError(t, err)
	// Only the two sablier.enable=true instances are listed.
	assert.Equal(t, len(instances), 2)

	groups, err := p.RedisGroups(context.Background())
	assert.NilError(t, err)

	romm := RedisName("default", "romm-redis", ParseOptions{Delimiter: "_"}).Original
	solo := RedisName("default", "solo-redis", ParseOptions{Delimiter: "_"}).Original
	assert.DeepEqual(t, groups["romm"], []string{romm})
	assert.DeepEqual(t, groups["default"], []string{solo})
}

// TestProvider_RedisSetSkipReconcileRemoval verifies that the null merge patch
// removes the annotation key rather than setting it to an empty string.
func TestProvider_RedisSetSkipReconcileRemoval(t *testing.T) {
	t.Parallel()

	redisObj := newRedisObj("default", "my-redis", nil, map[string]string{
		redisSkipReconcileAnnotation: "true",
		"other-annotation":           "keep-me",
	})
	p := newFakeRedisProvider(t, []*unstructured.Unstructured{redisObj}, nil)

	parsed := RedisName("default", "my-redis", ParseOptions{Delimiter: "_"})
	assert.NilError(t, p.redisSetSkipReconcile(context.Background(), parsed, false))

	u, err := p.dynamic.Resource(redisGVR).Namespace("default").Get(context.Background(), "my-redis", metav1.GetOptions{})
	assert.NilError(t, err)

	// skip-reconcile annotation must be gone.
	_, exists := u.GetAnnotations()[redisSkipReconcileAnnotation]
	assert.Assert(t, !exists, "skip-reconcile annotation should have been removed")

	// Unrelated annotations must be preserved.
	assert.Equal(t, u.GetAnnotations()["other-annotation"], "keep-me")
}
