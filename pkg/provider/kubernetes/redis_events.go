package kubernetes

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/sablierapp/sablier/pkg/provider"
	"github.com/sablierapp/sablier/pkg/sablier"
)

// redisCRDInstalled reports whether the OT-CONTAINER-KIT Redis CRD is served by the
// API server, allowing InstanceEvents to skip the dynamic informer on clusters that
// don't run the redis-operator.
func (p *Provider) redisCRDInstalled(ctx context.Context) bool {
	if p.dynamic == nil {
		return false
	}
	_, err := p.dynamic.Resource(redisGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false
		}
		p.l.WarnContext(ctx, "could not verify redis-operator CRD presence, enabling redis watcher anyway", "error", err)
		return true
	}
	return true
}

// redisFromObject extracts the *unstructured.Unstructured from an informer event,
// unwrapping a tombstone when the final state was missed.
func redisFromObject(obj any) (*unstructured.Unstructured, bool) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u, true
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	u, ok := tombstone.Obj.(*unstructured.Unstructured)
	return u, ok
}

func (p *Provider) watchRedis(ctx context.Context, instance chan<- sablier.InstanceEvent, wantStopped, wantStarted, wantCreated, wantRemoved bool) cache.SharedIndexInformer {
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if !wantCreated {
				return
			}
			u, ok := redisFromObject(obj)
			if !ok {
				return
			}
			parsed := RedisName(u.GetNamespace(), u.GetName(), ParseOptions{Delimiter: p.delimiter})
			info, err := p.InstanceInspect(ctx, parsed.Original)
			if err != nil {
				p.l.WarnContext(ctx, "inspect after add event failed, using bare info", "redis", parsed.Original, "error", err)
				instance <- sablier.InstanceEvent{Type: provider.InstanceEventCreated, Info: sablier.InstanceInfo{Name: parsed.Original, Provider: sablier.ProviderKubernetes}}
				return
			}
			instance <- sablier.InstanceEvent{Type: provider.InstanceEventCreated, Info: info}
		},
		UpdateFunc: func(old, new any) {
			newRedis, ok := redisFromObject(new)
			if !ok {
				return
			}
			oldRedis, ok := redisFromObject(old)
			if !ok {
				return
			}
			if newRedis.GetResourceVersion() == oldRedis.GetResourceVersion() {
				return
			}

			oldSkipping := oldRedis.GetAnnotations()[redisSkipReconcileAnnotation] == "true"
			newSkipping := newRedis.GetAnnotations()[redisSkipReconcileAnnotation] == "true"

			if wantStopped && !oldSkipping && newSkipping {
				parsed := RedisName(newRedis.GetNamespace(), newRedis.GetName(), ParseOptions{Delimiter: p.delimiter})
				info, err := p.InstanceInspect(ctx, parsed.Original)
				if err != nil {
					p.l.WarnContext(ctx, "inspect after stop event failed, using bare info", "redis", parsed.Original, "error", err)
					instance <- sablier.InstanceEvent{Type: provider.InstanceEventStopped, Info: sablier.InstanceInfo{Name: parsed.Original, Status: sablier.InstanceStatusStopped, Provider: sablier.ProviderKubernetes}}
					return
				}
				instance <- sablier.InstanceEvent{Type: provider.InstanceEventStopped, Info: info}
			}
			if wantStarted && oldSkipping && !newSkipping {
				parsed := RedisName(newRedis.GetNamespace(), newRedis.GetName(), ParseOptions{Delimiter: p.delimiter})
				info, err := p.InstanceInspect(ctx, parsed.Original)
				if err != nil {
					p.l.WarnContext(ctx, "inspect after start event failed, using bare info", "redis", parsed.Original, "error", err)
					instance <- sablier.InstanceEvent{Type: provider.InstanceEventStarted, Info: sablier.InstanceInfo{Name: parsed.Original, Status: sablier.InstanceStatusStarting, Provider: sablier.ProviderKubernetes}}
					return
				}
				instance <- sablier.InstanceEvent{Type: provider.InstanceEventStarted, Info: info}
			}
		},
		DeleteFunc: func(obj any) {
			if !wantRemoved && !wantStopped {
				return
			}
			u, ok := redisFromObject(obj)
			if !ok {
				return
			}
			parsed := RedisName(u.GetNamespace(), u.GetName(), ParseOptions{Delimiter: p.delimiter})
			image, _, _ := unstructured.NestedString(u.Object, "spec", "kubernetesConfig", "image")
			labels := u.GetLabels()
			info := sablier.InstanceInfo{
				Name:     parsed.Original,
				Status:   sablier.InstanceStatusStopped,
				Provider: sablier.ProviderKubernetes,
				Kubernetes: &sablier.KubernetesWorkloadInfo{
					Namespace: u.GetNamespace(),
					Kind:      KindRedis,
					Image:     image,
					Labels:    labels,
				},
			}
			sablier.PopulateEnabledAndGroup(&info, labels)
			if wantRemoved {
				instance <- sablier.InstanceEvent{Type: provider.InstanceEventRemoved, Info: info}
			}
			if wantStopped {
				instance <- sablier.InstanceEvent{Type: provider.InstanceEventStopped, Info: info}
			}
		},
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(p.dynamic, 2*time.Second, metav1.NamespaceAll, nil)
	informer := factory.ForResource(redisGVR).Informer()

	_, _ = informer.AddEventHandler(handler)
	return informer
}
