package kubernetes

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/sablierapp/sablier/pkg/sablier"
)

// listRedis returns the OT-CONTAINER-KIT Redis instances labelled sablier.enable=true
// across all namespaces. When the dynamic client is unset or the Redis CRD is not
// installed, it returns no instances rather than an error, so the provider keeps
// working on clusters that don't run the redis-operator.
func (p *Provider) listRedis(ctx context.Context) ([]unstructured.Unstructured, error) {
	if p.dynamic == nil {
		return nil, nil
	}

	list, err := p.dynamic.Resource(redisGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		LabelSelector: "sablier.enable=true",
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			p.l.DebugContext(ctx, "redis-operator CRD not installed, skipping redis discovery")
			return nil, nil
		}
		return nil, err
	}

	return list.Items, nil
}

func (p *Provider) RedisList(ctx context.Context) ([]sablier.InstanceConfiguration, error) {
	items, err := p.listRedis(ctx)
	if err != nil {
		return nil, err
	}

	instances := make([]sablier.InstanceConfiguration, 0, len(items))
	for i := range items {
		instances = append(instances, p.redisToInstance(&items[i]))
	}

	return instances, nil
}

func (p *Provider) redisToInstance(u *unstructured.Unstructured) sablier.InstanceConfiguration {
	labels := u.GetLabels()
	enabled := labels["sablier.enable"]
	var groups []string
	if enabled == "true" {
		groups = sablier.ParseGroups(labels["sablier.group"])
	}

	parsed := RedisName(u.GetNamespace(), u.GetName(), ParseOptions{Delimiter: p.delimiter})

	return sablier.InstanceConfiguration{
		Name:    parsed.Original,
		Groups:  groups,
		Enabled: enabled,
	}
}

func (p *Provider) RedisGroups(ctx context.Context) (map[string][]string, error) {
	items, err := p.listRedis(ctx)
	if err != nil {
		return nil, err
	}

	groups := make(map[string][]string)
	for i := range items {
		u := &items[i]
		parsed := RedisName(u.GetNamespace(), u.GetName(), ParseOptions{Delimiter: p.delimiter})
		for _, groupName := range sablier.ParseGroups(u.GetLabels()["sablier.group"]) {
			groups[groupName] = append(groups[groupName], parsed.Original)
		}
	}

	return groups, nil
}
