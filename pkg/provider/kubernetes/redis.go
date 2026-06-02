package kubernetes

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// KindRedis is the workload kind used in instance names to identify an
// OT-CONTAINER-KIT Redis custom resource (e.g. "redis_namespace_name_1").
const KindRedis = "redis"

// redisGVR is the GroupVersionResource of OT-CONTAINER-KIT Redis resources.
// See https://ot-container-kit.github.io/redis-operator/
var redisGVR = schema.GroupVersionResource{
	Group:    "redis.redis.opstreelabs.in",
	Version:  "v1beta2",
	Resource: "redis",
}

// redisSkipReconcileAnnotation is the annotation the redis-operator watches to
// pause its reconciliation loop, allowing external tooling (like Sablier) to
// safely scale the managed StatefulSet to zero without the operator reversing it.
// See https://ot-container-kit.github.io/redis-operator/docs/configuration/redis
const redisSkipReconcileAnnotation = "redis.opstreelabs.in/skip-reconcile"

// RedisName builds the ParsedName identifier for an OT-CONTAINER-KIT Redis,
// mirroring DeploymentName, StatefulSetName, and ClusterName.
func RedisName(namespace, name string, opts ParseOptions) ParsedName {
	original := fmt.Sprintf("%s%s%s%s%s%s%d", KindRedis, opts.Delimiter, namespace, opts.Delimiter, name, opts.Delimiter, 1)

	return ParsedName{
		Original:  original,
		Kind:      KindRedis,
		Namespace: namespace,
		Name:      name,
		Replicas:  1,
	}
}
