package cache

import (
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/hunkvm/locator/pkg/types"
)

type NodeHash struct{}

func (h NodeHash) ID(node *core.Node) string {
	namespace := types.DefaultNamespace
	if node.GetMetadata() != nil {
		fields := node.GetMetadata().GetFields()
		if v, ok := fields["locator_namespace"]; ok {
			if s := v.GetStringValue(); s != "" {
				namespace = s
			}
		}
	}
	return namespace
}
