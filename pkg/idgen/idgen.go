package idgen

import (
	"math"
	"sync/atomic"
	"time"
)

const (
	timestampLength = 5 * 8
	counterLength   = 8
	suffixLength    = timestampLength + counterLength
)

type Generator struct {
	prefix uint64
	suffix uint64
}

func New(nodeID uint64) *Generator {
	prefix := nodeID << suffixLength
	unit := uint64(time.Millisecond / time.Nanosecond)
	unixMilli := uint64(time.Now().UnixNano()) / unit
	suffix := lowbit(unixMilli, timestampLength) << counterLength
	return &Generator{prefix, suffix}
}

func (g *Generator) Next() uint64 {
	suffix := atomic.AddUint64(&g.suffix, 1)
	id := g.prefix | lowbit(suffix, suffixLength)
	return id
}

func lowbit(x uint64, n uint) uint64 {
	return x & (math.MaxUint64 >> (64 - n))
}
