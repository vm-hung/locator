package storage

import (
	"os"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

func NewPebbleDB(dir string, l *zap.Logger) (*pebble.DB, error) {
	if err := os.MkdirAll(dir, 0755); err == nil {
		return pebble.Open(dir, &pebble.Options{
			Logger: NewZapAdapter(l),
		})
	} else {
		return nil, err
	}
}

func upperbound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			return upper
		}
	}
	return nil
}
