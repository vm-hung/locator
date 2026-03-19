package storage

import "go.uber.org/zap"

type ZapAdapter struct {
	sugar *zap.SugaredLogger
}

func NewZapAdapter(logger *zap.Logger) *ZapAdapter {
	return &ZapAdapter{sugar: logger.Sugar()}
}

func (a *ZapAdapter) Infof(format string, args ...any) {
	a.sugar.Infof(format, args...)
}

func (a *ZapAdapter) Fatalf(format string, args ...any) {
	a.sugar.Fatalf(format, args...)
}
