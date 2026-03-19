package engine

import (
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

func newRaftLogger(lg *zap.Logger) raft.Logger {
	return &raftLogger{logger: lg, sugar: lg.Sugar()}
}

type raftLogger struct {
	logger *zap.Logger
	sugar  *zap.SugaredLogger
}

func (l *raftLogger) Debug(args ...any) {
	l.sugar.Debug(args...)
}

func (l *raftLogger) Debugf(format string, args ...any) {
	l.sugar.Debugf(format, args...)
}

func (l *raftLogger) Error(args ...any) {
	l.sugar.Error(args...)
}

func (l *raftLogger) Errorf(format string, args ...any) {
	l.sugar.Errorf(format, args...)
}

func (l *raftLogger) Info(args ...any) {
	l.sugar.Info(args...)
}

func (l *raftLogger) Infof(format string, args ...any) {
	l.sugar.Infof(format, args...)
}

func (l *raftLogger) Warning(args ...any) {
	l.sugar.Warn(args...)
}

func (l *raftLogger) Warningf(format string, args ...any) {
	l.sugar.Warnf(format, args...)
}

func (l *raftLogger) Fatal(args ...any) {
	l.sugar.Fatal(args...)
}

func (l *raftLogger) Fatalf(format string, args ...any) {
	l.sugar.Fatalf(format, args...)
}

func (l *raftLogger) Panic(args ...any) {
	l.sugar.Panic(args...)
}

func (l *raftLogger) Panicf(format string, args ...any) {
	l.sugar.Panicf(format, args...)
}
