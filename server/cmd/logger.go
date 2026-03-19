package main

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/hunkvm/locator/server/config"
)

func newLogger(cfg *config.LogConfig) (*zap.Logger, error) {
	var encoder zapcore.Encoder
	var level zap.AtomicLevel

	if cfg != nil {
		var err error
		levelStr := strings.ToLower(cfg.Level)
		level, err = zap.ParseAtomicLevel(levelStr)
		if err != nil {
			return nil, fmt.Errorf("parse log level: %w", err)
		}

		if cfg.Format == "json" {
			encoder = jsonEncoderConfig()
		} else {
			encoder = textEncoderConfig()
		}
	} else {
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
		encoder = textEncoderConfig()
	}

	core := zapcore.NewCore(encoder, os.Stdout, level)
	traceOpt := zap.AddStacktrace(zapcore.ErrorLevel)
	return zap.New(core, zap.AddCaller(), traceOpt), nil
}

func textEncoderConfig() zapcore.Encoder {
	encoderCfg := zap.NewDevelopmentEncoderConfig()
	encoderCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	encoderCfg.ConsoleSeparator = " "
	return zapcore.NewConsoleEncoder(encoderCfg)
}

func jsonEncoderConfig() zapcore.Encoder {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.TimeKey = "timestamp"
	return zapcore.NewJSONEncoder(encoderCfg)
}
