package log

import (
	"os"
	"time"

	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

type Log struct {
	*logr.Logger
}

var Logger *Log

func init() {
	opts := zap.Options{
		Development: true,
		TimeEncoder: zapcore.TimeEncoderOfLayout(time.RFC3339),
	}
	zaplogger := zap.New(zap.UseFlagOptions(&opts))
	Logger = &Log{&zaplogger}
}

func (l *Log) Fatal(msg string, keyvalues ...interface{}) {
	l.Error(nil, msg, keyvalues...)
	os.Exit(1)
}
