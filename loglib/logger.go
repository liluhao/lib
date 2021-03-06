/*举例:
	logger, err := loglib.NewProductionLogger()
	if err != nil {
		panic(err)
	}
	logger.Info("test logger", zap.String("hello", "world"))

  logger, err := loglib.NewProductionLogger()
	if err != nil {
		panic(err)
	}
	logger.Log(loglib.INFO, "test logger", "hello", "world")
*/
package loglib

import (
	"errors"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//二次封装Logger
type Logger struct {
	*zap.Logger
}

//传入配置来创造Looger;适用于生产环境
func NewProductionLogger(options ...zap.Option) (*Logger, error) {
	logger, err := zap.NewProduction(options...)
	if err != nil {
		return nil, err
	}
	return &Logger{logger}, nil
}

//传入配置来创造Looger;适用于开发环境
func NewDevelopmentLogger(options ...zap.Option) (*Logger, error) {
	logger, err := zap.NewDevelopment(options...)
	if err != nil {
		return nil, err
	}
	return &Logger{logger}, nil
}

///不提供传入配置来创造Looger;
func NewNopLogger() *Logger {
	return &Logger{zap.NewNop()}
}

type Level string

const (
	DEBUG  Level = "DEBUG"
	INFO   Level = "INFO"
	WARN   Level = "WARN"
	ERROR  Level = "ERROR"
	DPANIC Level = "DPANIC"
	PANIC  Level = "PANIC"
	FATAL  Level = "FATAL"
)

var logLevel = map[Level]zapcore.Level{
	DEBUG:  zapcore.DebugLevel,
	INFO:   zapcore.InfoLevel,
	WARN:   zapcore.WarnLevel,
	ERROR:  zapcore.ErrorLevel,
	DPANIC: zapcore.DPanicLevel,
	PANIC:  zapcore.PanicLevel,
	FATAL:  zapcore.FatalLevel,
}

// in order to fit go-kit logger interface
func (l *Logger) Log(keyvals ...interface{}) error {
	if len(keyvals) == 0 {
		err := errors.New("key value pair should not be zero number")
		l.Error(err.Error(), zap.Error(err))
		return err
	}

	if len(keyvals)%2 != 0 {
		err := errors.New("key value pair should be even number")
		l.Error(err.Error(), zap.Error(err))
		return err
	}

	level, ok := keyvals[0].(Level)
	if !ok {
		err := errors.New("first key should be level type")
		l.Error(err.Error(), zap.Error(err))
		return err
	}

	lv, ok := logLevel[level]
	if !ok {
		err := errors.New("can not find correspond log level")
		l.Error(err.Error(), zap.Error(err))
		return err
	}
	msg := fmt.Sprintf("%v", keyvals[1])

	var fields []zap.Field
	for i := 2; i < len(keyvals); i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			err := errors.New("key should be string type")
			l.Error(err.Error(), zap.Error(err))
			return err
		}
		fields = append(fields, zap.Any(key, keyvals[i+1]))
	}

	err := l.logging(lv, msg, fields)
	if err != nil {
		l.Error(err.Error(), zap.Error(err))
		return err
	}
	return nil
}

func (l *Logger) With(fields ...zap.Field) {
	l.Logger = l.Logger.With(fields...)
}

func (l *Logger) logging(level zapcore.Level, msg string, fields []zap.Field) error {
	if ce := l.Logger.Check(level, msg); ce != nil {
		ce.Write(fields...)
		return nil
	}
	return errors.New("fail to logging")
}
