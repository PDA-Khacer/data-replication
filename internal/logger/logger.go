package logger

import (
	"errors"
)

// A global variable so that log functions can be directly accessed
var log, _ = newZapLogger(Configuration{
	EnableConsole: true,
	//ConsoleLevel:      "error", // enable when remove atomic level
	ConsoleJSONFormat: true,
	EnableFile:        false,
})

// Fields Type to pass when we want to call WithFields for structured logging
type Fields map[string]interface{}

const (
	//DebugLevel has verbose message
	DebugLevel = "debug"
	//InfoLevel is default log level
	InfoLevel = "info"
	//WarnLevel is for logging messages about possible issues
	WarnLevel = "warn"
	//ErrorLevel is for logging errors
	ErrorLevel = "error"
	//FatalLevel is for logging fatal messages. The sytem shutsdown after logging the message.
	FatalLevel = "fatal"
	//PanicLevel is for logging panic messages. The sytem shutsdown after logging the message.
	PanicLevel = "panic"
)

const (
	InstanceZapLogger int = iota
	InstanceLogrusLogger
)

var (
	errInvalidLoggerInstance = errors.New("Invalid logger instance")
)

/*func NewChiLogger() middleware.LoggerInterface {
	return log
}*/

// Logger is our contract for the logger
type Logger interface {
	Debug(args ...interface{})

	Debugf(format string, args ...interface{})

	Info(args ...interface{})

	Infof(format string, args ...interface{})

	Warn(args ...interface{})

	Warnf(format string, args ...interface{})

	Error(args ...interface{})

	Errorf(format string, args ...interface{})

	Fatal(args ...interface{})

	Fatalf(format string, args ...interface{})

	Panic(args ...interface{})

	Panicf(format string, args ...interface{})

	WithFields(keyValues Fields) Logger

	Print(args ...interface{})
}

// Configuration stores the config for the logger
// For some loggers there can only be one level across writers, for such the level of Console is picked by default
type Configuration struct {
	EnableConsole     bool
	ConsoleJSONFormat bool
	ConsoleLevel      string
	EnableFile        bool
	FileJSONFormat    bool
	FileLevel         string
	FileLocation      string
}

// NewLogger returns an instance of logger
func NewLogger(config Configuration, loggerInstance int) (Logger, error) {
	switch loggerInstance {
	case InstanceZapLogger:
		logger, err := newZapLogger(config)
		if err != nil {
			return nil, err
		}
		log = logger
		return logger, nil

	/*case InstanceLogrusLogger:
	logger, err := newLogrusLogger(config)
	if err != nil {
		return err
	}
	log = logger
	return nil*/

	default:
		return nil, errInvalidLoggerInstance
	}
}

func Debug(args ...interface{}) {
	log.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

func Info(args ...interface{}) {
	log.Info(args...)
}

func Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

func Warn(args ...interface{}) {
	log.Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	log.Warnf(format, args...)
}

func Error(args ...interface{}) {
	log.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	log.Errorf(format, args...)
}

func Fatal(args ...interface{}) {
	log.Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}

func Panic(args ...interface{}) {
	log.Panic(args...)
}

func Panicf(format string, args ...interface{}) {
	log.Panicf(format, args...)
}

func WithFields(keyValues Fields) Logger {
	return log.WithFields(keyValues)
}
