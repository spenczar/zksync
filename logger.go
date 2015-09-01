package zksync

import (
	"io/ioutil"
	"log"
)

type Logger interface {
	Printf(string, ...interface{})
	Fatalf(string, ...interface{})
}

var (
	logger    Logger
	verbosity severity
)

type severity int

const (
	Fatal severity = iota
	Error
	Warning
	Info
	Debug
	Trace

	defaultVerbosity = Warning
)

func init() {
	logger = log.New(ioutil.Discard, "[zksync] ", log.LstdFlags)
	verbosity = defaultVerbosity
}

func SetLogger(l Logger) {
	logger = l
}

func SetVerbosity(v severity) {
	verbosity = v
}

func logFatal(fmt string, args ...interface{}) {
	if verbosity >= Error {
		logger.Fatalf("[FATAL] "+fmt, args...)
	}
}

func logError(fmt string, args ...interface{}) {
	if verbosity >= Error {
		logger.Printf("[ERROR] "+fmt, args...)
	}
}

func logWarning(fmt string, args ...interface{}) {
	if verbosity >= Warning {
		logger.Printf("[WARNING] "+fmt, args...)
	}
}

func logInfo(fmt string, args ...interface{}) {
	if verbosity >= Info {
		logger.Printf("[INFO] "+fmt, args...)
	}
}

func logDebug(fmt string, args ...interface{}) {
	if verbosity >= Debug {
		logger.Printf("[DEBUG] "+fmt, args...)
	}
}

func logTrace(fmt string, args ...interface{}) {
	if verbosity >= Trace {
		logger.Printf("[TRACE] "+fmt, args...)
	}
}
