package zksync

import (
	"io/ioutil"
	"log"
)

var logger *log.Logger

func init() {
	logger = log.New(ioutil.Discard, "[zksync] ", log.LstdFlags)
}

func SetLogger(l *log.Logger) {
	logger = l
}
