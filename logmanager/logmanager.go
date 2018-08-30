package logmanager

import (
	"log"
	"os"
)

//LogManager const values
const (
	RecoveryLogDefaultPath                 string = "./"
	RecoveryLogDefaultName                 string = "gpdDB_recovery_log"
	LogManagerDefaultLogRecordChanCapacity int8   = 20
	LogManagerDefaultSyncLogInterval       int16  = 100 //ms
)

//LogManager to manage the recovery log
type LogManager struct {
	logFile       *os.File
	logRecordChan chan *LogRecord
	nextLsn       int64
}

var logManager = new(LogManager)

func init() {
	InitLogManager()
	//recovery or something

	//start recovery log
	go logManager.write()
}

//InitLogManager init some fields of LogManager
func InitLogManager() {
	logManager.logRecordChan = make(chan *LogRecord, LogManagerDefaultLogRecordChanCapacity)
	_, err := os.Stat(RecoveryLogDefaultPath + RecoveryLogDefaultName)
	if os.IsNotExist(err) == true {
		logManager.logFile, err = os.Create(RecoveryLogDefaultPath + RecoveryLogDefaultName)
		if err != nil {
			log.Fatal("create logfile: ", err)
		}
		logManager.nextLsn = 0
	} else {
		logManager.logFile, err = os.Open(RecoveryLogDefaultPath + RecoveryLogDefaultName)
		if err != nil {
			log.Fatal("open logfile: ", err)
		}
	}
}
