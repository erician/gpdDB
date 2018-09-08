package relog

import (
	"os"
)

//RecoveryLog const values
const (
	RecoveryLogDefaultPath                  string = "./"
	RecoveryLogDefaultName                  string = "gpdDB_recovery_log"
	RecoveryLogDefaultSuffix                string = "_gpd_recovery_log"
	RecoveryLogDefaultLogRecordChanCapacity int8   = 20
	RecoveryLogDefaultSyncLogInterval       int16  = 100 //ms
)

//RecoveryLog just as the name
type RecoveryLog struct {
	logFile       *os.File
	logRecordChan chan *LogRecord
	nextLsn       int64
}

//NewRecoveryLog create a new log manager
func NewRecoveryLog(dbName string) (reLog *RecoveryLog, err error) {
	reLog = new(RecoveryLog)
	if err = reLog.init(dbName + RecoveryLogDefaultSuffix); err != nil {
		return
	}
	doesNeedRecovery, err := reLog.doesNeedRecovery()
	if err != nil {
		return
	}
	if doesNeedRecovery == true {
		reLog.recover()
	}
	return
}

//InitRecoveryLog init some fields of RecoveryLog
func (reLog *RecoveryLog) init(recovereLogName string) (err error) {
	reLog.logRecordChan = make(chan *LogRecord, RecoveryLogDefaultLogRecordChanCapacity)
	reLog.nextLsn = 0
	if _, err = os.Stat(recovereLogName); os.IsNotExist(err) == true {
		if reLog.logFile, err = os.Create(recovereLogName); err != nil {
			return
		}
		if err = InitLogHeader(reLog.logFile); err != nil {
			return
		}
		if _, err = reLog.logFile.Seek(int64(LogConstValueHeaderLen), 0); err != nil {
			return
		}
		if err = reLog.writeLogRecordToDisk(NewLogRecordCheckpoint()); err != nil {
			return
		}
		if err = reLog.syncLog(); err != nil {
			return
		}
	} else {
		reLog.logFile, err = os.OpenFile(RecoveryLogDefaultPath+RecoveryLogDefaultName, os.O_RDWR, 0644)
		if err != nil {
			return
		}
	}
	if _, err = reLog.logFile.Seek(0, 2); err != nil {
		return
	}
	return
}
