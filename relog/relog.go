package relog

import (
	"os"

	"github.com/erician/gpdDB/common/gpdconst"

	"github.com/erician/gpdDB/utils/conv"

	"github.com/erician/gpdDB/blkio"
)

//RecoveryLog const values
const (
	RecoveryLogDefaultPath                  string = "./"
	RecoveryLogDefaultName                  string = "gpdDB_recovery_log"
	RecoveryLogDefaultSuffix                string = "_gpd_recovery_log"
	RecoveryLogDefaultLogRecordChanCapacity int8   = 20
	RecoveryLogDefaultSyncLogInterval       int16  = 0 //ms
)

//RecoveryLog just as the name
type RecoveryLog struct {
	logFile       *os.File
	logRecordChan chan LogRecord
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
func (reLog *RecoveryLog) init(recoveryLogName string) (err error) {
	reLog.logRecordChan = make(chan LogRecord, RecoveryLogDefaultLogRecordChanCapacity)
	reLog.nextLsn = 0
	if _, err = os.Stat(recoveryLogName); os.IsNotExist(err) == true {
		if reLog.logFile, err = os.Create(recoveryLogName); err != nil {
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
		reLog.logFile, err = os.OpenFile(recoveryLogName, os.O_RDWR, 0644)
		if err != nil {
			return
		}
	}
	if _, err = reLog.logFile.Seek(0, 2); err != nil {
		return
	}
	go reLog.WriteLogRoutine()
	return
}

//Sync sync recovery log file
func (reLog *RecoveryLog) Sync() error {
	return blkio.SyncFile(reLog.logFile)
}

//Close sync recovery log file and sync
func (reLog *RecoveryLog) Close() (err error) {
	if err = blkio.SyncFile(reLog.logFile); err != nil {
		return
	}
	return reLog.logFile.Close()
}

//Display display the log for reading easily
func (reLog *RecoveryLog) Display() {
	curPos := int64(0)
	lsnAndOpBs := make([]byte, 9)
	for {
		if _, err := reLog.logFile.ReadAt(lsnAndOpBs, curPos); err != nil {
			switch op, _ := conv.Btoi(lsnAndOpBs[8:9]); int8(op) {
			case gpdconst.CHECKPOINT:
				curPos = DisplayLogRecordCheckpoint(reLog.logFile, lsnAndOpBs[0:8], curPos)
			}
		}
	}
}
