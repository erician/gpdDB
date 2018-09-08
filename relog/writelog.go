package relog

import (
	"fmt"
	"log"
	"time"
)

//WriteLogRecord write a LogRecord
func (reLog *RecoveryLog) WriteLogRecord(lr *LogRecord) {
	lr.re = make(chan struct{})
	reLog.logRecordChan <- lr
	<-lr.re
}

//WriteLog this is a loop
func (reLog *RecoveryLog) WriteLog() {
	var res []chan struct{}
	for {
		lr := <-reLog.logRecordChan
		reLog.writeLogRecordToDisk(lr)
		res = append(res, lr.re)
		syncLogChan := time.After(time.Duration(RecoveryLogDefaultSyncLogInterval) * time.Millisecond)
	loop:
		for {
			select {
			case <-syncLogChan:
				break loop
			case lr = <-reLog.logRecordChan:
				reLog.writeLogRecordToDisk(lr)
				res = append(res, lr.re)
			}
		}
		if err := reLog.syncLog(); err != nil {
			//shutdown this system
			log.Fatal("write log ", err)
		}
		for _, re := range res {
			re <- struct{}{}
		}
		res = nil
	}
}

func (reLog *RecoveryLog) syncLog() (err error) {
	if err = reLog.logFile.Sync(); err != nil {
		return fmt.Errorf("sync recovery log file, %v", err)
	}
	return
}

func (reLog *RecoveryLog) writeLogRecordToDisk(lr *LogRecord) (err error) {
	bs, err := lr.ToBytes(reLog.nextLsn)
	if err != nil {
		return fmt.Errorf("write logrecord, %v", err)
	}
	reLog.logFile.Write(bs)
	reLog.nextLsn++
	return
}
