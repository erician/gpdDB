package relog

import (
	"fmt"
	"time"
)

//WriteLogRecord write a LogRecord
func (reLog *RecoveryLog) WriteLogRecord(lr LogRecord) {
	lr.SetChan(make(chan struct{}))
	reLog.logRecordChan <- lr
	<-lr.GetChan()
}

//WriteLogRoutine this is a loop
func (reLog *RecoveryLog) WriteLogRoutine() {
	var res []chan struct{}
	for {
		lr := <-reLog.logRecordChan
		reLog.writeLogRecordToDisk(lr)
		res = append(res, lr.GetChan())
		syncLogChan := time.After(time.Duration(RecoveryLogDefaultSyncLogInterval) * time.Millisecond)
	loop:
		for {
			select {
			case <-syncLogChan:
				break loop
			case lr = <-reLog.logRecordChan:
				reLog.writeLogRecordToDisk(lr)
				res = append(res, lr.GetChan())
			}
		}
		/*
			if err := reLog.syncLog(); err != nil {
				//shutdown this system
				log.Fatal("write log ", err)
			}
		*/
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

func (reLog *RecoveryLog) writeLogRecordToDisk(lr LogRecord) (err error) {
	bs := lr.ToBytes(reLog.nextLsn)
	reLog.logFile.Write(bs)
	reLog.nextLsn++
	return
}
