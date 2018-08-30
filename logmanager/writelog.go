package logmanager

import (
	"fmt"
	"log"
	"time"
)

//WriteLogRecord write a LogRecord
func WriteLogRecord(lr *LogRecord) {
	lr.re = make(chan struct{})
	logManager.logRecordChan <- lr
	<-lr.re
}

func (lm *LogManager) writeLog() {
	var res []chan struct{}
	for {
		lr := <-logManager.logRecordChan
		lm.writeLogRecord(lr)
		res = append(res, lr.re)
		syncLogChan := time.After(time.Duration(LogManagerDefaultSyncLogInterval) * time.Millisecond)
	loop:
		for {
			select {
			case <-syncLogChan:
				break loop
			case lr = <-logManager.logRecordChan:
				lm.writeLogRecord(lr)
				res = append(res, lr.re)
			}
		}
		if err := lm.syncLog(); err != nil {
			//shutdown this system
			log.Fatal("write log ", err)
		}
		for _, re := range res {
			re <- struct{}{}
		}
		res = nil
	}
}

func (lm *LogManager) syncLog() (err error) {
	if err = lm.logFile.Sync(); err != nil {
		return fmt.Errorf("sync log file, %v", err)
	}
	return
}

func (lm *LogManager) writeLogRecord(lr *LogRecord) {
	bs, err := lr.ToBytes(lm.nextLsn)
	if err != nil {
		//lack of dealing with other go routines, it is not enough to just let this shutdown
		log.Fatal("write logrecord: ", err)
	}
	lm.logFile.Write(bs)
	lm.nextLsn++
}
