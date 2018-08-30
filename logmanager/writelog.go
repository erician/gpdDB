package logmanager

import (
	"log"
	"time"
)

//WriteLog write a LogRecord
func WriteLog(lr *LogRecord) {
	lr.re = make(chan struct{})
	logManager.logRecordChan <- lr
	<-lr.re
}

func (lm *LogManager) write() {
	var res []chan struct{}
	for {
		lr := <-logManager.logRecordChan
		lm.writeLogRecord(lr)
		res = append(res, lr.re)
		syncLogChan := time.After(time.Duration(LogManagerDefaultSyncLogInterval) * time.Millisecond)
		for {
			select {
			case <-syncLogChan:
				break
			case lr = <-logManager.logRecordChan:
				lm.writeLogRecord(lr)
				res = append(res, lr.re)
			}
		}
		for _, re := range res {
			re <- struct{}{}
		}
		res = nil
	}
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
