package main

import (
	"os"

	"github.com/erician/gpdDB/common/gpdconst"
	"github.com/erician/gpdDB/relog"
)

func main() {
	dbName := "aaa"
	key := "aaa"
	value := "bbb"
	reLog, _ := relog.NewRecoveryLog(dbName)
	reLog.WriteLogRecord(relog.NewLogRecordCheckpoint())
	reLog.WriteLogRecord(relog.NewLogRecordUserOp(gpdconst.PUT, 0, key, value))
	reLog.WriteLogRecord(relog.NewLogRecordUserOp(gpdconst.DELETE, 1, key, value))
	reLog.WriteLogRecord(relog.NewLogRecordAllocate(3))
	reLog.WriteLogRecord(relog.NewLogRecordSetField(4, 40, value))
	reLog.Display()
	reLog.Close()
	os.Remove(dbName + relog.RecoveryLogDefaultSuffix)
}
