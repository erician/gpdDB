package relog

import (
	"os"
	"testing"

	"github.com/erician/gpdDB/common/gpdconst"
)

func TestWriteLogRecord(t *testing.T) {
	dbName := "aaa"
	key := "bbb"
	value := "ccc"
	reLog, _ := NewRecoveryLog(dbName)
	reLog.WriteLogRecord(NewLogRecordCheckpoint())
	reLog.WriteLogRecord(NewLogRecordUserOp(gpdconst.PUT, 0, key, value))
	reLog.WriteLogRecord(NewLogRecordUserOp(gpdconst.DELETE, 1, key, value))
	reLog.WriteLogRecord(NewLogRecordAllocate(3))
	reLog.WriteLogRecord(NewLogRecordSetField(4, 40, value))

	filedPos := 16 + 9 + 9 + 27 + 27 + 17 + 21
	tFiledValue := make([]byte, len(value))
	if _, err := reLog.logFile.ReadAt(tFiledValue, int64(filedPos)); err != nil {
		t.Error("expected: ", nil, "not: ", err)
	}
	if string(tFiledValue) != value {
		t.Error("expected: ", value, "not: ", tFiledValue)
	}
	reLog.Close()

	os.Remove(dbName + RecoveryLogDefaultSuffix)
}
