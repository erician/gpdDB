package relog

import (
	"os"
	"testing"

	"github.com/erician/gpdDB/utils/byteutil"

	"github.com/erician/gpdDB/utils/conv"
)

const (
	TestLogName string = "test_log"
)

func TestLogSetField(t *testing.T) {
	logFile, err := os.Create(TestLogName)
	defer os.Remove(TestLogName)
	if err != nil {
		t.Error("open " + TestLogName + " failed")
	}
	var off int64 = 100
	var data int64 = 1
	if err = LogSetField(logFile, data, off); err != nil {
		t.Error("expected: ", nil, "not: ", err)
	}
	bs, _ := conv.Itob(data)
	buff := make([]byte, 8)
	logFile.ReadAt(buff, off)
	if c := byteutil.ByteCmp(buff, bs); c != 0 {
		t.Error("expected: ", bs, "not: ", buff)
	}

}

func TestLogGetField(t *testing.T) {
	logFile, err := os.Create(TestLogName)
	defer os.Remove(TestLogName)
	if err != nil {
		t.Error("open " + TestLogName + " failed")
	}
	var off int64 = 100
	var len int64 = 8
	var data int64 = 1

	if err = LogSetField(logFile, data, off); err != nil {
		t.Error("expected: ", nil, "not: ", err)
	}
	gotData, err := LogGetField(logFile, off, len)
	if err != nil {
		t.Error("expected: ", nil, "not: ", err)
	}
	if gotData != data {
		t.Error("expected: ", data, "not: ", gotData)
	}
}

func TestDisplayLogHeader(t *testing.T) {
	dbName := "aaa"
	reLog, _ := NewRecoveryLog(dbName)
	DisplayLogHeader(reLog.logFile)
	reLog.Close()
	os.Remove(dbName + RecoveryLogDefaultSuffix)
}
