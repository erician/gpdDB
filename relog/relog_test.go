package relog

import (
	"os"
	"testing"
)

func TestNewRecoveryLog(t *testing.T) {
	dbName := "aaa"
	reLog, err := NewRecoveryLog(dbName)
	if err != nil {
		t.Error("expected ", nil, "not ", err)
	}
	reLog.Close()
	os.Remove(dbName + RecoveryLogDefaultSuffix)

}
