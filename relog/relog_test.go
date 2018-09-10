package relog

import (
	"os"
	"testing"
)

func TestNewRecoveryLog(t *testing.T) {
	dbName := "aaa"
	_, err := NewRecoveryLog(dbName)
	defer os.Remove(dbName + RecoveryLogDefaultSuffix)
	if err != nil {
		t.Error("expected ", nil, "not ", err)
	}
}
