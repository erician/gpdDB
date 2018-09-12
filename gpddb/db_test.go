package gpddb

import (
	"os"
	"testing"

	"github.com/erician/gpdDB/relog"
)

func TestNewDbWithSimple(t *testing.T) {
	dbName := "aaa"
	db, err := NewDb(dbName)
	if err != nil {
		t.Error("expected: ", nil, "not: ", err)
	}

	db.Close()
	os.Remove(dbName)
	os.Remove(dbName + relog.RecoveryLogDefaultSuffix)
}

func TestNewDbWithError(t *testing.T) {
	dbName := "aaa"
	db, err := NewDb(dbName)
	_, err = NewDb(dbName)
	if err == nil {
		t.Error("expected: ", err, "not: ", nil)
	}
	db.Close()
	os.Remove(dbName)
	os.Remove(dbName + relog.RecoveryLogDefaultSuffix)
}

func TestOpenDbWithSimple(t *testing.T) {
	dbName := "aaa"
	db, _ := NewDb(dbName)
	db.Close()

	db, err := OpenDb(dbName)
	if err != nil {
		t.Error("expected: ", nil, "not: ", err)
	}
	db.Close()

	os.Remove(dbName)
	os.Remove(dbName + relog.RecoveryLogDefaultSuffix)
}

func TestOpenDbWithError(t *testing.T) {
	dbName := "aaa"
	db, _ := NewDb(dbName)
	db.Close()
	os.Remove(dbName)
	os.Remove(dbName + relog.RecoveryLogDefaultSuffix)

	db, err := OpenDb(dbName)
	if err == nil {
		t.Error("expected: ", nil, "not: ", err)
	}
}

func TestRemoveDbWithSimple(t *testing.T) {
	dbName := "aaa"
	db, _ := NewDb(dbName)
	db.Close()

	err := RemoveDb(dbName)
	if err != nil {
		t.Error("expected: ", nil, "not: ", err)
	}
	db.Close()
}

func TestRemoveDbWithError(t *testing.T) {
	dbName := "aaa"
	db, _ := NewDb(dbName)
	db.Close()
	os.Remove(dbName)
	os.Remove(dbName + relog.RecoveryLogDefaultSuffix)

	err := RemoveDb(dbName)
	if err == nil {
		t.Error("expected: ", nil, "not: ", err)
	}
}
