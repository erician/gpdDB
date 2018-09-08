package dataorg

import (
	"os"
	"testing"
)

func TestNewDbFileWithDbfileNotExist(t *testing.T) {
	dbName := "aaa"
	_, err := NewDbFile(dbName)
	defer os.Remove(dbName)
	if err != nil {
		t.Error("expected ", nil, "not ", err)
	}
	if _, err = os.Stat(dbName); os.IsNotExist(err) == true {
		t.Error("expected " + dbName + " exists")
	}

}

func TestNewDbFileWithDbfileAlreadyExist(t *testing.T) {
	dbName := "aaa"
	_, err := NewDbFile(dbName)
	defer os.Remove("aaa")
	if err != nil {
		t.Error("expected ", nil, "not ", err)
	}
	_, err = NewDbFile("aaa")
	if err != nil {
		t.Error("expected ", nil, "not ", err)
	}

}
