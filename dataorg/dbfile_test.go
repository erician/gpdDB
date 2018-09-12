package dataorg

import (
	"os"
	"testing"

	"github.com/erician/gpdDB/utils/byteutil"

	"github.com/erician/gpdDB/blkio"
	"github.com/erician/gpdDB/common/gpdconst"
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

func TestInitDbFile(t *testing.T) {
	dbFileName := "dbfile"
	dbFile, _ := os.Create(dbFileName)
	initDbFile(dbFile)

	e := make([]byte, gpdconst.BlockSize)
	SuperNodeInit(e)

	tc := make([]byte, gpdconst.BlockSize)
	blkio.ReadBlk(dbFile, tc, SuperNodeConstValueBlkID)
	if byteutil.ByteCmp(e, tc) != 0 {
		t.Error("expect: ", e, "not: ", t)
	}
	dbFile.Close()
	os.Remove(dbFileName)

}
