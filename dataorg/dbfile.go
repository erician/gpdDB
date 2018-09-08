package dataorg

import (
	"os"

	"github.com/erician/gpdDB/blkio"
	"github.com/erician/gpdDB/common/gpdconst"
)

//NewDbFile create a db file with the name:dbName
func NewDbFile(dbName string) (dbFile *os.File, err error) {
	if _, err = os.Stat(dbName); os.IsNotExist(err) == true {
		if dbFile, err = os.Create(dbName); err == nil {
			initDbFile(dbFile)
		} else {
			return
		}
	} else {
		dbFile, err = os.OpenFile(dbName, os.O_RDWR, 0644)
	}
	return
}

func initDbFile(dbFile *os.File) (err error) {
	superNode := make([]byte, gpdconst.BlockSize)
	SuperNodeInit(superNode)
	if err = blkio.WriteBlk(dbFile, superNode, SuperNodeConstValueBlkID); err != nil {
		return
	}
	if err = blkio.SyncFile(dbFile); err != nil {
		return
	}
	return
}
