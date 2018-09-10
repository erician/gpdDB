package gpddb

import (
	"github.com/erician/gpdDB/cache"
	"github.com/erician/gpdDB/dataorg"
	"github.com/erician/gpdDB/relog"
)

//Delete delete a key in DB
//if the key doesn't exist, return not nil; or return nil
func (db *GpdDb) Delete(key string) (err error) {
	return
}

func (db *GpdDb) deletePairInLeaf(ent *cache.Ent, key string, pos int) {
	value := dataorg.DNodeDeletePair(ent.Block[:], key, pos)
	db.reLog.WriteLogRecord(relog.NewLogRecordUserOpDelete(ent.BlkID, key, value)) //log
}
