package gpddb

import (
	"github.com/erician/gpdDB/cache"
	"github.com/erician/gpdDB/dataorg"
	"github.com/erician/gpdDB/errors"
	"github.com/erician/gpdDB/relog"
	"github.com/erician/gpdDB/utils/conv"
	"github.com/erician/gpdDB/utils/readwrite"
)

//Put put a pair of key-value
func (db *GpdDb) Put(key string, value string) (err error) {
	curNode := db.rootNode
	for dataorg.NodeIsLeaf(curNode.Block[:]) == false {
		index := dataorg.INodeFindIndex(curNode.Block[:], key)
		if curNode != db.rootNode {
			db.cache.ReleaseEnt(curNode)
		}
		curNode, _ = db.cache.GetEnt(db.dbFile, index, true)
	}
	if secEnt, splitKey := db.putPairInLeaf(curNode, key, value); secEnt != nil {
		splitIndex, _ := conv.Itob(secEnt.Block)
		parentEnt, err := db.cache.GetEnt(db.dbFile, dataorg.NodeGetParent(secEnt.Block[:]), true)
		if err != nil {
			return errors.NewErrPutFailed(err.Error())
		}
		db.putPairInIndex(parentEnt, splitKey, string(splitIndex))
	}
	return
}

func (db *GpdDb) putPairInLeaf(ent *cache.Ent, key string, value string) (secEnt *cache.Ent, splitKey string) {
	insertPos, doesAlreadyExist := dataorg.DNodeFindInsertPos(ent.Block[:], key)
	if doesAlreadyExist == true {
		db.deletePairInLeaf(ent, key, insertPos)
	}
	if dataorg.NodeIsEnoughSpaceLeft(ent.Block[:], int64(dataorg.DNodeGetPairLen(key, value))) == false {
		secEnt = db.getNewEnt()
		splitPos := db.splitLeaf(ent, secEnt)
		splitKey = string(dataorg.NodeGetKeyOrValue(secEnt.Block[:], splitPos))
		if insertPos < splitPos {
			db.insertPairInLeaf(ent, key, value, insertPos)
		} else {
			db.insertPairInLeaf(secEnt, key, value, insertPos-splitPos+int(dataorg.NodeGetHeaderLen(secEnt.Block[:])))
		}
	} else {
		db.insertPairInLeaf(ent, key, value, insertPos)
	}
	return
}

func (db *GpdDb) putPairInIndex(ent *cache.Ent, key string, index string) {

}

func (db *GpdDb) insertPairInLeaf(ent *cache.Ent, key string, value string, pos int) {
	dataorg.DNodeInsertPair(ent.Block[:], key, value, pos)
	db.reLog.WriteLogRecord(relog.NewLogRecordUserOpPut(ent.BlkID, key, value)) //log
}

func (db *GpdDb) splitLeaf(ent *cache.Ent, secEnt *cache.Ent) int {
	splitPos := dataorg.DNodeFindSplitPos(ent.Block[:])
	readwrite.WriteByte(secEnt.Block[:], int(dataorg.NodeGetHeaderLen(secEnt.Block[:])),
		ent.Block[:], splitPos, int(dataorg.NodeGetLen(ent.Block[:]))-splitPos)
	for curPos := dataorg.NodeGetHeaderLen(secEnt.Block[:]); curPos < dataorg.NodeGetLen(secEnt.Block[:]); {
		key := dataorg.NodeGetKeyOrValue(secEnt.Block[:], int(curPos))
		value := dataorg.NodeGetKeyOrValue(secEnt.Block[:], dataorg.NodeNextField(secEnt.Block[:], int(curPos)))

		db.reLog.WriteLogRecord(relog.NewLogRecordUserOpPut(secEnt.BlkID, string(key), string(value))) //log
		db.reLog.WriteLogRecord(relog.NewLogRecordUserOpDelete(ent.BlkID, string(key), string(value))) //log

		curPos = int16(dataorg.NodeNextKey(secEnt.Block[:], int(curPos)))
	}
	dataorg.NodeSetLen(ent.Block[:], int16(splitPos))
	t, _ := conv.Itob(int16(splitPos))
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(ent.BlkID, int16(dataorg.NodeOffLen), string(t))) //log

	dataorg.NodeSetNext(ent.Block[:], secEnt.BlkID)
	t, _ = conv.Itob(secEnt.BlkID)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(ent.BlkID, int16(dataorg.NodeOffNext), string(t))) //log

	secEntLen := dataorg.NodeGetHeaderLen(secEnt.Block[:]) + dataorg.NodeGetLen(ent.Block[:]) - int16(splitPos)
	dataorg.NodeSetLen(secEnt.Block[:], secEntLen)
	t, _ = conv.Itob(secEntLen)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(secEnt.BlkID, int16(dataorg.NodeOffLen), string(t))) //log

	parent := dataorg.NodeGetParent(secEnt.Block[:])
	dataorg.NodeSetParent(secEnt.Block[:], parent)
	t, _ = conv.Itob(parent)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(secEnt.BlkID, int16(dataorg.NodeOffParent), string(t))) //log

	return splitPos
}
