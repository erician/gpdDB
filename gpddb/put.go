package gpddb

import (
	"github.com/erician/gpdDB/cache"
	"github.com/erician/gpdDB/common/gpdconst"
	"github.com/erician/gpdDB/dataorg"
	"github.com/erician/gpdDB/errors"
	"github.com/erician/gpdDB/relog"
	"github.com/erician/gpdDB/utils/conv"
	"github.com/erician/gpdDB/utils/readwrite"
)

//Put put a pair of key-value
func (db *GpdDb) Put(key string, value string) (err error) {
	curNode, _ := db.getRootNode()
	for dataorg.NodeIsLeaf(curNode.Block[:]) == false {
		index := dataorg.INodeFindIndex(curNode.Block[:], key)
		db.cache.ReleaseEnt(curNode)
		if curNode, err = db.cache.GetEnt(db.dbFile, index, true); err != nil {
			return errors.NewErrPutFailed(err.Error())
		}
	}
	if secEntBlkNum, splitKey := db.putPairInLeaf(curNode, key, value); secEntBlkNum != 0 {
		rightIndex, _ := conv.Itob(secEntBlkNum)
		leftIndex, _ := conv.Itob(curNode.BlkID)
		db.putPairInInternalRecursive(dataorg.NodeGetParent(curNode.Block[:]), splitKey, string(leftIndex), string(rightIndex), dataorg.NodeConstValueLeafLevel)
	}
	curNode.SetStat(curNode.GetStat() | cache.EntStatDelaywrite)
	db.cache.ReleaseEnt(curNode)
	return
}

func (db *GpdDb) putPairInLeaf(ent *cache.Ent, key string, value string) (secEntBlkNum int64, splitKey string) {
	insertPos, doesAlreadyExist := dataorg.DNodeFindInsertPos(ent.Block[:], key)
	if doesAlreadyExist == true {
		db.deletePairInLeaf(ent, key, insertPos)
	}
	if dataorg.NodeIsEnoughSpaceLeft(ent.Block[:], int64(dataorg.DNodeGetPairLen(key, value))) == false {
		secEnt := db.getNewEnt()
		secEntBlkNum = secEnt.BlkID
		splitPos := db.splitLeaf(ent, secEnt)
		splitKey = string(dataorg.NodeGetKeyOrValue(secEnt.Block[:], int(dataorg.NodeGetHeaderLen(secEnt.Block[:]))))
		if insertPos < splitPos {
			db.insertPairInLeaf(ent, key, value, insertPos)
		} else {
			db.insertPairInLeaf(secEnt, key, value, insertPos-splitPos+int(dataorg.NodeGetHeaderLen(secEnt.Block[:])))
		}
		secEnt.SetStat(secEnt.GetStat() | cache.EntStatDelaywrite)
		db.cache.ReleaseEnt(secEnt)
	} else {
		db.insertPairInLeaf(ent, key, value, insertPos)
	}
	return
}

func (db *GpdDb) putPairInInternalRecursive(curNodeBlkID int64, key string, leftIndex string, rightIndex string, level int64) {
	if curNodeBlkID == gpdconst.NotAllocatedBlockID {
		rootEnt := db.getNewEnt()
		db.insertPairInInternalRootFirstTime(rootEnt, key, leftIndex, rightIndex, level)
		rootEnt.SetStat(rootEnt.GetStat() | cache.EntStatDelaywrite)
		dataorg.SuperNodeSetRootNodeID(db.superNode.Block[:], rootEnt.BlkID)
		db.cache.ReleaseEnt(rootEnt)
		t, _ := conv.Itob(rootEnt.BlkID)
		db.reLog.WriteLogRecord(relog.NewLogRecordSetField(db.superNode.BlkID, int16(dataorg.SuperNodeOffRootNodeID), string(t)))
	} else {
		ent, _ := db.cache.GetEnt(db.dbFile, curNodeBlkID, true)
		insertPos := dataorg.INodeFindInsertPos(ent.Block[:], key)
		if dataorg.NodeIsEnoughSpaceLeft(ent.Block[:], int64(dataorg.INodeGetPairLen(key, rightIndex))) == false {
			secEnt := db.getNewEnt()
			splitPos, splitKey := db.splitIndex(ent, secEnt)
			if insertPos < splitPos {
				db.insertPairInInternal(ent, key, rightIndex, insertPos)
			} else {
				db.insertPairInInternal(secEnt, key, rightIndex,
					insertPos-splitPos-int(dataorg.NodeKeyLenSize)-len(splitKey)+int(dataorg.NodeGetHeaderLen(secEnt.Block[:])))
			}
			rightIndex, _ := conv.Itob(secEnt.BlkID)
			leftIndex, _ := conv.Itob(ent.BlkID)

			secEnt.SetStat(secEnt.GetStat() | cache.EntStatDelaywrite)
			db.cache.ReleaseEnt(secEnt)
			db.putPairInInternalRecursive(dataorg.NodeGetParent(ent.Block[:]), splitKey, string(leftIndex), string(rightIndex), level+1)

		} else {
			db.insertPairInInternal(ent, key, rightIndex, insertPos)
		}
		ent.SetStat(ent.GetStat() | cache.EntStatDelaywrite)
		db.cache.ReleaseEnt(ent)
	}
}

func (db *GpdDb) insertPairInInternalRootFirstTime(rootEnt *cache.Ent, key string, leftIndex string, rightIndex string, level int64) {
	curPos := int(dataorg.NodeGetHeaderLen(rootEnt.Block[:]))
	dataorg.NodeSetKeyOrValue(rootEnt.Block[:], int(curPos), []byte(leftIndex), 0, len(leftIndex))
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(db.superNode.BlkID, int16(curPos), string(leftIndex)))

	curPos = dataorg.NodeNextField(rootEnt.Block[:], curPos)
	dataorg.NodeSetKeyOrValue(rootEnt.Block[:], curPos, []byte(key), 0, len(key))
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(db.superNode.BlkID, int16(curPos), string(key)))

	curPos = dataorg.NodeNextField(rootEnt.Block[:], curPos)
	dataorg.NodeSetKeyOrValue(rootEnt.Block[:], curPos, []byte(rightIndex), 0, len(rightIndex))
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(db.superNode.BlkID, int16(curPos), string(rightIndex)))

	len := dataorg.NodeGetHeaderLen(rootEnt.Block[:]) + int16(len(leftIndex)) + int16(dataorg.NodeIndexLenSize) + int16(dataorg.INodeGetPairLen(key, rightIndex))
	dataorg.NodeSetLen(rootEnt.Block[:], len)
	t, _ := conv.Itob(len)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(rootEnt.BlkID, int16(dataorg.NodeOffLen), string(t)))

	dataorg.NodeSetLevel(rootEnt.Block[:], level+1)
	t, _ = conv.Itob(level)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(rootEnt.BlkID, int16(dataorg.NodeOffLevel), string(t)))

	leftEntBlkNum, _ := conv.Btoi([]byte(leftIndex))
	leftEnt, _ := db.cache.GetEnt(db.dbFile, leftEntBlkNum, true)
	dataorg.NodeSetParent(leftEnt.Block[:], rootEnt.BlkID)
	leftEnt.SetStat(leftEnt.GetStat() | cache.EntStatDelaywrite)
	db.cache.ReleaseEnt(leftEnt)

	rightEntBlkNum, _ := conv.Btoi([]byte(rightIndex))
	rightEnt, _ := db.cache.GetEnt(db.dbFile, rightEntBlkNum, true)
	dataorg.NodeSetParent(rightEnt.Block[:], rootEnt.BlkID)
	rightEnt.SetStat(rightEnt.GetStat() | cache.EntStatDelaywrite)
	db.cache.ReleaseEnt(rightEnt)
}

func (db *GpdDb) insertPairInInternal(ent *cache.Ent, key string, index string, pos int) {
	dataorg.INodeInsertPair(ent.Block[:], key, index, pos)
	db.reLog.WriteLogRecord(relog.NewLogRecordUserOpPut(ent.BlkID, key, index)) //log

	len := dataorg.NodeGetLen(ent.Block[:]) + int16(dataorg.INodeGetPairLen(key, index))
	dataorg.NodeSetLen(ent.Block[:], len)
	t, _ := conv.Itob(int16(len))
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(ent.BlkID, int16(dataorg.NodeOffLen), string(t))) //log
}

func (db *GpdDb) insertPairInLeaf(ent *cache.Ent, key string, value string, pos int) {
	dataorg.DNodeInsertPair(ent.Block[:], key, value, pos)
	db.reLog.WriteLogRecord(relog.NewLogRecordUserOpPut(ent.BlkID, key, value)) //log

	len := dataorg.NodeGetLen(ent.Block[:]) + int16(dataorg.DNodeGetPairLen(key, value))
	dataorg.NodeSetLen(ent.Block[:], len)
	t, _ := conv.Itob(int16(len))
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(ent.BlkID, int16(dataorg.NodeOffLen), string(t))) //log
}

func (db *GpdDb) splitLeaf(ent *cache.Ent, secEnt *cache.Ent) int {
	splitPos := dataorg.DNodeFindSplitPos(ent.Block[:])
	readwrite.WriteByte(secEnt.Block[:], int(dataorg.NodeGetHeaderLen(secEnt.Block[:])),
		ent.Block[:], splitPos, int(dataorg.NodeGetLen(ent.Block[:]))-splitPos)
	for curPos := int16(splitPos); curPos < dataorg.NodeGetLen(ent.Block[:]); {
		key := dataorg.NodeGetKeyOrValue(ent.Block[:], int(curPos))
		value := dataorg.NodeGetKeyOrValue(ent.Block[:], dataorg.NodeNextField(ent.Block[:], int(curPos)))

		db.reLog.WriteLogRecord(relog.NewLogRecordUserOpPut(secEnt.BlkID, string(key), string(value))) //log
		db.reLog.WriteLogRecord(relog.NewLogRecordUserOpDelete(ent.BlkID, string(key), string(value))) //log

		curPos = int16(dataorg.NodeNextKey(ent.Block[:], int(curPos)))
	}

	dataorg.NodeSetNext(ent.Block[:], secEnt.BlkID)
	t, _ := conv.Itob(secEnt.BlkID)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(ent.BlkID, int16(dataorg.NodeOffNext), string(t))) //log

	secEntLen := dataorg.NodeGetHeaderLen(secEnt.Block[:]) + dataorg.NodeGetLen(ent.Block[:]) - int16(splitPos)
	dataorg.NodeSetLen(secEnt.Block[:], secEntLen)
	t, _ = conv.Itob(secEntLen)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(secEnt.BlkID, int16(dataorg.NodeOffLen), string(t))) //log

	parent := dataorg.NodeGetParent(ent.Block[:])
	dataorg.NodeSetParent(secEnt.Block[:], parent)
	t, _ = conv.Itob(parent)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(secEnt.BlkID, int16(dataorg.NodeOffParent), string(t))) //log

	dataorg.NodeSetLen(ent.Block[:], int16(splitPos))
	t, _ = conv.Itob(int16(splitPos))
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(ent.BlkID, int16(dataorg.NodeOffLen), string(t))) //log

	return splitPos
}

func (db *GpdDb) splitIndex(ent *cache.Ent, secEnt *cache.Ent) (int, string) {
	leftSplitPos := dataorg.INodeFindSplitPos(ent.Block[:])
	rightSplitPos := dataorg.NodeNextField(ent.Block[:], leftSplitPos)
	readwrite.WriteByte(secEnt.Block[:], int(dataorg.NodeGetHeaderLen(secEnt.Block[:])),
		ent.Block[:], rightSplitPos, int(dataorg.NodeGetLen(ent.Block[:]))-rightSplitPos)

	splitKey := dataorg.NodeGetKeyOrValue(ent.Block[:], leftSplitPos)
	splitIndex := dataorg.NodeGetKeyOrValue(ent.Block[:], rightSplitPos)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(secEnt.BlkID, dataorg.NodeGetHeaderLen(secEnt.Block[:]), string(splitIndex)))
	db.reLog.WriteLogRecord(relog.NewLogRecordUserOpDelete(ent.BlkID, string(splitKey), string(splitIndex))) //log
	for curPos := int16(dataorg.NodeNextKey(ent.Block[:], leftSplitPos)); curPos < dataorg.NodeGetLen(ent.Block[:]); {
		key := dataorg.NodeGetKeyOrValue(ent.Block[:], int(curPos))
		index := dataorg.NodeGetKeyOrValue(ent.Block[:], dataorg.NodeNextField(ent.Block[:], int(curPos)))

		db.reLog.WriteLogRecord(relog.NewLogRecordUserOpPut(secEnt.BlkID, string(key), string(index))) //log
		db.reLog.WriteLogRecord(relog.NewLogRecordUserOpDelete(ent.BlkID, string(key), string(index))) //log

		curPos = int16(dataorg.NodeNextKey(secEnt.Block[:], int(curPos)))
	}

	dataorg.NodeSetLen(ent.Block[:], int16(leftSplitPos))
	t, _ := conv.Itob(int16(leftSplitPos))
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(ent.BlkID, int16(dataorg.NodeOffLen), string(t))) //log

	dataorg.NodeSetNext(ent.Block[:], secEnt.BlkID)
	t, _ = conv.Itob(secEnt.BlkID)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(ent.BlkID, int16(dataorg.NodeOffNext), string(t))) //log

	secEntLen := dataorg.NodeGetHeaderLen(secEnt.Block[:]) + dataorg.NodeGetLen(ent.Block[:]) - int16(rightSplitPos)
	dataorg.NodeSetLen(secEnt.Block[:], secEntLen)
	t, _ = conv.Itob(secEntLen)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(secEnt.BlkID, int16(dataorg.NodeOffLen), string(t))) //log

	parent := dataorg.NodeGetParent(ent.Block[:])
	dataorg.NodeSetParent(secEnt.Block[:], parent)
	t, _ = conv.Itob(parent)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(secEnt.BlkID, int16(dataorg.NodeOffParent), string(t))) //log

	level := dataorg.NodeGetLevel(ent.Block[:])
	dataorg.NodeSetLevel(secEnt.Block[:], level)
	t, _ = conv.Itob(level)
	db.reLog.WriteLogRecord(relog.NewLogRecordSetField(secEnt.BlkID, int16(dataorg.NodeOffLevel), string(t))) //log
	return leftSplitPos, string(splitKey)
}
