package gpddb

import (
	"github.com/erician/gpdDB/cache"
	"github.com/erician/gpdDB/dataorg"
	"github.com/erician/gpdDB/errors"
	"github.com/erician/gpdDB/utils/byteutil"
)

//Get get a value from db, wth the key
func (db *GpdDb) Get(key string) (value string, err error) {
	curNode := db.rootNode
	for dataorg.NodeIsLeaf(curNode.Block[:]) == false {
		index := dataorg.INodeFindIndex(curNode.Block[:], key)
		if curNode != db.rootNode {
			db.cache.ReleaseEnt(curNode)
		}
		if curNode, err = db.cache.GetEnt(db.dbFile, index, true); err != nil {
			return value, errors.NewErrGetFailed(err.Error())
		}
	}
	value, err = getValueFromLeaf(curNode, key)
	if curNode != db.rootNode {
		db.cache.ReleaseEnt(curNode)
	}
	return
}

func getValueFromLeaf(ent *cache.Ent, key string) (value string, err error) {
	for curPos := int(dataorg.NodeGetHeaderLen(ent.Block[:])); curPos < int(dataorg.NodeGetLen(ent.Block[:])); {
		if byteutil.ByteCmp([]byte(key), dataorg.NodeGetKeyOrValue(ent.Block[:], curPos)) == 0 {
			return string(dataorg.NodeGetKeyOrValue(ent.Block[:], dataorg.NodeNextField(ent.Block[:], curPos))), nil
		}
		curPos = dataorg.NodeNextKey(ent.Block[:], curPos)
	}
	return value, errors.NewErrKeyNotExist(key + " not exists")
}
