package gpddb

import (
	"github.com/erician/gpdDB/cache"
	"github.com/erician/gpdDB/common/gpdconst"
	"github.com/erician/gpdDB/dataorg"
)

//Put put a pair of key-value
func (db *GpdDb) Put(key string, value string) (err error) {
	if dataorg.IsLeaf(db.rootNode) == true {
		db.putPairInLeaf(db.rootNode, key, value)
	} else {

	}

	putRecursive()
}

func (db *GpdDb) putPairInLeaf(ent *cache.Ent, key string, value string) {
	insertPos := dataorg.FindInsertPos(ent.Block, key)
	if dataorg.IsEnoughSpaceLeft(ent.Block, len(key)+len(value)+dataorg.NodeKeyLenSize+dataorg.NodeDataLenSize) == false {
		if scdEnt, err := db.cache.GetEnt(db.dbFile, gpdconst.NotAllocatedBlockID); err != nil {

		}
	}
}

//
