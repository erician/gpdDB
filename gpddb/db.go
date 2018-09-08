package gpddb

import (
	"os"

	"github.com/erician/gpdDB/common/gpdconst"

	"github.com/erician/gpdDB/cache"
	"github.com/erician/gpdDB/dataorg"
	"github.com/erician/gpdDB/errors"
	"github.com/erician/gpdDB/relog"
)

//GpdDb as the name
type GpdDb struct {
	dbFile    *os.File
	reLog     *relog.RecoveryLog
	cache     *cache.Cache
	superNode *Ent
	rootNode  *Ent
}

//NewDb create a new db
func NewDb(dbName string) (db *GpdDb, err error) {
	if _, err = os.Stat(dbName); err != nil {
		return
	}
	if os.IsNotExist(err) == false {
		return db, errors.NewErrDbAlreadyExist(dbName + " already exist, can't create it again")
	}
	db = new(GpdDb)
	err = db.init(dbName)
	return
}

//OpenDb open a db already existed
func OpenDb(dbName string) (db *GpdDb, err error) {
	if _, err = os.Stat(dbName); err != nil {
		return
	}
	if os.IsNotExist(err) == true {
		return db, errors.NewErrDbNotExist(dbName + " does not exist, please check the dbname")
	}
	db = new(GpdDb)
	db.init(dbName)
	return
}

//RemoveDb remove a db
func RemoveDb(dbName string) (err error) {
	if _, err = os.Stat(dbName); err != nil {
		return
	}
	if os.IsNotExist(err) == true {
		return errors.NewErrDbNotExist(dbName + " does not exist, please check the dbname")
	}

	if err = os.Remove(dbName); err != nil {
		return
	}

	if _, err = os.Stat(dbName + relog.RecoveryLogDefaultSuffix); err != nil {
		return
	}
	if os.IsNotExist(err) == false {
		return os.Remove(dbName + relog.RecoveryLogDefaultSuffix)
	}
	return
}

func (db *GpdDb) init(dbName string) (err error) {
	if db.dbFile, err = dataorg.NewDbFile(dbName); err != nil {
		return errors.NewErrCannotCreateOrOpenDbFile(err)
	}
	db.cache = cache.NewCache()
	if db.superNode, err = db.getSuperNode(); err != nil {
		return errors.FailedToGetNode(err)
	}
	if db.rootNodeID, err = db.getRootNode(); err != nil {
		return errors.FailedToGetNode(err)
	}
	if db.reLog, err = relog.NewRecoveryLog(dbName); err != nil {
		return errors.NewErrCannotCreateOrOpenRecoveryLogFile(err)
	}
}

func (db *GpdDb) getNode(blkNum int64) (ent *Ent, err error) {
	return db.cache.GetEnt(db.dbFile, blkNum)
}

func (db *GpdDb) getSuperNode() (ent *Ent, err error) {
	return getNode(dataorg.SuperNodeConstValueBlkID)
}

func (db *GpdDb) getRootNode() (ent *Ent, err error) {
	//there still exist bugs
	rootNodeID := db.getRootNodeID()
	if ent, err = db.cache.GetEnt(db.dbFile, rootNodeID); err == nil {
		if rootNodeID == gpdconst.NotAllocatedBlockID {
			ent.BlkID = gpdconst.RootNodeInitBlockID
			dataorg.NodeSetBlkID(ent.Block[:], ent.BlkID)
			dataorg.SuperNodeSetRootNodeID(db.superNode.Block[:], ent.BlkID)
			dataorg.SuperNodeSetNextBlkNum(db.superNode.Block[:], gpdconst.RootNodeInitBlockID+1)
			err = superNode.SyncBlk()
		}
	}
	return
}

func (db *GpdDb) getRootNodeID() int64 {
	return dataorg.SuperNodeGetRootNodeID(db.superNode.Block[:])
}
