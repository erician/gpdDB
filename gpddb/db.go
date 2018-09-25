package gpddb

import (
	"os"

	"github.com/erician/gpdDB/cache"
	"github.com/erician/gpdDB/common/gpdconst"
	"github.com/erician/gpdDB/dataorg"
	"github.com/erician/gpdDB/errors"
	"github.com/erician/gpdDB/relog"
)

//GpdDb as the name
type GpdDb struct {
	dbFile    *os.File
	reLog     *relog.RecoveryLog
	cache     *cache.Cache
	superNode *cache.Ent
	rootNode  *cache.Ent
}

//NewDb create a new db
func NewDb(dbName string) (db *GpdDb, err error) {
	if _, err = os.Stat(dbName); os.IsNotExist(err) == false {
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
	err = db.init(dbName)
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

//Close close db
func (db *GpdDb) Close() (err error) {
	if err = db.superNode.WriteBlk(db.dbFile); err != nil {
		return errors.NewErrCloseFailed(err.Error())
	}
	if err = db.rootNode.WriteBlk(db.dbFile); err != nil {
		return errors.NewErrCloseFailed(err.Error())
	}
	if err = db.cache.Close(db.dbFile); err != nil {
		return errors.NewErrCloseFailed(err.Error())
	}
	if err = db.reLog.Close(); err != nil {
		return errors.NewErrCloseFailed(err.Error())
	}
	return
}

func (db *GpdDb) init(dbName string) (err error) {
	if db.dbFile, err = dataorg.NewDbFile(dbName); err != nil {
		return errors.NewErrCannotCreateOrOpenDbFile(err.Error())
	}
	db.cache = cache.NewCache()
	if db.superNode, err = db.getSuperNode(); err != nil {
		return errors.NewErrFailedToGetNode(err.Error())
	}
	if db.rootNode, err = db.getRootNode(); err != nil {
		return errors.NewErrFailedToGetNode(err.Error())
	}
	if db.reLog, err = relog.NewRecoveryLog(dbName); err != nil {
		return errors.NewErrCannotCreateOrOpenRecoveryLogFile(err.Error())
	}
	return
}

func (db *GpdDb) getNode(blkNum int64, doesReadFromFile bool) (ent *cache.Ent, err error) {
	return db.cache.GetEnt(db.dbFile, blkNum, doesReadFromFile)
}

func (db *GpdDb) getSuperNode() (ent *cache.Ent, err error) {
	return db.getNode(dataorg.SuperNodeConstValueBlkID, true)
}

func (db *GpdDb) getRootNode() (ent *cache.Ent, err error) {
	//there still exist bugs
	rootNodeID := db.getRootNodeID()
	if rootNodeID == gpdconst.NotAllocatedBlockID {
		nextBlkNum := dataorg.SuperNodeGetNextBlkNum(db.superNode.Block[:])
		if ent, err = db.cache.GetEnt(db.dbFile, nextBlkNum, rootNodeID != gpdconst.NotAllocatedBlockID); err == nil {
			dataorg.SuperNodeSetRootNodeID(db.superNode.Block[:], nextBlkNum)
			dataorg.SuperNodeSetNextBlkNum(db.superNode.Block[:], nextBlkNum+1)
			err = db.superNode.SyncBlk(db.dbFile)
		}
	} else {
		ent, err = db.cache.GetEnt(db.dbFile, rootNodeID, true)
	}
	return
}

func (db *GpdDb) getRootNodeID() int64 {
	return dataorg.SuperNodeGetRootNodeID(db.superNode.Block[:])
}

func (db *GpdDb) getNewEnt() (ent *cache.Ent) {
	nextBlkNum := dataorg.SuperNodeGetNextBlkNum(db.superNode.Block[:])
	ent, _ = db.cache.GetEnt(db.dbFile, nextBlkNum, false)
	dataorg.SuperNodeSetNextBlkNum(db.superNode.Block[:], nextBlkNum+1)
	db.reLog.WriteLogRecord(relog.NewLogRecordAllocate(nextBlkNum))
	return
}
