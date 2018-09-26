package cache

import (
	"os"

	"github.com/erician/gpdDB/blkio"

	"github.com/erician/gpdDB/common/gpdconst"
)

//the state of Ent
const (
	EntStatValid      int8 = 1 //didn't know how to use this state
	EntStatDelaywrite int8 = 2
	EntStatLocked     int8 = 4 //when locked, must wait, can not write or read
	EntStatInFreeEnts int8 = 8 //when the ent is in FreeEnts, set the bit, to avoid multiple push
)

//Ent the entry of cache
type Ent struct {
	BlkID     int64
	Reference int16 //the number of reference
	Stat      int8  //the state of this Ent
	Block     *[gpdconst.CacheEntDefaultSize]byte
	Next      *Ent //used by hash table
	Prev      *Ent
	FreeNext  *Ent //used by freeents
	FreePrev  *Ent
}

//NewEnt create a new ent
func NewEnt() (ent *Ent) {
	ent = new(Ent)
	ent.BlkID = 0
	ent.Reference = 0
	ent.Stat = 0
	ent.Block = new([gpdconst.CacheEntDefaultSize]byte)
	ent.Next = nil
	ent.Prev = nil
	ent.FreeNext = nil
	ent.FreePrev = nil
	return
}

//ReadBlk read a block from file, and put into ent.Block
func (ent *Ent) ReadBlk(file *os.File, blkNum int64) error {
	return blkio.ReadBlk(file, ent.Block[:], blkNum)
}

//WriteBlk write a block to file
func (ent *Ent) WriteBlk(file *os.File) error {
	return blkio.WriteBlk(file, ent.Block[:], ent.BlkID)
}

//SyncBlk sync a block
func (ent *Ent) SyncBlk(file *os.File) (err error) {
	if err = blkio.WriteBlk(file, ent.Block[:], ent.BlkID); err != nil {
		return
	}
	if err = blkio.SyncFile(file); err != nil {
		return
	}
	return
}

//SetStat set the ent's state
func (ent *Ent) SetStat(stat int8) {
	ent.Stat = stat
}

//GetStat get the ent's state
func (ent *Ent) GetStat() int8 {
	return ent.Stat
}
