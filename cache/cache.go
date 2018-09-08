package cache

import (
	"os"

	"github.com/erician/gpdDB/dataorg"

	"github.com/erician/gpdDB/common/gpdconst"
)

//Cache manage ents and free ents
type Cache struct {
	ents     *[gpdconst.CacheEntDefaultNum]*Ent
	freeEnts *FreeEnts
}

//NewCache create a new cache
func NewCache() (cache *Cache) {
	cache = new(Cache)
	cache.ents = new([gpdconst.CacheEntDefaultNum]*Ent)
	cache.freeEnts = NewFreeEnts()
	for i := 0; i < int(gpdconst.CacheEntDefaultNum); i++ {
		cache.freeEnts.PushRight(NewEnt())
	}
	return
}

//GetEnt get an ent from cache, seach ents first, then search freeEnts
//When get ent from freeEnts and blkNum is NotAllocatedBlockID, we won't read from dbFile
func (cache *Cache) GetEnt(dbFile *os.File, blkNum int64) (ent *Ent, err error) {
	ent = getEntFromHashLinkList(cache.ents[blkNum%gpdconst.CacheEntDefaultNum], blkNum)
	if ent != nil {
		cache.freeEnts.RemoveWithBlkNum(blkNum)
	} else {
		ent = cache.freeEnts.PopLeft()
		if blkNum != gpdconst.NotAllocatedBlockID {
			if err = ent.ReadBlk(dbFile, blkNum); err == nil {
				putEntInHashLinkList(cache.ents, ent, blkNum)
			} else {
				cache.freeEnts.PushLeft(ent)
			}
		} else {
			dataorg.NodeInit(ent.Block[:])
			putEntInHashLinkList(cache.ents, ent, blkNum)
		}
	}
	return
}

func putEntInHashLinkList(ents *[gpdconst.CacheEntDefaultNum]*Ent, ent *Ent, blkNum int64) {
	ent.BlkID = blkNum
	ent.Next = nil
	ent.Prev = nil
	ents[blkNum%gpdconst.CacheEntDefaultNum] = ent
	if ents[blkNum%gpdconst.CacheEntDefaultNum] != nil {
		ent.Next = ents[blkNum%gpdconst.CacheEntDefaultNum]
		ents[blkNum%gpdconst.CacheEntDefaultNum].Prev = ent
		ents[blkNum%gpdconst.CacheEntDefaultNum] = ent
	}
}

func getEntFromHashLinkList(cur *Ent, blkNum int64) *Ent {
	if cur == nil {
		return nil
	}
	for cur != nil && cur.BlkID != blkNum {
		cur = cur.Next
	}
	return cur
}
