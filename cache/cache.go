package cache

import (
	"os"

	"github.com/erician/gpdDB/blkio"

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
func (cache *Cache) GetEnt(file *os.File, blkNum int64, doesReadFromFile bool) (ent *Ent, err error) {
	ent = getEntFromHashLinkList(cache.ents[blkNum%gpdconst.CacheEntDefaultNum], blkNum)
	if ent != nil {
		cache.freeEnts.RemoveWithBlkNum(blkNum)
	} else {
		ent = cache.freeEnts.PopLeft()
		removeEntFromHashLinkList(cache.ents, blkNum)
		if ent.GetStat()&EntStatDelaywrite == EntStatDelaywrite {
			if err = ent.WriteBlk(file); err != nil { //can be optimizated with go routine
				return
			}
		}
		ent.BlkID = blkNum
		if doesReadFromFile == true {
			if err = ent.ReadBlk(file, blkNum); err != nil {
				cache.freeEnts.PushLeft(ent)
				return
			}
		} else {
			dataorg.NodeInit(ent.Block[:])
			dataorg.NodeSetBlkID(ent.Block[:], blkNum)
		}
		putEntInHashLinkList(cache.ents, ent, blkNum)
	}
	return
}

func putEntInHashLinkList(ents *[gpdconst.CacheEntDefaultNum]*Ent, ent *Ent, blkNum int64) {
	ent.BlkID = blkNum
	ent.Next = nil
	ent.Prev = nil
	if ents[blkNum%gpdconst.CacheEntDefaultNum] != nil {
		ent.Next = ents[blkNum%gpdconst.CacheEntDefaultNum]
		ents[blkNum%gpdconst.CacheEntDefaultNum].Prev = ent
		ents[blkNum%gpdconst.CacheEntDefaultNum] = ent
	} else {
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

//if the ent of blkNum exists, remove it, or do nothing
func removeEntFromHashLinkList(ents *[gpdconst.CacheEntDefaultNum]*Ent, blkNum int64) {
	cur := ents[blkNum%gpdconst.CacheEntDefaultNum]
	prev := cur
	for cur != nil && cur.BlkID != blkNum {
		prev = cur
		cur = cur.Next
	}
	if cur == nil {
		return
	}
	if cur == prev {
		ents[blkNum%gpdconst.CacheEntDefaultNum] = cur.Next
	} else {
		prev.Next = cur.Next
		if cur.Next != nil {
			cur.Next.Prev = prev
		}
	}
	cur.Next = nil
	cur.Prev = nil
}

//ReleaseEnt release a ent
func (cache *Cache) ReleaseEnt(ent *Ent) {
	cache.freeEnts.PushRight(ent)
}

//Close sync ents with delay write and sync dbfiel
func (cache *Cache) Close(file *os.File) (err error) {
	freeEntsSize := int(cache.freeEnts.Size())
	for i := 0; i < freeEntsSize; i++ {
		if ent := cache.freeEnts.PopLeft(); (ent.GetStat() & EntStatDelaywrite) == EntStatDelaywrite {
			if err = ent.WriteBlk(file); err != nil {
				return
			}
		}
	}
	if err = blkio.SyncFile(file); err != nil {
		return
	}
	return file.Close()
}
