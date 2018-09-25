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
	for i := 0; i < int(gpdconst.CacheEntDefaultNum); i++ {
		cache.ents[i] = NewEnt()
		cache.ents[i].Next = cache.ents[i]
		cache.ents[i].Prev = cache.ents[i]
	}
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
		removeEntFromHashLinkList(cache.ents[ent.BlkID%gpdconst.CacheEntDefaultNum], ent.BlkID)
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
		putEntInHashLinkList(cache.ents[blkNum%gpdconst.CacheEntDefaultNum], ent)
	}
	return
}

func putEntInHashLinkList(sen *Ent, ent *Ent) {
	sen.Next.Prev = ent
	ent.Next = sen.Next
	ent.Prev = sen
	sen.Next = ent
}

func getEntFromHashLinkList(sen *Ent, blkNum int64) *Ent {
	cur := sen.Next
	for cur != sen && cur.BlkID != blkNum {
		cur = cur.Next
	}
	if cur == sen {
		return nil
	}
	return cur
}

//if the ent of blkNum exists, remove it, or do nothing
func removeEntFromHashLinkList(sen *Ent, blkNum int64) {
	cur := sen.Next
	for cur != sen && cur.BlkID != blkNum {
		cur = cur.Next
	}
	if cur != sen {
		cur.Prev.Next = cur.Next
		cur.Next.Prev = cur.Prev
	}
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
