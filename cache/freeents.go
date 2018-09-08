package cache

import "sync"

//FreeEnts manage all free ents
type FreeEnts struct {
	sen    *Ent
	size   int64
	locker *sync.Mutex
	cond   *sync.Cond
}

//NewFreeEnts create a FreeEnts, and init it
func NewFreeEnts() (freeEnts *FreeEnts) {
	freeEnts = new(FreeEnts)
	freeEnts.sen = new(Ent)
	freeEnts.sen.FreeNext = freeEnts.sen
	freeEnts.sen.FreePrev = freeEnts.sen
	freeEnts.size = 0
	freeEnts.locker = new(sync.Mutex)
	freeEnts.cond = sync.NewCond(freeEnts.locker)
	return
}

//Size get the number of the freeEnts's ents
func (freeEnts *FreeEnts) Size() int64 {
	return freeEnts.size
}

//PushRight push a ent into the back of the freeEnts
func (freeEnts *FreeEnts) PushRight(ent *Ent) {
	freeEnts.locker.Lock()
	defer freeEnts.locker.Unlock()
	freeEnts.sen.FreePrev.FreeNext = ent
	ent.FreePrev = freeEnts.sen.FreePrev
	ent.FreeNext = freeEnts.sen
	freeEnts.sen.FreePrev = ent
	freeEnts.size++
	if freeEnts.size == 1 {
		freeEnts.cond.Broadcast()
	}
}

//PushLeft push a ent into the front of the freeEnts
func (freeEnts *FreeEnts) PushLeft(ent *Ent) {
	freeEnts.locker.Lock()
	defer freeEnts.locker.Unlock()
	freeEnts.sen.FreeNext.FreePrev = ent
	ent.FreeNext = freeEnts.sen.FreeNext
	ent.FreePrev = freeEnts.sen
	freeEnts.sen.FreeNext = ent
	freeEnts.size++
	if freeEnts.size == 1 {
		freeEnts.cond.Broadcast()
	}
}

//PopRight pop a ent from the back
func (freeEnts *FreeEnts) PopRight() (ent *Ent) {
	freeEnts.locker.Lock()
	defer freeEnts.locker.Unlock()
	for freeEnts.size == 0 {
		freeEnts.cond.Wait()
	}
	ent = freeEnts.sen.FreePrev
	ent.FreePrev.FreeNext = freeEnts.sen
	freeEnts.sen.FreePrev = ent.FreePrev
	freeEnts.size--
	return
}

//PopLeft pop a ent from the front
func (freeEnts *FreeEnts) PopLeft() (ent *Ent) {
	freeEnts.locker.Lock()
	defer freeEnts.locker.Unlock()
	for freeEnts.size == 0 {
		freeEnts.cond.Wait()
	}
	ent = freeEnts.sen.FreeNext
	ent.FreeNext.FreePrev = freeEnts.sen
	freeEnts.sen.FreeNext = ent.FreeNext
	freeEnts.size--
	return
}

//RemoveWithBlkNum remove ent with blkNum
func (freeEnts *FreeEnts) RemoveWithBlkNum(blkNum int64) {
	freeEnts.locker.Lock()
	defer freeEnts.locker.Unlock()
	cur := freeEnts.sen.FreeNext
	for cur != freeEnts.sen && cur.BlkID != blkNum {
		cur = cur.FreeNext
	}
	if cur != freeEnts.sen {
		cur.FreePrev.FreeNext = cur.FreeNext
		cur.FreeNext.FreePrev = cur.FreePrev
	}
}
