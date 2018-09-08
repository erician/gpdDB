package ds

//ds means data-struct
import (
	"sync"
)

//Cdll cdll means circular, doubly linked list.
//In this directory, we implement it with a sentinel(哨兵).
//And it is thread-safe
type Cdll struct {
	sen    CdllNode
	size   int64
	locker *sync.Mutex
	cond   *sync.Cond
}

//CdllNode the interface of cdll Node
type CdllNode interface {
	SetNext(nextNode CdllNode)
	GetNext() CdllNode
	SetPrev(prevNode CdllNode)
	GetPrev() CdllNode
}

//CdllCommNode cdll common Node
type CdllCommNode struct {
	next CdllNode
	prev CdllNode
}

//SetNext set the field of next
func (node *CdllCommNode) SetNext(nextNode CdllNode) {
	node.next = nextNode
}

//GetNext get the field of next
func (node *CdllCommNode) GetNext() CdllNode {
	return node.next
}

//SetPrev set the field of Prev
func (node *CdllCommNode) SetPrev(prevNode CdllNode) {
	node.prev = prevNode
}

//GetPrev get the field of Prev
func (node *CdllCommNode) GetPrev() CdllNode {
	return node.prev
}

//NewCdll create a cdll, and init it
func NewCdll() (cdll *Cdll) {
	cdll = new(Cdll)
	cdll.sen = new(CdllCommNode)
	cdll.sen.SetNext(cdll.sen)
	cdll.sen.SetPrev(cdll.sen)
	cdll.size = 0
	cdll.locker = new(sync.Mutex)
	cdll.cond = sync.NewCond(cdll.locker)
	return
}

//Size get the number of the cdll's nodes
func (cdll *Cdll) Size() int64 {
	return cdll.size
}

//PushRight push a node into the back of the cdll
func (cdll *Cdll) PushRight(node CdllNode) {
	cdll.locker.Lock()
	defer cdll.locker.Unlock()
	cdll.sen.GetPrev().SetNext(node)
	node.SetPrev(cdll.sen.GetPrev())
	node.SetNext(cdll.sen)
	cdll.sen.SetPrev(node)
	cdll.size++
	if cdll.size == 1 {
		cdll.cond.Broadcast()
	}
}

//PushLeft push a node into the front of the cdll
func (cdll *Cdll) PushLeft(node CdllNode) {
	cdll.locker.Lock()
	defer cdll.locker.Unlock()
	cdll.sen.GetNext().SetPrev(node)
	node.SetNext(cdll.sen.GetNext())
	node.SetPrev(cdll.sen)
	cdll.sen.SetNext(node)
	cdll.size++
	if cdll.size == 1 {
		cdll.cond.Broadcast()
	}
}

//PopRight pop a node from the back
func (cdll *Cdll) PopRight() (node CdllNode) {
	cdll.locker.Lock()
	defer cdll.locker.Unlock()
	for cdll.size == 0 {
		cdll.cond.Wait()
	}
	node = cdll.sen.GetPrev()
	node.GetPrev().SetNext(cdll.sen)
	cdll.sen.SetPrev(node.GetPrev())
	cdll.size--
	return
}

//PopLeft pop a node from the front
func (cdll *Cdll) PopLeft() (node CdllNode) {
	cdll.locker.Lock()
	defer cdll.locker.Unlock()
	for cdll.size == 0 {
		cdll.cond.Wait()
	}
	node = cdll.sen.GetNext()
	node.GetNext().SetPrev(cdll.sen)
	cdll.sen.SetNext(node.GetNext())
	cdll.size--
	return
}
