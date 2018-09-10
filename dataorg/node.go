package dataorg

import (
	"github.com/erician/gpdDB/common/gpdconst"
	"github.com/erician/gpdDB/utils/conv"
	"github.com/erician/gpdDB/utils/readwrite"
)

//NodeHeader each tree node's header has the following format
//NOTE: the struct of NodeHeader is just to show the format of a block, not used
type NodeHeader struct {
	blkID     int64 //use the block number as the blkId
	next      int64 //link the same level blocks with this field
	parent    int64 //the block number of the parent
	level     int64 //the node level, incrementing from the lowest node, root node is biggest
	len       int16 //the length of the data, including this header
	headerLen int16 //the lenght of this header and also the offset of valid data
	version   int8  //the gpdDB version used for parsing data
	//the rest region whose offset is bigger than 0x28 is data region
}

//Node's offset of all field
const (
	NodeOffBlkID     int64 = 0
	NodeOffNext      int64 = 8
	NodeOffParent    int64 = 16
	NodeOffLevel     int64 = 24
	NodeOffLen       int64 = 32
	NodeOffHeaderLen int64 = 34
	NodeOffVersion   int64 = 36
)

//Node all fields size
const (
	NodeFieldSizeBlkID     int64 = 8
	NodeFieldSizeNext      int64 = 8
	NodeFieldSizeParent    int64 = 8
	NodeFieldSizeLevel     int64 = 8
	NodeFieldSizeLen       int64 = 2
	NodeFieldSizeHeaderLen int64 = 2
	NodeFieldSizeVersion   int64 = 1
)

//some filed value of Node
const (
	NodeConstValueVersion   int8  = 1      //first version
	NodeConstValueLeafLevel int64 = 0x0000 //leaf node's level, which is the smallest
	NodeConstValueHeaderLen int16 = 0x28   //also the offset of the real data
)

//some const values of Node
const (
	NodeKeyLenSize   int64 = 2               //the size of the length of key
	NodeValueLenSize int64 = NodeKeyLenSize  //the size of the length of data
	NodeSize         int64 = 4 * gpdconst.KB //equal to the block size
)

//NodeInit the Node's common field with the default value
func NodeInit(node []byte) {
	NodeSetLevel(node, NodeConstValueLeafLevel)
	NodeSetLen(node, NodeConstValueHeaderLen)
	NodeSetHeaderLen(node, NodeConstValueHeaderLen)
	NodeSetVersion(node, NodeConstValueVersion)

	NodeSetNext(node, gpdconst.NotAllocatedBlockID)
	NodeSetParent(node, gpdconst.NotAllocatedBlockID)
}

//NodeSetField set a field of node
//before using this func, you should test the node and bs' lenght first
func NodeSetField(node []byte, pos int, bs []byte, bsStart int, len int) int {
	readwrite.WriteByte(node, pos, bs, bsStart, len)
	return pos + len
}

//NodeGetField get a field of node
func NodeGetField(node []byte, pos int, fieldLen int) (bs []byte) {
	bs, _ = readwrite.ReadByte(node, pos, fieldLen)
	return
}

//NodeSetKeyOrValue set a key or value
func NodeSetKeyOrValue(node []byte, pos int, bs []byte, bsStart int, len int) int {
	lenBs, _ := conv.Itob(int16(len))
	return NodeSetField(node, NodeSetField(node, pos, lenBs, 0, int(NodeKeyLenSize)), bs, bsStart, len)
}

//NodeGetKeyOrValue get a key or value, inode can also use this func
func NodeGetKeyOrValue(node []byte, pos int) (bs []byte) {
	lenBs := NodeGetField(node, pos, int(NodeKeyLenSize))
	len, _ := conv.Btoi(lenBs)
	return NodeGetField(node, pos+int(NodeKeyLenSize), int(len))
}

//NodeNextField get the next field pos
func NodeNextField(node []byte, pos int) int {
	bs := NodeGetField(node, pos, int(NodeValueLenSize))
	len, _ := conv.Btoi(bs)
	return pos + int(NodeValueLenSize) + int(len)
}

//NodeNextKey get next key field pos
func NodeNextKey(node []byte, pos int) int {
	return NodeNextField(node, NodeNextField(node, pos))
}

//NodeSetBlkID set the node's blkID
func NodeSetBlkID(node []byte, blkID int64) {
	bs, _ := conv.Itob(blkID)
	NodeSetField(node, int(NodeOffBlkID), bs, 0, int(NodeFieldSizeBlkID))
}

//NodeGetBklID get the blkID
func NodeGetBklID(node []byte) (blkID int64) {
	bs := NodeGetField(node, int(NodeOffBlkID), int(NodeFieldSizeBlkID))
	blkID, _ = conv.Btoi(bs)
	return
}

//NodeSetNext set the node's next
func NodeSetNext(node []byte, next int64) {
	bs, _ := conv.Itob(next)
	NodeSetField(node, int(NodeOffNext), bs, 0, int(NodeFieldSizeNext))
}

//NodeGetNext get the next
func NodeGetNext(node []byte) (next int64) {
	bs := NodeGetField(node, int(NodeOffNext), int(NodeFieldSizeNext))
	next, _ = conv.Btoi(bs)
	return
}

//NodeSetParent set the node's parent
func NodeSetParent(node []byte, parent int64) {
	bs, _ := conv.Itob(parent)
	NodeSetField(node, int(NodeOffParent), bs, 0, int(NodeFieldSizeNext))
}

//NodeGetParent get the parent
func NodeGetParent(node []byte) (parent int64) {
	bs := NodeGetField(node, int(NodeOffParent), int(NodeFieldSizeNext))
	parent, _ = conv.Btoi(bs)
	return
}

//NodeSetLevel set the node's level
func NodeSetLevel(node []byte, level int64) {
	bs, _ := conv.Itob(level)
	NodeSetField(node, int(NodeOffLevel), bs, 0, int(NodeFieldSizeLevel))
}

//NodeGetLevel get the level
func NodeGetLevel(node []byte) (level int64) {
	bs := NodeGetField(node, int(NodeOffLevel), int(NodeFieldSizeLevel))
	level, _ = conv.Btoi(bs)
	return
}

//NodeSetLen set the node's len
func NodeSetLen(node []byte, len int16) {
	bs, _ := conv.Itob(len)
	NodeSetField(node, int(NodeOffLen), bs, 0, int(NodeFieldSizeLen))
}

//NodeGetLen get the len
func NodeGetLen(node []byte) int16 {
	bs := NodeGetField(node, int(NodeOffLen), int(NodeFieldSizeLen))
	len, _ := conv.Btoi(bs)
	return int16(len)
}

//NodeSetHeaderLen set the node's headerLen
func NodeSetHeaderLen(node []byte, headerLen int16) {
	bs, _ := conv.Itob(headerLen)
	NodeSetField(node, int(NodeOffHeaderLen), bs, 0, int(NodeFieldSizeHeaderLen))
}

//NodeGetHeaderLen get the headerLen
func NodeGetHeaderLen(node []byte) int16 {
	bs := NodeGetField(node, int(NodeOffHeaderLen), int(NodeFieldSizeHeaderLen))
	headerLen, _ := conv.Btoi(bs)
	return int16(headerLen)
}

//NodeSetVersion set the node's version
func NodeSetVersion(node []byte, version int8) {
	bs, _ := conv.Itob(version)
	NodeSetField(node, int(NodeOffVersion), bs, 0, int(NodeFieldSizeVersion))
}

//NodeGetVersion get the version
func NodeGetVersion(node []byte) int8 {
	bs := NodeGetField(node, int(NodeOffVersion), int(NodeFieldSizeVersion))
	version, _ := conv.Btoi(bs)
	return int8(version)
}

//NodeIsLeaf decide whether the node is leaf
func NodeIsLeaf(node []byte) bool {
	return NodeGetLevel(node) == NodeConstValueLeafLevel
}

//NodeIsEnoughSpaceLeft to be sure if there is enough space to hold this pair
func NodeIsEnoughSpaceLeft(node []byte, pairLen int64) bool {
	return int64(NodeGetLen(node))+pairLen < gpdconst.BlockSize
}
