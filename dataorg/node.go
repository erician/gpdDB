package dataorg

import (
	"log"

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
	NodeInitValueLeafLevel  int64 = 0x0000 //leaf node's level, which is the smallest
	NodeConstValueHeaderLen int16 = 0x28   //also the offset of the real data
)

//some const values of Node
const (
	NodeKeyLenSize  int64 = 2               //the size of the length of key
	NodeDataLenSize int64 = 2               //the size of the length of data
	NodeSize        int64 = 4 * gpdconst.KB //equal to the block size
)

//NodeInit the Node's common field with the default value
func NodeInit(node []byte) {
	NodeSetLevel(node, NodeInitValueLeafLevel)
	NodeSetLen(node, NodeConstValueHeaderLen)
	NodeSetHeaderLen(node, NodeConstValueHeaderLen)
	NodeSetVersion(node, NodeConstValueVersion)
}

//NodeSetField set a field of node
func NodeSetField(node []byte, pos int, bs []byte, bsStart int, len int) int {
	err := readwrite.WriteByte(node, pos, bs, bsStart, len)
	if err != nil {
		log.Fatal("setfield ", err)
	}
	return pos + len
}

//NodeGetField get a field of node
func NodeGetField(node []byte, pos int, fieldLen int) (bs []byte) {
	bs, err := readwrite.ReadByte(node, pos, fieldLen)
	if err != nil {
		log.Fatal("getblkid ", err)
	}
	return
}

//NodeSetKey set the key of data
func NodeSetKey(node []byte, pos int, bs []byte, bsStart int, len int) int {
	keyLenLenBs, err := conv.Itob(int16(len))
	if err != nil {
		log.Fatal("set key ", err)
	}
	pos = NodeSetField(node, pos, keyLenLenBs, 0, int(NodeKeyLenSize))
	pos = NodeSetField(node, pos, bs, bsStart, len)
	return pos
}

//NodeSetValue set the value of data
//NOTE: for index leaf the value is the blkID
func NodeSetValue(node []byte, pos int, bs []byte, bsStart int, len int) int {
	dataLenLenBs, err := conv.Itob(int16(len))
	if err != nil {
		log.Fatal("set value ", err)
	}
	pos = NodeSetField(node, pos, dataLenLenBs, 0, int(NodeDataLenSize))
	pos = NodeSetField(node, pos, bs, bsStart, len)
	return pos
}

//NodeNextField get the next field pos
func NodeNextField(node []byte, pos int) int {
	bs := NodeGetField(node, pos, int(NodeDataLenSize))
	len, err := conv.Btoi(bs)
	if err != nil {
		log.Fatal("next field ", err)
	}
	return pos + int(NodeDataLenSize) + int(len)
}

//NodeNextKey get next key field pos
func NodeNextKey(node []byte, pos int) int {
	return NodeNextField(node, NodeNextField(node, pos))
}

//NodeSetBlkID set the node's blkID
func NodeSetBlkID(node []byte, blkID int64) {
	bs, err := conv.Itob(blkID)
	if err != nil {
		log.Fatal("setblkid ", err)
	}
	NodeSetField(node, int(NodeOffBlkID), bs, 0, int(NodeFieldSizeBlkID))
}

//NodeGetBklID get the blkID
func NodeGetBklID(node []byte) int64 {
	bs := NodeGetField(node, int(NodeOffBlkID), int(NodeFieldSizeBlkID))
	returnVal, err := conv.Btoi(bs)
	if err != nil {
		log.Fatal("getblkid ", err)
	}
	return int64(returnVal)
}

//NodeSetNext set the node's next
func NodeSetNext(node []byte, next int64) {
	bs, err := conv.Itob(next)
	if err != nil {
		log.Fatal("setnext ", err)
	}
	NodeSetField(node, int(NodeOffNext), bs, 0, int(NodeFieldSizeNext))
}

//NodeGetNext get the next
func NodeGetNext(node []byte) int64 {
	bs := NodeGetField(node, int(NodeOffNext), int(NodeFieldSizeNext))
	returnVal, err := conv.Btoi(bs)
	if err != nil {
		log.Fatal("getnext ", err)
	}
	return int64(returnVal)
}

//NodeSetParent set the node's parent
func NodeSetParent(node []byte, parent int64) {
	bs, err := conv.Itob(parent)
	if err != nil {
		log.Fatal("setparent ", err)
	}
	NodeSetField(node, int(NodeOffParent), bs, 0, int(NodeFieldSizeNext))
}

//NodeGetParent get the parent
func NodeGetParent(node []byte) int64 {
	bs := NodeGetField(node, int(NodeOffParent), int(NodeFieldSizeNext))
	returnVal, err := conv.Btoi(bs)
	if err != nil {
		log.Fatal("getparent ", err)
	}
	return int64(returnVal)
}

//NodeSetLevel set the node's level
func NodeSetLevel(node []byte, level int64) {
	bs, err := conv.Itob(level)
	if err != nil {
		log.Fatal("setlevel ", err)
	}
	NodeSetField(node, int(NodeOffLevel), bs, 0, int(NodeFieldSizeLevel))
}

//NodeGetLevel get the level
func NodeGetLevel(node []byte) int64 {
	bs := NodeGetField(node, int(NodeOffLevel), int(NodeFieldSizeLevel))
	returnVal, err := conv.Btoi(bs)
	if err != nil {
		log.Fatal("getlevel ", err)
	}
	return int64(returnVal)
}

//NodeSetLen set the node's len
func NodeSetLen(node []byte, len int16) {
	bs, err := conv.Itob(len)
	if err != nil {
		log.Fatal("setlen ", err)
	}
	NodeSetField(node, int(NodeOffLen), bs, 0, int(NodeFieldSizeLen))
}

//NodeGetLen get the len
func NodeGetLen(node []byte) int16 {
	bs := NodeGetField(node, int(NodeOffLen), int(NodeFieldSizeLen))
	returnVal, err := conv.Btoi(bs)
	if err != nil {
		log.Fatal("getlen ", err)
	}
	return int16(returnVal)
}

//NodeSetHeaderLen set the node's headerLen
func NodeSetHeaderLen(node []byte, headerLen int16) {
	bs, err := conv.Itob(headerLen)
	if err != nil {
		log.Fatal("setheaderlen ", err)
	}
	NodeSetField(node, int(NodeOffHeaderLen), bs, 0, int(NodeFieldSizeHeaderLen))
}

//NodeGetHeaderLen get the headerLen
func NodeGetHeaderLen(node []byte) int16 {
	bs := NodeGetField(node, int(NodeOffHeaderLen), int(NodeFieldSizeHeaderLen))
	returnVal, err := conv.Btoi(bs)
	if err != nil {
		log.Fatal("getheaderlen ", err)
	}
	return int16(returnVal)
}

//NodeSetVersion set the node's version
func NodeSetVersion(node []byte, version int8) {
	bs, err := conv.Itob(version)
	if err != nil {
		log.Fatal("setversion ", err)
	}
	NodeSetField(node, int(NodeOffVersion), bs, 0, int(NodeFieldSizeVersion))
}

//NodeGetVersion get the version
func NodeGetVersion(node []byte) int8 {
	bs := NodeGetField(node, int(NodeOffVersion), int(NodeFieldSizeVersion))
	returnVal, err := conv.Btoi(bs)
	if err != nil {
		log.Fatal("getversion ", err)
	}
	return int8(returnVal)
}
