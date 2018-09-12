package dataorg

import (
	"github.com/erician/gpdDB/common/gpdconst"
	"github.com/erician/gpdDB/utils/byteutil"
	"github.com/erician/gpdDB/utils/conv"
)

//inode means index node, and it has the same header with node
//inode is not leaf node

//INodeFindIndex find the index in inode
func INodeFindIndex(node []byte, key string) (index int64) {
	curPos := int(NodeGetHeaderLen(node))
	leftIndexPos := curPos
	curPos = NodeNextField(node, curPos)
	for curPos < int(NodeGetLen(node)) && byteutil.ByteCmp([]byte(key), NodeGetKeyOrValue(node, curPos)) >= 0 {
		leftIndexPos = NodeNextField(node, curPos)
		curPos = NodeNextKey(node, curPos)
	}
	index, _ = conv.Btoi(NodeGetKeyOrValue(node, leftIndexPos))
	return
}

//INodeFindInsertPos find the insert pos
func INodeFindInsertPos(node []byte, key string) (pos int) {
	pos = NodeNextField(node, int(NodeGetHeaderLen(node)))
	for pos < int(NodeGetLen(node)) && byteutil.ByteCmp([]byte(key), NodeGetKeyOrValue(node, pos)) >= 0 {
		pos = NodeNextKey(node, pos)
	}
	return
}

//INodeGetPairLen get the space that a pair occupies
func INodeGetPairLen(key string, index string) int {
	return len(key) + len(index) + int(NodeKeyLenSize) + int(NodeIndexLenSize)
}

//INodeFindSplitPos return the split pos
//like DNodeFindSplitPos
func INodeFindSplitPos(node []byte) (splitPos int) {
	splitPos = NodeNextField(node, int(NodeGetHeaderLen(node)))
	for splitPos < int(gpdconst.BlockSize/2) {
		splitPos = NodeNextKey(node, splitPos)
	}
	return
}

//INodeInsertPair insert a pair of key-index
//need log
func INodeInsertPair(node []byte, key string, index string, pos int) {
	if int(NodeGetLen(node)) != pos {
		DNodeRightShift(node, pos, INodeGetPairLen(key, index))
	}
	pos = NodeSetKeyOrValue(node, pos, []byte(key), 0, len(key))
	NodeSetKeyOrValue(node, pos, []byte(index), 0, len(index))
}
