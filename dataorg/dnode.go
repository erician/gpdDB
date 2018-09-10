package dataorg

import (
	"github.com/erician/gpdDB/common/gpdconst"
	"github.com/erician/gpdDB/utils/byteutil"
)

//dnode means data node, and it has the same header with node
//dnode is leaf node

//DNodeGetPairLen get the space that a pair occupies
func DNodeGetPairLen(key string, value string) int {
	return len(key) + len(value) + int(NodeKeyLenSize) + int(NodeValueLenSize)
}

//DNodeFindInsertPos find the pos where the key shoud insert
func DNodeFindInsertPos(node []byte, key string) (pos int, doesAlreadyExist bool) {
	insertKey := []byte(key)
	nodeLen := NodeGetLen(node)
	pos = int(NodeConstValueHeaderLen)
	for pos < int(nodeLen) {
		desKey := NodeGetKeyOrValue(node, pos)
		result := byteutil.ByteCmp(insertKey, desKey)
		if result >= 0 {
			if result == 0 {
				return pos, true
			}
			return pos, false
		}
		pos = NodeNextKey(node, pos)
	}
	return pos, false
}

//DNodeInsertPair insert a pair.
//need log
func DNodeInsertPair(node []byte, key string, value string, pos int) {
	if int(NodeGetLen(node)) != pos {
		DNodeRightShift(node, pos, DNodeGetPairLen(key, value))
	}
	pos = NodeSetKeyOrValue(node, pos, []byte(key), 0, len(key))
	NodeSetKeyOrValue(node, pos, []byte(value), 0, len(value))
}

//DNodeRightShift logical right shift
func DNodeRightShift(node []byte, pos int, distance int) {
	for i := int(NodeGetLen(node)) - 1; i >= pos; i-- {
		node[distance+i] = node[i]
	}
}

//DNodeLeftShift logical right shift
func DNodeLeftShift(node []byte, pos int, distance int) {
	for i := pos; i < int(NodeGetLen(node)); i++ {
		node[i-distance] = node[i]
	}
}

//DNodeFindSplitPos split the srcNode into secNode
//To be simple, just split from the middile,
//NOTE: the splitPos will be bigger the gpdconst.BlockSize/2
func DNodeFindSplitPos(srcNode []byte) (splitPos int) {
	splitPos = int(NodeGetHeaderLen(srcNode))
	for splitPos < int(gpdconst.BlockSize/2) {
		splitPos = NodeNextKey(srcNode, splitPos)
	}
	return
}

//DNodeDeletePair delete a pair
//need log
func DNodeDeletePair(node []byte, key string, pos int) string {
	nextKeyPos := NodeNextKey(node, pos)
	DNodeLeftShift(node, nextKeyPos, nextKeyPos-pos)
	NodeSetLen(node, int16(nextKeyPos-pos))
	return string(NodeGetKeyOrValue(node, NodeNextField(node, pos))[:])
}
