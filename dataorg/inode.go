package dataorg

import (
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
	for curPos < int(NodeGetLen(node)) && byteutil.ByteCmp([]byte(key), NodeGetKeyOrValue(node, curPos)) <= 0 {
		leftIndexPos = NodeNextField(node, curPos)
		curPos = NodeNextKey(node, curPos)
	}
	index, _ = conv.Btoi(NodeGetKeyOrValue(node, leftIndexPos))
	return
}
