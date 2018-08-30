package dataorg

import (
	"testing"

	"github.com/erician/gpdDB/utils/byteutil"
)

func TestSuperNodeInit(t *testing.T) {
	node := make([]byte, NodeSize)
	SuperNodeInit(node)
	if c := byteutil.ByteCmp(node[SuperNodeOffBlkID:SuperNodeOffBlkID+SuperNodeFieldSizeBlkID],
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); c != 0 {
		t.Error("expected: ", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, "not: ",
			node[SuperNodeOffBlkID:SuperNodeOffBlkID+SuperNodeFieldSizeBlkID])
	}
	if c := byteutil.ByteCmp(node[SuperNodeOffRootNodeID:SuperNodeOffRootNodeID+SuperNodeFieldSizeRootNodeID],
		[]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); c != 0 {
		t.Error("expected: ", []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, "not: ",
			node[SuperNodeOffRootNodeID:SuperNodeOffRootNodeID+SuperNodeFieldSizeRootNodeID])
	}
	if c := byteutil.ByteCmp(node[SuperNodeOffAllPairsNum:SuperNodeOffAllPairsNum+SuperNodeFieldSizeAllPairsNum],
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); c != 0 {
		t.Error("expected: ", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, "not: ",
			node[SuperNodeOffAllPairsNum:SuperNodeOffAllPairsNum+SuperNodeFieldSizeAllPairsNum])
	}
	if c := byteutil.ByteCmp(node[SuperNodeOffNextBlkNum:SuperNodeOffNextBlkNum+SuperNodeFieldSizeNextBlkNum],
		[]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); c != 0 {
		t.Error("expected: ", []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, "not: ",
			node[SuperNodeOffNextBlkNum:SuperNodeOffNextBlkNum+SuperNodeFieldSizeNextBlkNum])
	}
	if c := byteutil.ByteCmp(node[SuperNodeOffLen:SuperNodeOffLen+SuperNodeFieldSizeLen],
		[]byte{0x23, 0x00}); c != 0 {
		t.Error("expected: ", []byte{0x23, 0x00}, "not: ",
			node[SuperNodeOffLen:SuperNodeOffLen+SuperNodeFieldSizeLen])
	}
	if c := byteutil.ByteCmp(node[SuperNodeOffVersion:SuperNodeOffVersion+SuperNodeFieldSizeVersion],
		[]byte{0x01}); c != 0 {
		t.Error("expected: ", []byte{0x01}, "not: ",
			node[SuperNodeOffVersion:SuperNodeOffVersion+SuperNodeFieldSizeVersion])
	}
}
