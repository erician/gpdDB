package dataorg

import (
	"testing"

	"github.com/erician/gpdDB/utils/byteutil"
)

func TestNodeInit(t *testing.T) {
	node := make([]byte, NodeSize)
	NodeInit(node)
	if c := byteutil.ByteCmp(node[NodeOffLevel:NodeOffLevel+NodeFieldSizeLevel],
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); c != 0 {
		t.Error("expected: ", []byte{0x00, 0x00}, "not: ",
			node[NodeOffLevel:NodeOffLevel+NodeFieldSizeLevel])
	}
	if c := byteutil.ByteCmp(node[NodeOffLen:NodeOffLen+NodeFieldSizeLen],
		[]byte{0x28, 0x00}); c != 0 {
		t.Error("expected: ", []byte{0x28, 0x00}, "not: ",
			node[NodeOffLen:NodeOffLen+NodeFieldSizeLen])
	}
	if c := byteutil.ByteCmp(node[NodeOffHeaderLen:NodeOffHeaderLen+NodeFieldSizeHeaderLen],
		[]byte{0x28, 0x00}); c != 0 {
		t.Error("expected: ", []byte{0x28, 0x00}, "not: ",
			node[NodeOffHeaderLen:NodeOffHeaderLen+NodeFieldSizeHeaderLen])
	}
	if c := byteutil.ByteCmp(node[NodeOffVersion:NodeOffVersion+NodeFieldSizeVersion],
		[]byte{0x01}); c != 0 {
		t.Error("expected: ", []byte{0x01}, "not: ",
			node[NodeOffVersion:NodeOffVersion+NodeFieldSizeVersion])
	}
}

func TestNodeSetField(t *testing.T) {
	node := make([]byte, NodeSize)
	pos := NodeSetField(node, int(NodeConstValueHeaderLen), []byte{0xff}, 0, 1)
	if c := byteutil.ByteCmp(node[NodeConstValueHeaderLen:NodeConstValueHeaderLen+1],
		[]byte{0xff}); c != 0 {
		t.Error("expected: ", []byte{0xff}, "not: ",
			node[NodeConstValueHeaderLen:NodeConstValueHeaderLen+1])
	}
	if pos != int(NodeConstValueHeaderLen)+1 {
		t.Error("expected: ", NodeConstValueHeaderLen+1, "not: ", pos)
	}
}

func TestNodeSetFieldWithEmptyBytes(t *testing.T) {
	node := make([]byte, NodeSize)
	pos := NodeSetField(node, int(NodeConstValueHeaderLen), []byte{}, 0, 0)
	if c := byteutil.ByteCmp(node[NodeConstValueHeaderLen:NodeConstValueHeaderLen],
		[]byte{}); c != 0 {
		t.Error("expected: ", []byte{}, "not: ",
			node[NodeConstValueHeaderLen:NodeConstValueHeaderLen])
	}
	if pos != int(NodeConstValueHeaderLen) {
		t.Error("expected: ", NodeConstValueHeaderLen, "not: ", pos)
	}
}

func TestNodeGetField(t *testing.T) {
	node := make([]byte, NodeSize)
	NodeSetField(node, int(NodeConstValueHeaderLen), []byte{0xff}, 0, 1)
	bs := NodeGetField(node, int(NodeConstValueHeaderLen), 1)
	if c := byteutil.ByteCmp(bs, []byte{0xff}); c != 0 {
		t.Error("expected: ", []byte{0xff}, "not: ", bs)
	}
}

func TestNodeGetFieldWithEmptyBytes(t *testing.T) {
	node := make([]byte, NodeSize)
	NodeSetField(node, int(NodeConstValueHeaderLen), []byte{0xff}, 0, 1)
	bs := NodeGetField(node, int(NodeConstValueHeaderLen), 0)
	if c := byteutil.ByteCmp(bs, []byte{}); c != 0 {
		t.Error("expected: ", []byte{}, "not: ", bs)
	}
}
