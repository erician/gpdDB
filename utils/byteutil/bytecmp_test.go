package byteutil

import (
	"testing"
)

func TestByteCmpWithSimpleExample(t *testing.T) {
	bs1 := []byte{0x11, 0x22, 0x33, 0x44}
	bs2 := []byte{0x11, 0x22, 0x33, 0x44}
	bs3 := []byte{0x11, 0x22, 0x33, 0x55}
	bs4 := []byte{0x11, 0x11, 0x33, 0x44}
	bs5 := []byte{0x00, 0x44}
	bs6 := []byte{}

	if c := ByteCmp(bs1, bs2); c != 0 {
		t.Error("expect: ", 0, "not: ", c)
	}
	if c := ByteCmp(bs1, bs3); c != -1 {
		t.Error("expect: ", -1, "not: ", c)
	}
	if c := ByteCmp(bs1, bs4); c != 1 {
		t.Error("expect: ", 1, "not: ", c)
	}
	if c := ByteCmp(bs1, bs5); c != 1 {
		t.Error("expect: ", 1, "not: ", c)
	}
	if c := ByteCmp(bs1, bs6); c != 1 {
		t.Error("expect: ", 1, "not: ", c)
	}
}
