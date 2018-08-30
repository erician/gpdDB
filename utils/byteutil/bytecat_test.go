package byteutil

import (
	"testing"
)

func TestByteCat(t *testing.T) {
	bs1 := []byte{0x11, 0x22}
	bs2 := []byte{0x33, 0x44}
	bs3 := []byte{0x11, 0x22, 0x33, 0x44}
	bs4 := []byte{0x11, 0x22, 0x61, 0x62}
	str := "ab"
	result := ByteCat(bs1, bs2)
	if c := ByteCmp(result, bs3); c != 0 {
		t.Error("expect: ", bs3, "not: ", result)
	}

	result = ByteCat(bs1, []byte(str))
	if c := ByteCmp(result, bs4); c != 0 {
		t.Error("expect: ", bs4, "not: ", result)
	}

}
