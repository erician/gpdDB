package conv

import (
	"testing"

	"github.com/erician/gpdDB/utils/byteutil"
)

func TestBtoiWithMismatchLength(t *testing.T) {
	bs := []byte{11, 22, 22, 22, 22, 22, 22, 22, 22}
	_, err := Btoi(bs)
	if err == nil {
		t.Error(err)
	}
}

func TestBtoiWithSimpleExample(t *testing.T) {
	var u8 uint8 = 0x11
	u8Bs := []byte{0x11}
	var u16 uint16 = 0x1122
	u16Bs := []byte{0x22, 0x11}

	var s32 int32 = 0x11223344
	s32Bs := []byte{0x44, 0x33, 0x22, 0x11}
	var s64 int64 = 0x1122334455667788
	s64Bs := []byte{0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11}
	if i, _ := Btoi(u8Bs); uint8(i) != u8 {
		t.Errorf("expect: %x not: %x", u8, i)
	}
	if i, _ := Btoi(u16Bs); uint16(i) != u16 {
		t.Errorf("expect: %x not: %x", u16, i)
	}
	if i, _ := Btoi(s32Bs); int32(i) != s32 {
		t.Errorf("expect: %x not: %x", s32, i)
	}
	if i, _ := Btoi(s64Bs); int64(i) != s64 {
		t.Errorf("expect: %x not: %x", s64, i)
	}
}

func TestBtoiUsingItob(t *testing.T) {
	u8Bs := []byte{0x11}
	s64Bs := []byte{0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11}

	i, _ := Btoi(u8Bs)
	if a, _ := Itob(i); byteutil.ByteCmp(a, u8Bs) != 0 {
		t.Errorf("expect: %x not: %x", u8Bs, a)
	}
	i, _ = Btoi(s64Bs)
	a, _ := Itob(i)
	if byteutil.ByteCmp(a, s64Bs) != 0 {
		t.Errorf("expect: %x not: %x", s64Bs, a)
	}
}
