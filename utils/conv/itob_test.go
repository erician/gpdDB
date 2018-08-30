package conv

import (
	"testing"

	"github.com/erician/gpdDB/utils/byteutil"
)

func TestItobWithInt(t *testing.T) {
	var a int
	a = 1
	_, err := Itob(a)
	if err == nil {
		t.Errorf("type int cannot convert to byte")
	}
}

func TestItobWithSimpleExample(t *testing.T) {
	var u8 uint8 = 0x11
	u8Bs := []byte{0x11}
	var u16 uint16 = 0x1122
	u16Bs := []byte{0x22, 0x11}

	var s32 int32 = 0x11223344
	s32Bs := []byte{0x44, 0x33, 0x22, 0x11}
	s64 := int64(1)
	s64Bs := []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	//s64 := int64(0x1122334455667788)
	//s64Bs := []byte{0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11}
	if bytes, _ := Itob(u8); byteutil.ByteCmp(bytes, u8Bs) != 0 {
		t.Errorf("expect: %x not: %x", u8Bs, bytes)
	}
	if bytes, _ := Itob(u16); byteutil.ByteCmp(bytes, u16Bs) != 0 {
		t.Errorf("expect: %x not: %x", u16Bs, bytes)
	}
	if bytes, _ := Itob(s32); byteutil.ByteCmp(bytes, s32Bs) != 0 {
		t.Errorf("expect: %x not: %x", s32Bs, bytes)
	}
	if bytes, _ := Itob(s64); byteutil.ByteCmp(bytes, s64Bs) != 0 {
		t.Errorf("expect: %x not: %x", s64Bs, bytes)
	}
}

func TestItobUsingBtoi(t *testing.T) {
	var u8 uint8 = 0x11
	var s64 int64 = 0x1122334455667788

	bytes, _ := Itob(u8)
	if a, _ := Btoi(bytes); uint8(a) != u8 {
		t.Errorf("expect: %x not: %x", u8, a)
	}
	bytes, _ = Itob(s64)
	if a, _ := Btoi(bytes); int64(a) != s64 {
		t.Errorf("expect: %x not: %x", s64, a)
	}

}
