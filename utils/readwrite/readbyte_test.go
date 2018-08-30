package readwrite

import (
	"testing"

	"github.com/erician/gpdDB/utils/byteutil"
)

func TestReadByteWithSimpleExample(t *testing.T) {
	src := make([]byte, 10)
	for i := 0; i < 10; i++ {
		src[i] = uint8(i)
	}
	des, _ := ReadByte(src, 0, 10)
	if c := byteutil.ByteCmp(des, src); c != 0 {
		t.Errorf("des is not the same with src, des: %x, src: %x", des, src)
	}
}

func TestReadByteWithOutOfRange(t *testing.T) {
	src := make([]byte, 10)
	for i := 0; i < 10; i++ {
		src[i] = uint8(i)
	}
	_, err := ReadByte(src, 0, 11)
	if err == nil {
		t.Error("expected not nil")
	}
}
