package readwrite

import "testing"
import "github.com/erician/gpdDB/utils/byteutil"

func TestWriteByteWithSimpleExample(t *testing.T) {
	des := make([]byte, 10)
	src := make([]byte, 10)
	for i := 0; i < 10; i++ {
		src[i] = uint8(i)
	}
	WriteByte(des, 0, src, 0, 10)
	if c := byteutil.ByteCmp(des, src); c != 0 {
		t.Errorf("des is not the same with src, des: %x, src: %x", des, src)
	}
}

func TestWriteByteWithOutOfRange(t *testing.T) {
	des := make([]byte, 10)
	src := make([]byte, 11)
	for i := 0; i < 10; i++ {
		src[i] = uint8(i)
	}
	err := WriteByte(des, 0, src, 0, 11)
	if err == nil {
		t.Error("expected not nil")
	}
}
