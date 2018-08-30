package readwrite

import (
	"fmt"
)

//WriteByte is to copy the bytes from src to des
func WriteByte(des []byte, desStart int, src []byte, srcStart int, writeLen int) (err error) {
	if desStart+writeLen > len(des) {
		return fmt.Errorf("des out of range, desStart: %d, writeLen: %d, len(des): %d",
			desStart, writeLen, len(des))
	}
	if srcStart+writeLen > len(src) {
		return fmt.Errorf("src out of range, srcStart: %d, writeLen: %d, len(src): %d",
			srcStart, writeLen, len(src))
	}
	for i := 0; i < writeLen; i++ {
		des[i+desStart] = src[i+srcStart]
	}
	return
}
