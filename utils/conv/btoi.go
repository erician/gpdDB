package conv

import (
	"fmt"
)

//Btoi convert []byte to int
//NOTE we use LittleEndian
func Btoi(bs []byte) (data int64, err error) {
	if bs == nil {
		return 0, nil
	}
	if len(bs) > 8 {
		return data, fmt.Errorf("the length of bs must be <= 8, not %d", len(bs))
	}
	for i := len(bs) - 1; i >= 0; i-- {
		data = (data << 8) | int64(bs[i])
	}
	return data, nil
}
