package conv

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

//Itob convert INTEGER to []byte
//NOTE: the data must has the specify length, looks like uint8 etc.
func Itob(data interface{}) ([]byte, error) {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, data)
	if err != nil {
		return nil, fmt.Errorf("itob: %v", err)
	}
	return buff.Bytes(), nil
}
