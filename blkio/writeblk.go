package blkio

import (
	"os"

	"github.com/erician/gpdDB/common/gpdconst"
)

//WriteBlk write a block
func WriteBlk(file *os.File, block []byte, blkNum int64) (err error) {
	_, err = file.WriteAt(block, blkNum*gpdconst.BlockSize)
	return
}
