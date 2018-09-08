package blkio

import (
	"os"

	"github.com/erician/gpdDB/common/gpdconst"
)

//ReadBlk write a block
func ReadBlk(file *os.File, block []byte, blkNum int64) (err error) {
	_, err = file.ReadAt(block, blkNum*gpdconst.BlockSize)
	return
}
