package blkio

import (
	"log"
	"os"

	"github.com/erician/gpdDB/common/gpdconst"
)

//ReadBlk write a block
func ReadBlk(file os.File, block []byte, blkNum int64) {
	_, err := file.ReadAt(block, blkNum*gpdconst.BlockSize)
	if err != nil {
		log.Fatal("writeblk ", err)
	}
}
