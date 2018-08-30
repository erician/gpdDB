package blkio

import (
	"log"
	"os"

	"github.com/erician/gpdDB/common/gpdconst"
)

//WriteBlk write a block
func WriteBlk(file os.File, block []byte, blkNum int64) {
	_, err := file.WriteAt(block, blkNum*gpdconst.BlockSize)
	if err != nil {
		log.Fatal("writeblk ", err)
	}
}
