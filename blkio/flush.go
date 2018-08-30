package blkio

import (
	"log"
	"os"
)

//FlushFile flush the modified data to storage
func FlushFile(file os.File) {
	err := file.Sync()
	if err != nil {
		log.Fatal("flushfile ", err)
	}
}
