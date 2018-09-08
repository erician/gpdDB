package blkio

import (
	"os"
)

//SyncFile sync the modified data to storage
func SyncFile(file *os.File) (err error) {
	err = file.Sync()
	return
}
