package trans

import (
	"github.com/erician/gpdDB/relog"
)

type Trans struct {
	ID             int64
	logReocrdSlice []relog.LogRecord
}

/*
Start() error
Commit() error
Rollback() error
*/
