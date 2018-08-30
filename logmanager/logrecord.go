package logmanager

import (
	"fmt"

	"github.com/erician/gpdDB/common/gpdconst"
	"github.com/erician/gpdDB/utils/byteutil"
	"github.com/erician/gpdDB/utils/conv"
)

//LogRecord the log record layout from the view of the caller,
//so the real format is not that, and please read the readme
type LogRecord struct {
	Oparation int8 //Oparation defined in gpdconst
	BlkNum    int64
	Key       string
	Value     string
	//when a caller call the WriteLog function, it
	//won't return immediately. Insteadly, waiting for
	//the logrecord to be really write to logfile, such
	//as the finishment of fsync. So the field re
	//is deciding when to return.
	re chan struct{}
}

//ToBytes concert LogRecord to bytes
func (lr *LogRecord) ToBytes(lsn int64) (bs []byte, err error) {
	var valuesNeededConv []interface{}
	valuesNeededConv = append(valuesNeededConv, lsn)
	valuesNeededConv = append(valuesNeededConv, lr.Oparation)
	if lr.Oparation != gpdconst.CHECKPOINT {
		valuesNeededConv = append(valuesNeededConv, lr.BlkNum)
	}
	if lr.Oparation != gpdconst.ALLOCATE && lr.Oparation != gpdconst.CHECKPOINT {
		valuesNeededConv = append(valuesNeededConv, int16(len(lr.Key)))
		valuesNeededConv = append(valuesNeededConv, int16(len(lr.Value)))
	}
	for i, value := range valuesNeededConv {
		t, err := conv.Itob(value)
		if err != nil {
			return nil, fmt.Errorf("logrecord to bytes, %v", err)
		}
		bs = byteutil.ByteCat(bs, t)

		if i == 3 {
			bs = byteutil.ByteCat(bs, []byte(string(lr.Key)))
		}
		if i == 4 {
			bs = byteutil.ByteCat(bs, []byte(string(lr.Value)))
		}
	}
	return
}
