package relog

import (
	"fmt"
	"os"

	"github.com/erician/gpdDB/common/gpdconst"
	"github.com/erician/gpdDB/utils/byteutil"
	"github.com/erician/gpdDB/utils/conv"
)

//LogRecord the log record layout from the view of the caller,
//so the real format is not that, and please read the readme
type LogRecord interface {
	ToBytes(lsn int64) (bs []byte)
	SetChan(re chan struct{})
	GetChan() chan struct{}
}

//LogRecordUserOp logrecord about user's operations
type LogRecordUserOp struct {
	Operation int8 //Oparation defined in gpdconst
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

//LogRecordCheckpoint logrecord of checkpoint
type LogRecordCheckpoint struct {
	Operation int8
	re        chan struct{}
}

//some const values of LogRecord
const (
	LogRecordConstValueCheckpointSize int64 = 9
)

//LogRecordAllocate logrecord of checkpoint
type LogRecordAllocate struct {
	Operation int8
	BlkNum    int64
	re        chan struct{}
}

//LogRecordSetfield logrecord of set field
type LogRecordSetfield struct {
	Operation  int8
	BlkNum     int64
	Pos        int16
	FieldValue string
	re         chan struct{}
}

//ToBytes conv logrecord to bytes
func (lr *LogRecordUserOp) ToBytes(lsn int64) (bs []byte) {
	tmp, _ := conv.Itob(lsn)
	bs = byteutil.ByteCat(bs, tmp)
	tmp, _ = conv.Itob(lr.Operation)
	bs = byteutil.ByteCat(bs, tmp)
	tmp, _ = conv.Itob(lr.BlkNum)
	bs = byteutil.ByteCat(bs, tmp)

	tmp, _ = conv.Itob(int16(len(lr.Key)))
	bs = byteutil.ByteCat(bs, tmp)
	bs = byteutil.ByteCat(bs, []byte(lr.Key))

	tmp, _ = conv.Itob(int16(len(lr.Value)))
	bs = byteutil.ByteCat(bs, tmp)
	bs = byteutil.ByteCat(bs, []byte(lr.Value))
	return
}

//SetChan implement interface
func (lr *LogRecordUserOp) SetChan(re chan struct{}) {
	lr.re = re
}

//GetChan implement interface
func (lr *LogRecordUserOp) GetChan() chan struct{} {
	return lr.re
}

//NewLogRecordGPD create a log record of ALLOCATE
func NewLogRecordGPD(op int8, blkNum int64, key string, value string) (lr *LogRecordUserOp) {
	lr = new(LogRecordUserOp)
	lr.Operation = op
	lr.BlkNum = blkNum
	lr.Key = key
	lr.Value = value
	return
}

//NewLogRecordUserOpGet won't be used
func NewLogRecordUserOpGet(blkNum int64, key string, value string) (lr *LogRecordUserOp) {
	return NewLogRecordGPD(gpdconst.GET, blkNum, key, value)
}

//NewLogRecordUserOpPut create a log record of put
func NewLogRecordUserOpPut(blkNum int64, key string, value string) (lr *LogRecordUserOp) {
	return NewLogRecordGPD(gpdconst.PUT, blkNum, key, value)
}

//NewLogRecordUserOpDelete create a log record of delete
func NewLogRecordUserOpDelete(blkNum int64, key string, value string) (lr *LogRecordUserOp) {
	return NewLogRecordGPD(gpdconst.DELETE, blkNum, key, value)
}

//NewLogRecordCheckpoint create a log record of checkpoint
func NewLogRecordCheckpoint() (lr *LogRecordCheckpoint) {
	lr = new(LogRecordCheckpoint)
	lr.Operation = gpdconst.CHECKPOINT
	return
}

//ToBytes implement interface
func (lr *LogRecordCheckpoint) ToBytes(lsn int64) (bs []byte) {
	tmp, _ := conv.Itob(lsn)
	bs = byteutil.ByteCat(bs, tmp)
	tmp, _ = conv.Itob(lr.Operation)
	bs = byteutil.ByteCat(bs, tmp)
	return
}

//SetChan implement interface
func (lr *LogRecordCheckpoint) SetChan(re chan struct{}) {
	lr.re = re
}

//GetChan implement interface
func (lr *LogRecordCheckpoint) GetChan() chan struct{} {
	return lr.re
}

//NewLogRecordAllocate create a log record of ALLOCATE
func NewLogRecordAllocate(blkNum int64) (lr *LogRecordAllocate) {
	lr = new(LogRecordAllocate)
	lr.Operation = gpdconst.ALLOCATE
	lr.BlkNum = blkNum
	return
}

//ToBytes implement interface
func (lr *LogRecordAllocate) ToBytes(lsn int64) (bs []byte) {
	tmp, _ := conv.Itob(lsn)
	bs = byteutil.ByteCat(bs, tmp)
	tmp, _ = conv.Itob(lr.Operation)
	bs = byteutil.ByteCat(bs, tmp)
	tmp, _ = conv.Itob(lr.BlkNum)
	bs = byteutil.ByteCat(bs, tmp)
	return
}

//SetChan implement interface
func (lr *LogRecordAllocate) SetChan(re chan struct{}) {
	lr.re = re
}

//GetChan implement interface
func (lr *LogRecordAllocate) GetChan() chan struct{} {
	return lr.re
}

//NewLogRecordSetField create a log record of SETFIELD
func NewLogRecordSetField(blkNum int64, pos int16, fieldValue string) (lr *LogRecordSetfield) {
	lr = new(LogRecordSetfield)
	lr.Operation = gpdconst.SETFIELD
	lr.BlkNum = blkNum
	lr.Pos = pos
	lr.FieldValue = fieldValue
	return
}

//ToBytes implement interface
func (lr *LogRecordSetfield) ToBytes(lsn int64) (bs []byte) {
	tmp, _ := conv.Itob(lsn)
	bs = byteutil.ByteCat(bs, tmp)
	tmp, _ = conv.Itob(lr.Operation)
	bs = byteutil.ByteCat(bs, tmp)
	tmp, _ = conv.Itob(lr.BlkNum)
	bs = byteutil.ByteCat(bs, tmp)
	tmp, _ = conv.Itob(lr.Pos)
	bs = byteutil.ByteCat(bs, tmp)

	tmp, _ = conv.Itob(int16(len(lr.FieldValue)))
	bs = byteutil.ByteCat(bs, tmp)
	bs = byteutil.ByteCat(bs, []byte(lr.FieldValue))
	return
}

//SetChan implement interface
func (lr *LogRecordSetfield) SetChan(re chan struct{}) {
	lr.re = re
}

//GetChan implement interface
func (lr *LogRecordSetfield) GetChan() chan struct{} {
	return lr.re
}

//DisplayLogRecordCheckpoint
func DisplayLogRecordCheckpoint(file *os.File, lsnBs []byte, pos int64) int64 {
	fmt.Println(string(lsnBs) + "\t" + gpdconst.OperationEnum[gpdconst.CHECKPOINT].Name)
	return pos
}
