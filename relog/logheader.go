package relog

import (
	"fmt"
	"os"

	"github.com/erician/gpdDB/utils/conv"
)

//LogHeader the format of recovery log header,
//and it occupies the first block of recovery log file
type LogHeader struct {
	//most recent checkpoint pos(specisely, the pos added by its lenght),
	//can be used for whether need recovery or not
	//if the value of currentCheckPonitPos is the end of the recovery log file,
	//nothing needed to do, or recovery from the currentCheckPonitPos
	currentCheckponitPos int64
	headerLen            int16
	version              int8
	//the rest fileds are log records
}

//some fields' offset of log
const (
	LogOffCurrentCheckpointPos int64 = 0
	LogOffHeaderLen            int64 = 8
	LogOffVersion              int64 = 10
)

//field size of log
const (
	LogFieldSizeCurrentCheckpointPos int64 = 8
	LogFieldSizeHeaderLen            int64 = 2
	LogFieldSizeVersion              int64 = 1
)

//some fields' value of Log
const (
	LogConstValueVersion             int8  = 1
	LogConstValueHeaderLen           int16 = 0x10
	LogInitValueCurrentCheckpointPos int64 = 0 + int64(LogConstValueHeaderLen) + LogRecordConstValueCheckpointSize
)

//InitLogHeader init log file's header, NOT sync to nonvolatile device, such as disk, SSD, etc
func InitLogHeader(logFile *os.File) (err error) {
	if err = LogSetCurrentCheckpointPos(logFile, LogInitValueCurrentCheckpointPos); err != nil {
		return
	}
	if err = LogSetHeaderLen(logFile, LogConstValueHeaderLen); err != nil {
		return
	}
	if err = LogSetVersion(logFile, LogConstValueVersion); err != nil {
		return
	}
	return
}

//LogSetField set field of log
func LogSetField(logFile *os.File, data interface{}, offser int64) error {
	bs, err := conv.Itob(data)
	if err != nil {
		return fmt.Errorf("set field of log, %v", err)
	}
	_, err = logFile.WriteAt(bs, offser)
	if err != nil {
		return fmt.Errorf("set field of log, %v", err)
	}
	return nil
}

//LogGetField get a field of logfile
func LogGetField(logFile *os.File, offset int64, len int64) (data int64, err error) {
	bs := make([]byte, len)
	_, err = logFile.ReadAt(bs, offset)
	if err != nil {
		return data, fmt.Errorf("get field of log, %v", err)
	}
	data, err = conv.Btoi(bs)
	if err != nil {
		return data, fmt.Errorf("get field of log, %v", err)
	}
	return
}

//LogSetCurrentCheckpointPos set currentCheckPonitPos
func LogSetCurrentCheckpointPos(logFile *os.File, checkpointPos int64) error {
	return LogSetField(logFile, checkpointPos, LogOffCurrentCheckpointPos)
}

//LogGetCurrentCheckpointPos get currentCheckPonitPos
func LogGetCurrentCheckpointPos(logFile *os.File) (pos int64, err error) {
	pos, err = LogGetField(logFile, LogOffCurrentCheckpointPos, LogFieldSizeCurrentCheckpointPos)
	if err != nil {
		return pos, fmt.Errorf("get currentCheckPonitPos of log, %v", err)
	}
	return
}

//LogSetHeaderLen set logfile's headerLen
func LogSetHeaderLen(logFile *os.File, headerLen int16) error {
	return LogSetField(logFile, headerLen, LogOffHeaderLen)
}

//LogGetHeaderLen get headerLen
func LogGetHeaderLen(logFile *os.File) (pos int16, err error) {
	data, err := LogGetField(logFile, LogOffHeaderLen, LogFieldSizeHeaderLen)
	if err != nil {
		return pos, fmt.Errorf("get headerLen of log, %v", err)
	}
	return int16(data), err
}

//LogSetVersion set logfile's version
func LogSetVersion(logFile *os.File, version int8) error {
	return LogSetField(logFile, version, LogOffVersion)
}

//LogGetVersion get version
func LogGetVersion(logFile *os.File) (pos int8, err error) {
	data, err := LogGetField(logFile, LogOffVersion, LogFieldSizeVersion)
	if err != nil {
		return pos, fmt.Errorf("get version of log, %v", err)
	}
	return int8(data), err
}

//DisplayLogHeader as name
func DisplayLogHeader(file *os.File) {
	currentCheckPonitPosBs := make([]byte, 8)
	file.ReadAt(currentCheckPonitPosBs, LogOffCurrentCheckpointPos)
	currentCheckPonitPos, _ := conv.Btoi(currentCheckPonitPosBs)

	headerLenBs := make([]byte, 2)
	file.ReadAt(headerLenBs, LogOffHeaderLen)
	headerLen, _ := conv.Btoi(headerLenBs)

	versionBs := make([]byte, 1)
	file.ReadAt(versionBs, LogOffVersion)
	version, _ := conv.Btoi(versionBs)

	fmt.Println(currentCheckPonitPos, "\t", headerLen, "\t", version, "\t")
}
