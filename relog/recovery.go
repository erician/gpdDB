package relog

import "fmt"

func (relog *RecoveryLog) doesNeedRecovery() (doesNeed bool, err error) {
	logLen, err := relog.logFile.Seek(0, 2)
	if err != nil {
		return doesNeed, fmt.Errorf("does need recover, %v", err)
	}
	currentCheckpointPos, err := LogGetCurrentCheckpointPos(relog.logFile)
	if err != nil {
		return doesNeed, fmt.Errorf("does need recover, %v", err)
	}
	doesNeed = logLen != currentCheckpointPos
	return
}

func (relog *RecoveryLog) recover() (err error) {
	checkPointPos, err := LogGetCurrentCheckpointPos(relog.logFile)

}
