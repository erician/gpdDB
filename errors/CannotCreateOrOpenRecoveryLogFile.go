package errors

//ErrCannotCreateOrOpenRecoveryLogFile just as the same
type ErrCannotCreateOrOpenRecoveryLogFile struct {
	s string
}

//NewErrCannotCreateOrOpenRecoveryLogFile just as the name
func NewErrCannotCreateOrOpenRecoveryLogFile(text string) error {
	return &ErrDbAlreadyExist{text}
}

func (e *ErrCannotCreateOrOpenRecoveryLogFile) Error() string {
	return e.s
}
