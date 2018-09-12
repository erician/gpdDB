package errors

//CloseFailed just as the same
type CloseFailed struct {
	s string
}

//NewErrCloseFailed just as the name
func NewErrCloseFailed(text string) error {
	return &CloseFailed{text}
}

func (e *CloseFailed) Error() string {
	return e.s
}
