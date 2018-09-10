package errors

//PutFailed just as the same
type PutFailed struct {
	s string
}

//NewErrPutFailed just as the name
func NewErrPutFailed(text string) error {
	return &PutFailed{text}
}

func (e *PutFailed) Error() string {
	return e.s
}
