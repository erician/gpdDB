package errors

//GetFailed just as the same
type GetFailed struct {
	s string
}

//NewErrGetFailed just as the name
func NewErrGetFailed(text string) error {
	return &GetFailed{text}
}

func (e *GetFailed) Error() string {
	return e.s
}
