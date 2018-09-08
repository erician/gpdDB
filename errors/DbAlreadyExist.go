package errors

//ErrDbAlreadyExist just as the same
type ErrDbAlreadyExist struct {
	s string
}

//NewErrDbAlreadyExist just as the name
func NewErrDbAlreadyExist(text string) error {
	return &ErrDbAlreadyExist{text}
}

func (e *ErrDbAlreadyExist) Error() string {
	return e.s
}
