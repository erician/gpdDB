package errors

//ErrDbNotExist just as the same
type ErrDbNotExist struct {
	s string
}

//NewErrDbNotExist just as the name
func NewErrDbNotExist(text string) error {
	return &ErrDbNotExist{text}
}

func (e *ErrDbNotExist) Error() string {
	return e.s
}
