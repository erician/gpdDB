package errors

//ErrKeyNotExist just as the same
type ErrKeyNotExist struct {
	s string
}

//NewErrKeyNotExist just as the name
func NewErrKeyNotExist(text string) error {
	return &ErrKeyNotExist{text}
}

func (e *ErrKeyNotExist) Error() string {
	return e.s
}
