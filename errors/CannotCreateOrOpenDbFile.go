package errors

//ErrCannotCreateOrOpenDbFile just as the same
type ErrCannotCreateOrOpenDbFile struct {
	s string
}

//NewErrCannotCreateOrOpenDbFile just as the name
func NewErrCannotCreateOrOpenDbFile(text string) error {
	return &ErrDbAlreadyExist{text}
}

func (e *ErrCannotCreateOrOpenDbFile) Error() string {
	return e.s
}
