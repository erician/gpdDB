package errors

type ErrMismathArg struct {
    s string
}
func NewErrMismathArg(text string) error {
    return &ErrMismathArg{text}
}
func (e *ErrMismathArg) Error() string {
    return e.s
}
