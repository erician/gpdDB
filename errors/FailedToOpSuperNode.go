package errors

//FailedToOpSuperNode just as the same
type FailedToOpSuperNode struct {
	s string
}

//NewErrFailedToOpSuperNode just as the name
func NewErrFailedToOpSuperNode(text string) error {
	return &FailedToOpSuperNode{text}
}

func (e *FailedToOpSuperNode) Error() string {
	return e.s
}
