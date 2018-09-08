package errors

//FailedToOpNode just as the same
type FailedToOpNode struct {
	s string
}

//NewErrFailedToOpNode just as the name
func NewErrFailedToOpNode(text string) error {
	return &FailedToOpNode{text}
}

func (e *FailedToOpNode) Error() string {
	return e.s
}
