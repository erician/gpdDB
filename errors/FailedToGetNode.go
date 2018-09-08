package errors

//FailedToGetNode just as the same
type FailedToGetNode struct {
	s string
}

//NewErrFailedToGetNode just as the name
func NewErrFailedToGetNode(text string) error {
	return &FailedToGetNode{text}
}

func (e *FailedToGetNode) Error() string {
	return e.s
}
