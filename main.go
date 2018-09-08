package main

import (
	"fmt"
)

func main() {
	a := new([10]byte)
	b := a[:]
	fmt.Printf("%T %T\n", b, a)
}
