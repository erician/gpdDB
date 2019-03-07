package cache

import (
	"testing"
)

func TestNewEnt(t *testing.T) {
	ent := NewEnt()
	if ent.Next != nil {
		t.Error("expected: ", nil, "not: ", ent.Next)
	}

}
