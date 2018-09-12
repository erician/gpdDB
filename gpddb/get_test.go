package gpddb

import "testing"

func TestGetWithSimple(t *testing.T) {
	dbName := "aaa"
	db, _ := NewDb(dbName)

	key := "aaa"
	if _, err := db.Get(key); err == nil {
		t.Error("expect: ", key+" not exist", "not: ", err)
	}
	db.Close()
	RemoveDb(dbName)
}
