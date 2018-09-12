package gpddb

import (
	"strconv"
	"testing"
)

func TestPutWithSimple(t *testing.T) {
	dbName := "aaa"
	db, _ := NewDb(dbName)

	key := "aaa"
	value := "bbb"
	if err := db.Put(key, value); err != nil {
		t.Error("expect: ", nil, "not: ", err)
	}
	tValue, err := db.Get(key)
	if err != nil {
		t.Error("expect: ", key+"'value", "not: ", err)
	}
	if tValue != value {
		t.Error("expect: ", value, "not: ", tValue)
	}
	db.Close()

	db, _ = OpenDb(dbName)
	tValue, err = db.Get(key)
	if err != nil {
		t.Error("expect: ", key+"'value", "not: ", err)
	}
	if tValue != value {
		t.Error("expect: ", value, "not: ", tValue)
	}
	db.Close()
	RemoveDb(dbName)
}

func TestPutWith1000Keys(t *testing.T) {
	dbName := "aaa"
	db, err := NewDb(dbName)
	if err != nil {
		RemoveDb(dbName)
		db, _ = NewDb(dbName)
	}

	keysNum := 500
	key := "aaa"
	value := "bbb"
	for i := 0; i < keysNum; i++ {
		if err := db.Put(key+strconv.Itoa(i), value+strconv.Itoa(i)); err != nil {
			t.Error("expect: ", nil, "not: ", err)
		}
	}
	db.Close()

	db, err = OpenDb(dbName)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < keysNum; i++ {
		tValue, err := db.Get(key + strconv.Itoa(i))
		if err != nil {
			t.Error("expect: ", key+strconv.Itoa(i)+"'value", "not: ", err)
		}
		if tValue != value+strconv.Itoa(i) {
			t.Error("expect: ", value+strconv.Itoa(i), "not: ", tValue)
		}
	}
	db.Close()
	RemoveDb(dbName)
}
