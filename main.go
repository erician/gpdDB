package main

import (
	"fmt"
	"strconv"

	"github.com/erician/gpdDB/gpddb"
)

func main() {
	dbName := "aaa"
	db, err := gpddb.NewDb(dbName)
	if err != nil {
		gpddb.RemoveDb(dbName)
		db, _ = gpddb.NewDb(dbName)
	}

	keysNum := 100000
	key := "aaa"
	value := "bbb"
	for i := 0; i < keysNum; i++ {

		fmt.Println(i)

		if err := db.Put(key+strconv.Itoa(i), value+strconv.Itoa(i)); err != nil {
			fmt.Println("expect: ", nil, "not: ", err)
		}

	}
	db.Close()
	gpddb.RemoveDb(dbName)
}
