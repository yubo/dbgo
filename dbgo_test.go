package dbgo

import (
	"fmt"
	"os"
	"testing"
)

var db *Db
var file = "/tmp/a.db"

func TestAll(t *testing.T) {
	var err error
	defer os.Remove(file)

	if db, err = Open(file, "hash", "", 0); err != nil {
		t.Fatal(err)
	}

	e := &Db_entry{
		Key:    "test",
		Ts:     1431327783,
		Offset: 1234567,
	}

	if err = db.Put(e); err != nil {
		t.Fatal(err)
	}

	e = &Db_entry{Key: "test"}

	if err = db.Get(e); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(e.Key, e.Offset, e.Ts)
	}

	e.Offset = 222222
	if err = db.Update(e); err != nil {
		t.Fatal(err)
	}

	if err = db.Put(e); err == nil {
		t.Fatalf("Put success to exist key")
	}

	if err = db.Get(e); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(e.Key, e.Offset, e.Ts)
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}
