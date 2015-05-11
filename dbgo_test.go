package dbgo

import (
	"fmt"
	"os"
	"testing"
)

var db *DB
var file = "/tmp/a.db"

func TestAll(t *testing.T) {
	var err error
	defer os.Remove(file)

	if db, err = DbOpen(file, "hash", "", 0); err != nil {
		t.Fatal(err)
	}

	e := &Db_entry{
		key:    "test",
		ts:     1431327783,
		offset: 1234567,
	}

	if err = db.Put(e); err != nil {
		t.Fatal(err)
	}

	e = &Db_entry{key: "test"}

	if err = db.Get(e); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(e.key, e.offset, e.ts)
	}

	e.offset = 222222
	if err = db.Put(e); err != nil {
		t.Fatal(err)
	}

	if err = db.Get(e); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(e.key, e.offset, e.ts)
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}
