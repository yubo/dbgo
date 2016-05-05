package dbgo

/*
#include <stdlib.h>
#include "dbgo.h"
#cgo CFLAGS: -D__DBINTERFACE_PRIVATE
*/
import "C"

import (
	"fmt"
	"unsafe"
)

const (
	RET_ERROR       = -1 /* Return values. */
	RET_SUCCESS     = 0
	RET_SPECIAL     = 1
	MAX_PAGE_NUMBER = 0xffffffff /* >= # of pages in a file */
	MAX_PAGE_OFFSET = 65535      /* >= # of bytes in a page */
	MAX_REC_NUMBER  = 0xffffffff /* >= # of records in a tree */
	R_CURSOR        = 1          /* del, put, seq */
	__R_UNUSED      = 2          /* UNUSED */
	R_FIRST         = 3          /* seq */
	R_IAFTER        = 4          /* put (RECNO) */
	R_IBEFORE       = 5          /* put (RECNO) */
	R_LAST          = 6          /* seq (BTREE, RECNO) */
	R_NEXT          = 7          /* seq */
	R_NOOVERWRITE   = 8          /* put */
	R_PREV          = 9          /* seq (BTREE, RECNO) */
	R_SETCURSOR     = 10         /* put (RECNO) */
	R_RECNOSYNC     = 11         /* sync (RECNO) */
	DB_BTREE        = 0
	DB_HASH         = 1
	DB_RECNO        = 2
	DB_LOCK         = 0x20000000 /* Do locking. */
	DB_SHMEM        = 0x40000000 /* Use shared memory. */
	DB_TXN          = 0x80000000 /* Do transactions. */
	BTREEMAGIC      = 0x053162
	BTREEVERSION    = 3
	HASHMAGIC       = 0x061561
	HASHVERSION     = 2
	R_FIXEDLEN      = 0x01 /* fixed-length records */
	R_NOKEY         = 0x02 /* key not required */
	R_SNAPSHOT      = 0x04 /* snapshot the input */
)

type Db struct {
	db     unsafe.Pointer
	dbname string
	typ    string
	info   string
	lock   int
}

type Db_entry struct {
	Key    string
	Ts     C.time_t
	Offset C.off_t
}

func Open(dbname, typ, info string, lock int) (*Db, error) {
	var null unsafe.Pointer
	d := &Db{
		dbname: dbname,
		typ:    typ,
		info:   info,
		lock:   lock,
	}

	_dbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(_dbname))
	_typ := C.CString(typ)
	defer C.free(unsafe.Pointer(_typ))
	_info := C.CString(info)
	defer C.free(unsafe.Pointer(_info))

	d.db = unsafe.Pointer(C.db_open(_dbname, _typ, _info, C.int(d.lock)))

	if d.db == null {
		return nil, fmt.Errorf("db_open error")
	}

	return d, nil
}

func (d *Db) String() string {
	return fmt.Sprintf("db[0x%08x] dbname[%s] type[%s] info[%s] lock[%d]",
		d.db, d.dbname, d.typ, d.info, d.lock)
}

func (e *Db_entry) String() string {
	return fmt.Sprintf("key %s ts %d offset %d",
		e.Key, int64(e.Ts), int64(e.Offset))
}

func (d *Db) Close() error {
	ret := C.db_close(d.db)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_close error")
}

func (d *Db) Get(entry *Db_entry) error {
	_key := C.CString(entry.Key)
	defer C.free(unsafe.Pointer(_key))

	ret := C.db_get(d.db, _key, &entry.Ts, &entry.Offset, 0)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_get error")
}

func (d *Db) Put(entry *Db_entry) error {
	_key := C.CString(entry.Key)
	defer C.free(unsafe.Pointer(_key))

	ret := C.db_put(d.db, _key, entry.Ts, entry.Offset, R_NOOVERWRITE)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_put error")
}

func (d *Db) Update(entry *Db_entry) error {
	_key := C.CString(entry.Key)
	defer C.free(unsafe.Pointer(_key))

	ret := C.db_put(d.db, _key, entry.Ts, entry.Offset, 0)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_put error")
}

func (d *Db) Delete(entry *Db_entry) error {
	_key := C.CString(entry.Key)
	defer C.free(unsafe.Pointer(_key))

	ret := C.db_delete(d.db, _key, 0)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_delete error")
}
