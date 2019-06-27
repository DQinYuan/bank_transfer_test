package logstore

import (
	"github.com/syndtr/goleveldb/leveldb"
	"testing"
)

func TestLeveldb(t *testing.T) {
	db, err := leveldb.OpenFile("testdb", nil)
	if err != nil {
		t.Errorf("%v", err)
	}

	db.Put([]byte("ssss"), []byte("pppp"), nil)

	value, _ := db.Get([]byte("ssss"), nil)
	if string(value) != "pppp" {
		t.Errorf("key not expected %s\n", string(value))
	}
}
