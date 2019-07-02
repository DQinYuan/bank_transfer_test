package logstore

import (
	"database/sql"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
)

// not completed do not use
type leveldbStore struct {
	dbname string
	db *leveldb.DB
}

func (ls *leveldbStore) Open(dbname string, tableNum int) {
	ls.dbname = dbname
	db, err := leveldb.OpenFile("testdb", nil)
	if err != nil {
		log.Fatalln("logstore open fail")
	}

	ls.db = db
}

func (*leveldbStore) InsertOrUpdate(tablename string, rowId int, account int) {
}

func (*leveldbStore) GetBalance(tablename string, rowId int) int {
	return 0
}

func (ls *leveldbStore) SafeIncrKeyPair(tablename string, rowId1 int, rowId2 int, change1 int, change2 int)  {

}

func (*leveldbStore) Verify(tableName string, tableRows *sql.Rows) *VerifyInfo  {
	return nil
}

func (*leveldbStore) Dump(filepath string) error {
	return nil
}


