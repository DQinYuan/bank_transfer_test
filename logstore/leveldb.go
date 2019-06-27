package logstore

import "database/sql"

type leveldbStore struct {
	dbname string
}

func (ls *leveldbStore) Open(dbname string) {
	ls.dbname = dbname
}

func (*leveldbStore) InsertOrUpdate(tablename string, rowId string, account string) {

}

func (*leveldbStore) GetBalance(tablename string, rowId string) {

}

func (*leveldbStore) SafeIncrKeyPair(tablename string, rowId1 int, rowId2 int, change1 int, change2 int)  {

}

func (*leveldbStore) Verify(tableName string, tableRows *sql.Rows) *VerifyInfo  {
	return nil
}

func NewStore(dbname string) Store {
	ls := &leveldbStore{}
	ls.Open(dbname)
	return ls
}
