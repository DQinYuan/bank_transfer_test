package logstore

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

func NewStore(dbname string) Store {
	ls := &leveldbStore{}
	ls.Open(dbname)
	return ls
}
