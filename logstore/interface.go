package logstore


type Store interface {
	Open(dbname string)
	InsertOrUpdate(tablename string, rowId string, account string)
	GetBalance(tablename string, rowId string)
}
