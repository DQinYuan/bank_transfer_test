package logstore

import (
	"database/sql"
	"fmt"
)

type VerifyInfo struct {
	TableName string
	RowId   int
	Expected  string
	Real      string
}

func (v *VerifyInfo) String() string {
	return fmt.Sprintf("verify error table %s, rowId %d, expected balance %s, real balance %s",
		v.TableName, v.RowId, v.Expected, v.Real)
}

type Store interface {
	Open(dbname string, tableNum int)
	InsertOrUpdate(tablename string, rowId int, account int)
	GetBalance(tablename string, rowId int) int
	// change1 will apply to rowId1 and change2 will apply to rowId2
	SafeIncrKeyPair(tableName string, rowId1 int, rowId2 int, change1 int, change2 int)
	// verify a table
	Verify(tableName string, tableRows *sql.Rows) *VerifyInfo
	Dump(filepath string) error
}

func NewStore(dbname string, tableNum int) Store {
	store := &memStore{}
	store.Open(dbname, tableNum)
	return store
}
