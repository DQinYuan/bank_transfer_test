package logstore

import (
	"database/sql"
	"fmt"
)

type VerifyInfo struct {
	TableName string
	LineNum   int
	Expected  int
	Real      int
}

func (v *VerifyInfo) String() string {
	return fmt.Sprintf("verify error table %s, linenum %d, expected balance %d, real balance %d",
		v.TableName, v.LineNum, v.Expected, v.Real)
}

type Store interface {
	Open(dbname string)
	InsertOrUpdate(tablename string, rowId string, account string)
	GetBalance(tablename string, rowId string)
	// change1 will apply to rowId1 and change2 will apply to rowId2
	SafeIncrKeyPair(tableName string, rowId1 int, rowId2 int, change1 int, change2 int)
	// verify a table
	Verify(tableName string, tableRows *sql.Rows) *VerifyInfo
}
