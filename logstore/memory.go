package logstore

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
)

type memStore struct {
	dbname string
	// after loaddate, it is read only
	tableMap map[string]*[]*int64
}

func (m *memStore) Open(dbname string, tableNum int) {
	m.dbname = dbname
	m.tableMap = make(map[string]*[]*int64)
	for i := 0; i < tableNum; i++ {
		tableName := fmt.Sprintf("accounts%d", i)
		rows := make([]*int64, 0)
		m.tableMap[tableName] = &rows
	}
}

func (m *memStore) InsertOrUpdate(tablename string, rowId int, account int) {
	accountInt64 := int64(account)

	tablerows := m.tableMap[tablename]

	*tablerows = append(*tablerows, &accountInt64)
}

func (m *memStore) GetBalance(tablename string, rowId int) int {
	return int(*(*m.tableMap[tablename])[rowId])
}

func (m *memStore) GetBalancePoint(tablename string, rowId int) *int64 {
	return (*m.tableMap[tablename])[rowId]
}

func (m *memStore) SafeIncrKeyPair(tablename string, rowId1 int, rowId2 int, change1 int, change2 int) {
	atomic.AddInt64(m.GetBalancePoint(tablename, rowId1), int64(change1))
	atomic.AddInt64(m.GetBalancePoint(tablename, rowId2), int64(change2))
}

func (m *memStore) Verify(tableName string, tableRows *sql.Rows) *VerifyInfo {
	trueTable := *m.tableMap[tableName]

	// verify content
	var id int
	var balance int64
	for tableRows.Next() {
		tableRows.Scan(&id, &balance)

		var expected int64
		if id < len(trueTable) {
			expected = *trueTable[id]
		} else {
			// db extra data
			return &VerifyInfo{TableName: tableName,
				RowId:    id,
				Expected: "none data",
				Real:     strconv.Itoa(int(balance))}
		}

		if balance != expected {
			// data inconsistent
			return &VerifyInfo{TableName: tableName,
				RowId:    id,
				Expected: strconv.Itoa(int(expected)),
				Real:     strconv.Itoa(int(balance))}
		}
	}

	// db loss data
	if id+1 != len(trueTable) {
		return &VerifyInfo{TableName: tableName,
			RowId:    id + 1,
			Expected: strconv.Itoa(int(*trueTable[id+1])),
			Real:     "none db loss data"}
	}

	return nil
}
