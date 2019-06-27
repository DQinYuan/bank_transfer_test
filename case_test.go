package main

import (
	"database/sql"
	. "github.com/pingcap/check"
	"github.com/pingcap/qa/bank/db"
	"github.com/pingcap/qa/bank/logstore"
	"log"
	"testing"
)

var _ = Suite(&testBankCaseSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type mockStore struct {
}

func (*mockStore) Open(dbname string) {

}

func (*mockStore) InsertOrUpdate(tablename string, rowId string, account string) {

}

func (*mockStore) GetBalance(tablename string, rowId string) {

}

func (*mockStore) SafeIncrKeyPair(tablename string, rowId1 int, rowId2 int, change1 int, change2 int)()  {

}

func (*mockStore) Verify(tableName string, tableRows *sql.Rows) *logstore.VerifyInfo  {

	log.Printf("%s verifying data\n", tableName)

	for tableRows.Next() {
		var id int
		var balance int
		tableRows.Scan(&id, &balance)
		if balance != 1000 {
			return &logstore.VerifyInfo{tableName, id, 1000, balance}
		}
	}

	return nil
}

type testBankCaseSuite struct {
	bc *bankCase
}

const testNumAccounts = 10
const testTableNum = 3

func (s *testBankCaseSuite) SetUpSuite(c *C) {
	bankConfig := &bankConfig{
		passwd:      "123456",
		dbname:      "test",
		user:        "root",
		numAccounts: testNumAccounts,
		tableNum:    testTableNum,
	}

	dbCtl, err := db.NewDb("127.0.0.1:3306", bankConfig.user, bankConfig.passwd, bankConfig.dbname)
	if err != nil {
		log.Fatalf("new db fail %+v", err)
	}

	s.bc = &bankCase{bankConfig, dbCtl, &mockStore{}}
}

func (s *testBankCaseSuite) TearDownSuite(c *C) {
	s.bc.dbctl.Close()
}

func (s *testBankCaseSuite) TestInitTable(c *C) {
	s.bc.initTable("1")

}

func (s *testBankCaseSuite) TestLoaddata(c *C)  {
	s.bc.loaddata()
}

func (s *testBankCaseSuite) TestExecTransaction(c *C)  {
	s.bc.loaddata()
	s.bc.execTransaction(3, 4, 600, "1")
}

func (s *testBankCaseSuite) TestVerifyAllState(c *C)  {
	s.bc.loaddata()
	s.bc.verifyAllState()
}
