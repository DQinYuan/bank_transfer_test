package main

import (
	"database/sql"
	. "github.com/pingcap/check"
	"github.com/pingcap/qa/bank/db"
	"github.com/pingcap/qa/bank/logstore"
	"log"
	"os"
	"strconv"
	"testing"
)

var _ = Suite(&testBankCaseSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type mockStore struct {
}

func (*mockStore) Open(dbname string, tableNum int) {

}

func (*mockStore) InsertOrUpdate(tablename string, rowId int, account int) {

}

func (*mockStore) GetBalance(tablename string, rowId int) int {
	return 0
}

func (*mockStore) SafeIncrKeyPair(tableName string, rowId1 int, rowId2 int, change1 int, change2 int) () {

}

func (*mockStore) Dump(filePath string) error {
	return nil
}

func (*mockStore) Verify(tableName string, tableRows *sql.Rows) *logstore.VerifyInfo {

	log.Printf("%s verifying data\n", tableName)

	for tableRows.Next() {
		var id int
		var balance int
		tableRows.Scan(&id, &balance)
		if balance != 1000 {
			return &logstore.VerifyInfo{tableName, id, "1000",
				strconv.Itoa(balance)}
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
		recordFile:  "bank.log",
	}

	dbCtl, err := db.NewDb("127.0.0.1:3306", bankConfig.user, bankConfig.passwd, bankConfig.dbname)
	if err != nil {
		log.Fatalf("new db fail %+v", err)
	}

	rFile, err := os.OpenFile(bankConfig.recordFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("create transfer record file %s error, %v", bankConfig.recordFile, err)
	}
	s.bc = &bankCase{bankConfig, dbCtl, &mockStore{}, rFile}
}

func (s *testBankCaseSuite) TearDownSuite(c *C) {
	s.bc.dbctl.Close()
}

func (s *testBankCaseSuite) TestInitTable(c *C) {
	s.bc.initTable("1")

}

func (s *testBankCaseSuite) TestLoaddata(c *C) {
	s.bc.loaddata()
}

func (s *testBankCaseSuite) TestExecTransaction(c *C) {
	s.bc.loaddata()
	s.bc.execTransaction(3, 4, 600, "1", 10)
}

func (s *testBankCaseSuite) TestVerifyAllState(c *C) {
	s.bc.loaddata()
	s.bc.verifyAllState()
}
