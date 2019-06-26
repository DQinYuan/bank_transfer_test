package main

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/qa/bank/db"
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
