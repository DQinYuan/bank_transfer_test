package logstore

import (
	"database/sql"
	. "github.com/pingcap/check"
	_ "github.com/proullon/ramsql/driver"
	"sync"
	"testing"
)

var _ = Suite(&testMemSuite{})
var _ = Suite(&testMemRamSqlSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testMemSuite struct {
}

func (*testMemSuite) TestMemStore(c *C) {
	tables := []string{"accounts0", "accounts1"}

	m := &memStore{}
	m.Open("test", len(tables))

	wg := &sync.WaitGroup{}
	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()

			for j := 0; j < 10000; j++ {
				m.InsertOrUpdate(tables[i], j, 1000)
			}
		}()
	}

	wg.Wait()

	c.Assert(len(*m.tableMap[tables[0]]), Equals, 10000)
	c.Assert(len(*m.tableMap[tables[1]]), Equals, 10000)
	c.Assert(m.GetBalance(tables[0], 10), Equals, 1000)

	wg = &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		for j := 0; j < 1000; j++ {
			m.SafeIncrKeyPair(tables[0], 1, 10, -1, 1)
		}
	}()

	go func() {
		defer wg.Done()
		for j := 0; j < 1300; j++ {
			m.SafeIncrKeyPair(tables[0], 1, 10, 1, -1)
		}
	}()

	wg.Wait()

	c.Assert(m.GetBalance(tables[0], 1), Equals, 1300)
	c.Assert(m.GetBalance(tables[0], 10), Equals, 700)
}

type testMemRamSqlSuite struct {
	db *sql.DB
}

func (s *testMemRamSqlSuite) SetUpSuite(c *C) {
	batch := []string{
		`CREATE TABLE accounts0 (id BIGINT(20) PRIMARY KEY, balance BIGINT(20) NOT NULL);`,
		`INSERT INTO accounts0 (id, balance) VALUES (0, 10);`,
		`INSERT INTO accounts0 (id, balance) VALUES (1, 10);`,
		`INSERT INTO accounts0 (id, balance) VALUES (2, 10);`,
	}

	db, err := sql.Open("ramsql", "TestLoadUserAddresses")
	c.Assert(err, IsNil)

	for _, b := range batch {
		_, err = db.Exec(b)
		c.Assert(err, IsNil)
	}
	s.db = db
}

func (s *testMemRamSqlSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testMemRamSqlSuite) TestMemStore_Verify(c *C) {

	rows, err := s.db.Query("select * from accounts0")
	c.Assert(err, IsNil)

	tables := []string{"accounts0"}

	m := &memStore{}
	m.Open("test", len(tables))

	for j := 0; j < 3; j++ {
		m.InsertOrUpdate(tables[0], j, 10)
	}

	vInfo := m.Verify(tables[0], rows)
	c.Assert(vInfo, IsNil)
}

func (s *testMemRamSqlSuite) TestMemStore_Verify2(c *C) {
	rows, err := s.db.Query("select * from accounts0")
	c.Assert(err, IsNil)

	tables := []string{"accounts0"}

	m := &memStore{}
	m.Open("test", len(tables))

	for j := 0; j < 4; j++ {
		m.InsertOrUpdate(tables[0], j, 10)
	}

	vInfo := m.Verify(tables[0], rows)
	c.Assert(vInfo.Real, Equals, "none db loss data")
}

func (s *testMemRamSqlSuite) TestMemStore_Verify3(c *C) {
	rows, err := s.db.Query("select * from accounts0")
	c.Assert(err, IsNil)

	tables := []string{"accounts0"}

	m := &memStore{}
	m.Open("test", len(tables))

	for j := 0; j < 2; j++ {
		m.InsertOrUpdate(tables[0], j, 10)
	}

	vInfo := m.Verify(tables[0], rows)
	c.Assert(vInfo.Expected, Equals, "none data")
}

func (s *testMemRamSqlSuite) TestMemStore_Verify4(c *C) {
	rows, err := s.db.Query("select * from accounts0")
	c.Assert(err, IsNil)

	tables := []string{"accounts0"}

	m := &memStore{}
	m.Open("test", len(tables))

	for j := 0; j < 2; j++ {
		if j == 1 {
			m.InsertOrUpdate(tables[0], j, 12)
			continue
		}
		m.InsertOrUpdate(tables[0], j, 10)
	}

	vInfo := m.Verify(tables[0], rows)
	c.Assert(vInfo.Expected, Equals, "12")
	c.Assert(vInfo.Real, Equals, "10")
	c.Assert(vInfo.RowId, Equals, 1)
}
