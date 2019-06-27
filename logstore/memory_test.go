package logstore

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	_ "github.com/proullon/ramsql/driver"
)

func TestMemStore(t *testing.T) {
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

	fmt.Println(len(*m.tableMap[tables[0]]))
	fmt.Println(len(*m.tableMap[tables[1]]))
	fmt.Println(m.GetBalance(tables[0], 10))

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

	fmt.Println(m.GetBalance(tables[0], 1))
	fmt.Println(m.GetBalance(tables[0], 10))
}

func TestMemStore_Verify(t *testing.T) {
	batch := []string{
		`CREATE TABLE accounts0 (id BIGINT(20) PRIMARY KEY, balance BIGINT(20) NOT NULL);`,
		`INSERT INTO accounts0 (id, balance) VALUES (0, 10);`,
		`INSERT INTO accounts0 (id, balance) VALUES (1, 10);`,
		`INSERT INTO accounts0 (id, balance) VALUES (2, 10);`,
	}

	db, err := sql.Open("ramsql", "TestLoadUserAddresses")
	if err != nil {
		t.Fatalf("sql.Open : Error : %v\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	rows, err := db.Query("select * from accounts0")
	if err != nil {
		t.Fatalf("query err %v\n", err)
	}

	tables := []string{"accounts0"}

	m := &memStore{}
	m.Open("test", len(tables))

	for j := 0; j < 3; j++ {
		m.InsertOrUpdate(tables[0], j, 10)
	}

	vInfo := m.Verify(tables[0], rows)
	if vInfo != nil {
		t.Errorf("vInfo should be <nil>, but real is '%v'\n", vInfo)
	}
}

func TestMemStore_Verify2(t *testing.T) {
	batch := []string{
		`CREATE TABLE accounts0 (id BIGINT(20) PRIMARY KEY, balance BIGINT(20) NOT NULL);`,
		`INSERT INTO accounts0 (id, balance) VALUES (0, 10);`,
		`INSERT INTO accounts0 (id, balance) VALUES (1, 10);`,
		`INSERT INTO accounts0 (id, balance) VALUES (2, 10);`,
	}

	db, err := sql.Open("ramsql", "TestLoadUserAddresses")
	if err != nil {
		t.Fatalf("sql.Open : Error : %v\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	rows, err := db.Query("select * from accounts0")
	if err != nil {
		t.Fatalf("query err %v\n", err)
	}

	tables := []string{"accounts0"}

	m := &memStore{}
	m.Open("test", len(tables))

	for j := 0; j < 4; j++ {
		m.InsertOrUpdate(tables[0], j, 10)
	}

	vInfo := m.Verify(tables[0], rows)
	if vInfo.Real != "none db loss data" {
		t.Errorf("vInfo.Real should be 'none db loss data', but real is '%v'\n", vInfo)
	}
}

func TestMemStore_Verify3(t *testing.T) {
	batch := []string{
		`CREATE TABLE accounts0 (id BIGINT(20) PRIMARY KEY, balance BIGINT(20) NOT NULL);`,
		`INSERT INTO accounts0 (id, balance) VALUES (0, 10);`,
		`INSERT INTO accounts0 (id, balance) VALUES (1, 10);`,
		`INSERT INTO accounts0 (id, balance) VALUES (2, 10);`,
	}

	db, err := sql.Open("ramsql", "TestLoadUserAddresses")
	if err != nil {
		t.Fatalf("sql.Open : Error : %v\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	rows, err := db.Query("select * from accounts0")
	if err != nil {
		t.Fatalf("query err %v\n", err)
	}

	tables := []string{"accounts0"}

	m := &memStore{}
	m.Open("test", len(tables))

	for j := 0; j < 2; j++ {
		m.InsertOrUpdate(tables[0], j, 10)
	}

	vInfo := m.Verify(tables[0], rows)
	if vInfo.Expected != "none data" {
		t.Errorf("vInfo.Expected should be 'none data', but real is '%v'\n", vInfo)
	}
}

func TestMemStore_Verify4(t *testing.T) {
	batch := []string{
		`CREATE TABLE accounts0 (id BIGINT(20) PRIMARY KEY, balance BIGINT(20) NOT NULL);`,
		`INSERT INTO accounts0 (id, balance) VALUES (0, 10);`,
		`INSERT INTO accounts0 (id, balance) VALUES (1, 12);`,
		`INSERT INTO accounts0 (id, balance) VALUES (2, 10);`,
	}

	db, err := sql.Open("ramsql", "TestLoadUserAddresses")
	if err != nil {
		t.Fatalf("sql.Open : Error : %v\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	rows, err := db.Query("select * from accounts0")
	if err != nil {
		t.Fatalf("query err %v\n", err)
	}

	tables := []string{"accounts0"}

	m := &memStore{}
	m.Open("test", len(tables))

	for j := 0; j < 2; j++ {
		m.InsertOrUpdate(tables[0], j, 10)
	}

	vInfo := m.Verify(tables[0], rows)
	if vInfo.Real != "12" {
		t.Errorf("vInfo.Real should be '12', but real is '%v'\n", vInfo)
	}
}