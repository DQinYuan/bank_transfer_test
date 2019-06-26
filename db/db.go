package db

import (
	"database/sql"
	"fmt"
	"github.com/pingcap/errors"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

type DbCtl struct {
	dsn string
	db  *sql.DB
}

func NewDb(ipPort string, user string, passwd string, dbname string) (*DbCtl, error) {
	dsn := fmt.Sprintf("%s:%s@(%s)/%s", user, passwd, ipPort, dbname)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbCtl := &DbCtl{dsn:dsn, db:db}

	log.Printf("open %s db %s success\n", dsn, dbname)

	return dbCtl, nil
}

func (dc *DbCtl) Exec(sql string) error  {
	_, err := dc.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (dc *DbCtl) MustExec(sql string)  {
	err := dc.Exec(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func (dc *DbCtl) Query(sql string) (*sql.Rows, error) {
	rows, err := dc.db.Query(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

/*	balances := make([]int, 0)
	for rows.Next() {
		var id int
		var balance int
		rows.Scan(&id, &balance)
		balances = append(balances, balance)
	}*/

	return rows, nil
}

func (dc *DbCtl) MustQuery(sql string) *sql.Rows  {
	res, err := dc.Query(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	return res
}

func (dc *DbCtl) MustCount(tablename string) int {
	row := dc.db.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s", tablename))

	var count int
	row.Scan(&count)

	return count
}

func (dc *DbCtl) Close()  {
	dc.db.Close()
}