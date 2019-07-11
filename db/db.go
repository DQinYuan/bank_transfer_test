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
	*sql.DB
}

func NewDb(ipPort string, user string, passwd string, dbname string) (*DbCtl, error) {
	dsn := fmt.Sprintf("%s:%s@(%s)/%s?readTimeout=1s", user, passwd, ipPort, dbname)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbCtl := &DbCtl{dsn:dsn, DB:db}

	log.Printf("open %s db %s success\n", dsn, dbname)

	return dbCtl, nil
}

func (dc *DbCtl) Exec(sql string) error  {
	_, err := dc.DB.Exec(sql)
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
	rows, err := dc.DB.Query(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

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
	row := dc.DB.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s", tablename))

	var count int
	row.Scan(&count)

	return count
}

func (dc *DbCtl) Close()  {
	dc.DB.Close()
}
