package main

import (
	"fmt"
	"github.com/pingcap/qa/bank/db"
	"github.com/pingcap/qa/bank/logstore"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type bankCase struct {
	cfg   *bankConfig
	dbctl *db.DbCtl
	store logstore.Store
}

func (b *bankCase) loaddata() {
	log.Println("start load data..")

	wg := &sync.WaitGroup{}
	wg.Add(b.cfg.tableNum)

	for i := 0; i < b.cfg.tableNum; i++ {
		i := i
		go func() {
			defer wg.Done()
			index := strconv.Itoa(i)
			b.initTable(index)
			// verfy state and start verify gorotine
			b.startVerify(index)
		}()
	}

	wg.Wait()

}

type batchRange struct {
	start int
	size  int
}

// store state in local db
func (b *bankCase) syncState(tableName string, numAccounts int) {
	for i := 0; i < numAccounts; i++ {
		b.store.InsertOrUpdate(tableName, strconv.Itoa(i), "1000")
	}
}

func (b *bankCase) initTable(index string) {

	tableName := fmt.Sprintf("accounts%s", index)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// sync state in local db
	go func() {
		defer wg.Done()
		b.syncState(tableName, b.cfg.numAccounts)
	}()

	b.dropTable(index)

	b.dbctl.MustExec(fmt.Sprintf("create table if not exists %s "+
		"(id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)", tableName))

	batchSize := 100000
	jobCount, mod := b.cfg.numAccounts/batchSize, b.cfg.numAccounts%batchSize
	if mod != 0 {
		jobCount += 1
	}

	args := make([]string, 0, batchSize)
	for i := 0; i < jobCount; i++ {
		var br *batchRange
		if i == jobCount-1 && mod != 0 {
			br = &batchRange{i * batchSize, mod}
		} else {
			br = &batchRange{i * batchSize, batchSize}
		}

		start := time.Now()
		for i := 0; i < br.size; i++ {
			args = append(args, fmt.Sprintf("(%d, %d)", br.start + i, 1000))
		}
		query := fmt.Sprintf("INSERT IGNORE INTO %s (id, balance) VALUES %s",
			tableName, strings.Join(args, ","))
		insertF := func() error {
			err := b.dbctl.Exec(query)
			return err
		}
		// retry three times
		err := runWithRetry(3, insertF)
		if err != nil {
			log.Fatalf("exec %s err %s", query, err)
		}

		log.Printf("insert %d %s, takes %s", br.size, tableName, time.Now().Sub(start))
		args = args[:0]
	}

	wg.Wait()
}

func (b *bankCase) dropTable(index string) {
	b.dbctl.MustExec(fmt.Sprintf("drop table if exists accounts%s", index))
}

func (b *bankCase) startVerify(index string) {

}

func (b *bankCase) verifyAllState(index string) {

}