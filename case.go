package main

import (
	"context"
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
			// verfy state
			b.verifyAllState(index)
		}()
	}

	wg.Wait()
}

/*func (b *bankCase) startVerify()  {

}*/

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
			args = append(args, fmt.Sprintf("(%d, %d)", br.start+i, 1000))
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

type verifyInfo struct {
	tablename string
	linenum int
	expected int
	real int
}

func (v *verifyInfo) String() string {
	return fmt.Sprintf("verify error table %s, linenum %d, expected balance %d, real balance %d",
		v.tablename, v.linenum, v.expected, v.real)
}

func (b *bankCase) startVerify(index string) {
	b.verifyState(index)

}

func (b *bankCase) verifyAllState() *verifyInfo {
	return nil
}

func (b *bankCase) verifyState(index string) *verifyInfo {
	return nil
}

// d is transfer duration
func (b *bankCase) transfer(duration time.Duration, interval time.Duration) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// start workers
	stopChs := make([]chan struct{}, 0, b.cfg.concurrency)
	startChs := make([]chan struct{}, 0, b.cfg.concurrency)
	for i := 0; i < b.cfg.concurrency; i++ {
		stopCh := make(chan struct{})
		stopChs = append(stopChs, stopCh)
		startCh := make(chan struct{})
		startChs = append(startChs, startCh)
		w := &worker{stopSig:stopCh, startSig:startCh}
		go w.work(timeoutCtx, b)
	}

	ctl := &controller{startSigs:startChs, stopSigs:stopChs}
	ticker := time.NewTicker(interval)

	for {
		<- ticker.C
		log.Println("stopping the world")
		//stop the world
		ctl.stopAll()
		log.Println("stop the world success")

		if info := b.verifyAllState(); info != nil {
			log.Println(info)
			break
		}

		log.Println("restarting the world")
		ctl.startAll()
		log.Println("restart the world success")
	}

}
