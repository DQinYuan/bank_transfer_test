package main

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
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
			if info := b.verifyState(index); info != nil {
				log.Fatalf("load data not consistent %v", info)
			}
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
		b.store.InsertOrUpdate(tableName, i, 1000)
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

// verifyAllState will verify one table per goroutine
// if there are verify errors, it will return one randomly
func (b *bankCase) verifyAllState() *logstore.VerifyInfo {
	wg := &sync.WaitGroup{}
	wg.Add(b.cfg.tableNum)

	infoCh := make(chan *logstore.VerifyInfo)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		close(infoCh)
	}()

	for i := 0; i < b.cfg.tableNum; i++ {
		go func(index string, ctx context.Context) {
			defer wg.Done()
			info := b.verifyState(index)
			if info != nil {
				select {
				case infoCh <- info:
				case <-ctx.Done():
				}
			}
		}(strconv.Itoa(i), ctx)
	}

	completeCh := make(chan struct{})
	go func() {
		wg.Wait()
		completeCh <- struct{}{}
	}()

	select {
	case <-completeCh:
		return  nil
	case vInfo := <-infoCh:
		return vInfo
	}

	return nil
}

func (b *bankCase) verifyState(index string) *logstore.VerifyInfo {

	tableName := fmt.Sprintf("accounts%s", index)

	rows, err := b.dbctl.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName))
	if err != nil {
		if strings.Contains(err.Error(), "bad connection") || strings.Contains(err.Error(), "connection refused") {
			log.Println("connection fail")
			return nil
		}
		log.Fatalf("query table %s fail %+v\n", tableName, err)
	}

	return b.store.Verify(tableName, rows)
}

// concurrency transfer with stw(implement by worker-controller pattern)
// duration is transfer duration and  interval is stw interval
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
		w := &worker{stopSig: stopCh, startSig: startCh}
		go w.work(timeoutCtx, b)
	}

	ctl := &controller{startSigs: startChs, stopSigs: stopChs}
	ticker := time.NewTicker(interval)

	stw:
	for {
		select {
		case <-timeoutCtx.Done():
			break stw
		case <-ticker.C:
		}

		log.Println("stopping the world")
		// stop the world
		ctl.stopAll(timeoutCtx)
		log.Println("stop the world success")

		select {
		case <-timeoutCtx.Done():
			break stw
		default:
			if info := b.verifyAllState(); info != nil {
				// exit immediately if verify error
				log.Fatalln(info)
				break stw
			}
		}


		log.Println("restarting the world")
		ctl.startAll(timeoutCtx)
		log.Println("restart the world success")
	}

}

func (b *bankCase) execTransaction(from, to int, amount int, index string) (txerr error) {
	// start transaction
	tx, err := b.dbctl.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	tableName := fmt.Sprintf("accounts%s", index)

	// select for update
	rows, err := tx.Query(fmt.Sprintf("SELECT id, balance FROM "+
		"%s WHERE id IN (%d, %d) FOR UPDATE", tableName, from, to))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	var (
		fromBalance int
		toBalance   int
		count       int = 0
	)

	for rows.Next() {
		var id, balance int
		if err = rows.Scan(&id, &balance); err != nil {
			return errors.Trace(err)
		}
		switch id {
		case from:
			fromBalance = balance
		case to:
			toBalance = balance
		default:
			log.Fatalf("got unexpected account %d", id)
		}

		count += 1
	}

	if err = rows.Err(); err != nil {
		return errors.Trace(err)
	}

	if count != 2 {
		log.Fatalf("select %d(%d) -> %d(%d) invalid count %d", from, fromBalance, to, toBalance, count)
	}

	update := fmt.Sprintf(`
UPDATE %s
  SET balance = CASE id WHEN %d THEN %d WHEN %d THEN %d END
  WHERE id IN (%d, %d)
`, tableName, to, toBalance+amount, from, fromBalance-amount, from, to)
	_, err = tx.Exec(update)
	if err != nil {
		return errors.Trace(err)
	}

	defer func() {
		if txerr != nil {
			log.Printf("transaction commit error table name %s from %d to %d, %v",
				tableName, from, to, txerr)
			tx.Rollback()
		} else {
			log.Printf("transaction commit success table name %s from %d to %d",
				tableName, from, to)
			// update local state
			b.store.SafeIncrKeyPair(tableName, from, to, -amount, amount)
		}
	}()

	txerr = errors.Trace(tx.Commit())

	return
}
