package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
)

/*
worker:
select
1. timeout
2. checkpoint
3.
*/

var txnCounter int64 = -1

type worker struct {
	stopSig  chan struct{}
	startSig chan struct{}
}

func (w *worker) work(context context.Context, bc *bankCase) {
	for {
		select {
		case <-context.Done():
			// timout
			break
		default:
			// normal transfer task

			// random select table and transfer account
			from, to, id := rand.Intn(bc.cfg.numAccounts), rand.Intn(bc.cfg.numAccounts), rand.Intn(bc.cfg.tableNum)
			if from == to {
				continue
			}

			// random transfer account
			amount := rand.Intn(999)

			// get transaction id
			txnId := atomic.AddInt64(&txnCounter, 1)

			bc.execTransaction(from, to, amount, strconv.Itoa(id), txnId)

			// listen safe point application
			w.safePoint()
		}
	}

}

// stop world safe point
func (w *worker) safePoint() {
	select {
	case <- w.stopSig:
		log.Println("enter safePoint")
		<- w.startSig
	default:
	}
}

type controller struct {
	stopSigs  []chan struct{}
	startSigs []chan struct{}
}

func (c *controller) stopAll(context context.Context)  {
	wg := &sync.WaitGroup{}
	wg.Add(len(c.stopSigs))
	for _, s := range c.stopSigs {
		s := s
		go func() {
			defer wg.Done()
			select {
			case s <-struct {}{}:
			case <-context.Done():
			}

		}()
	}

	wg.Wait()
}

func (c *controller) startAll(context context.Context) {
	wg := &sync.WaitGroup{}
	wg.Add(len(c.stopSigs))
	for _, s := range c.startSigs {
		s := s
		go func() {
			defer wg.Done()
			select {
			case s <-struct {}{}:
			case <-context.Done():
			}
		}()
	}

	wg.Wait()
}


