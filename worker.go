package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
)

/*
worker:
select
1. timeout
2. checkpoint
3.
*/

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

			bc.execTransaction(from, to, amount, strconv.Itoa(id))

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

func (c *controller) stopAll()  {
	wg := &sync.WaitGroup{}
	wg.Add(len(c.stopSigs))
	for _, s := range c.stopSigs {
		s := s
		go func() {
			defer wg.Done()
			s <-struct {}{}
		}()
	}

	wg.Wait()
}

func (c *controller) startAll() {
	wg := &sync.WaitGroup{}
	wg.Add(len(c.stopSigs))
	for _, s := range c.startSigs {
		s := s
		go func() {
			defer wg.Done()
			s <-struct {}{}
		}()
	}

	wg.Wait()
}


