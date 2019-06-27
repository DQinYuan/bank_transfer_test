package main

import (
	"context"
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

			w.safePoint()
		}
	}

}

// stop world safe point
func (w *worker) safePoint() {
	select {
	case <- w.stopSig:
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


