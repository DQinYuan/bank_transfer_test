package main

import (
	"sync"
	"testing"
)

var globalIndex int

func (w *worker) mockWork()  {
	for globalIndex = 0; globalIndex < 10000; globalIndex++ {
	}

	w.safePoint()

	for globalIndex = 10001; globalIndex < 20000; globalIndex++ {
	}
}

func TestSafePoint(t *testing.T) {
	stopSig := make(chan struct{})
	startSig := make(chan struct{})
	stopChs := []chan struct{}{stopSig}
	startChs := []chan struct{}{startSig}

	w := &worker{startSig:startSig, stopSig:stopSig}
	c := &controller{startSigs:startChs, stopSigs:stopChs}

	wg := &sync.WaitGroup{}
	wg.Add(1)


	go func() {
		defer wg.Done()
		w.mockWork()
	}()

	c.stopAll()
	if globalIndex != 10000 {
		t.Errorf("controllerr sync err %d\n", globalIndex)
	}
	c.startAll()

}
