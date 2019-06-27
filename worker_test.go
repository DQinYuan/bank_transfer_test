package main

import (
	"context"
	. "github.com/pingcap/check"
	"sync"
)

var _ = Suite(&testWorkerSuite{})

type testWorkerSuite struct {
	globalIndex int
}

func (w *worker) mockWork(suite *testWorkerSuite) {
	for suite.globalIndex = 0; suite.globalIndex < 10000; suite.globalIndex++ {
	}

	w.safePoint()

	for suite.globalIndex = 10001; suite.globalIndex < 20000; suite.globalIndex++ {
	}
}

func (suite *testWorkerSuite) TestSafePoint(c *C) {
	stopSig := make(chan struct{})
	startSig := make(chan struct{})
	stopChs := []chan struct{}{stopSig}
	startChs := []chan struct{}{startSig}

	w := &worker{startSig: startSig, stopSig: stopSig}
	con := &controller{startSigs: startChs, stopSigs: stopChs}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		w.mockWork(suite)
	}()

	ctx := context.Background()

	con.stopAll(ctx)
	c.Assert(suite.globalIndex, Equals, 10000)
	con.startAll(ctx)

	wg.Wait()

	c.Assert(suite.globalIndex, Equals, 20000)
}
