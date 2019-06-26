package main

import (
	"github.com/pingcap/errors"
)

func runWithRetry(retryCnt int, f func() error) (err error) {
	for i := 0; i < retryCnt; i++ {
		err = f()
		if err == nil {
			return nil
		}
	}
	return errors.Trace(err)
}
