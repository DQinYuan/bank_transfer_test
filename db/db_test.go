package db

import (
	"fmt"
	"testing"
)

func TestDbCtl_MustCount(t *testing.T) {

	ctl, err := NewDb("127.0.0.1:3306", "root", "123456", "test")
	if err != nil {
		t.Errorf("open db err %v\n", err)
	}

	count := ctl.MustCount("accounts0")
	fmt.Println(count)
}