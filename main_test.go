package main

import "testing"

func TestAction(t *testing.T) {
	t.SkipNow()
	bConfig = &bankConfig{
		passwd: "123456",
		concurrency: 15,
		dbname: "test",
		user: "root",
		numAccounts: 10,
		tableNum: 3,
		duration: "50s",
		interval: "2s",
	}

	action(nil, []string{"127.0.0.1:4406"})
}
