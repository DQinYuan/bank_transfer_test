package main

import "testing"

func TestAction(t *testing.T) {
	bConfig = &bankConfig{
		passwd: "123456",
		concurrency: 1,
		dbname: "test",
		user: "root",
		numAccounts: 10,
		tableNum: 3,
		duration: "50s",
		interval: "5s",
	}

	action(nil, []string{"127.0.0.1:4406"})
}
