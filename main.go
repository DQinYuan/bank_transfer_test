package main

import (
	"fmt"
	"github.com/pingcap/qa/bank/db"
	"github.com/pingcap/qa/bank/logstore"
	"github.com/spf13/cobra"
	"log"
	"os"
	"runtime"
	"time"
)

const defaultPasswd = ""

var defaultConcurrency = 2 * runtime.NumCPU()

const defaultUser = "root"
const defaultDbname = "test"
const defaultNumAccounts = 1000
const defaultTableNum = 1
const defaultDuration = "30s"
const defaultInterval = "10s"


type bankConfig struct {
	passwd      string
	concurrency int
	dbname      string
	user        string
	numAccounts int
	tableNum    int
	duration    string
	interval    string
}

func main() {
	bankConfig := &bankConfig{}
	rootCmd := &cobra.Command{
		Use:   "bank db_ip:db_port",
		Short: "benchmark tool to mock transfer scene in bank",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			dbCtl, err := db.NewDb(args[0], bankConfig.user, bankConfig.passwd, bankConfig.dbname)
			if err != nil {
				log.Fatalf("new db fail %+v", err)
			}
			bCase := &bankCase{bankConfig,
				dbCtl,
				logstore.NewStore(bankConfig.dbname)}
			bCase.loaddata()

			duration, err := time.ParseDuration(bCase.cfg.duration)
			if err != nil {
				log.Fatalln("duration param format error, for example 10s, 20m, 2h, 1h10m")
			}
			interval, err := time.ParseDuration(bCase.cfg.interval)
			if err != nil {
				log.Fatalln("interval param format error, for example 10s, 20m, 2h, 1h10m")
			}
			// start bank transfer
			bCase.transfer(duration, interval)
		},
	}

	rootCmd.Flags().StringVarP(&bankConfig.passwd, "passwd", "P", defaultPasswd, "db password");
	rootCmd.Flags().IntVarP(&bankConfig.concurrency, "concurrency", "C", defaultConcurrency, "concurrency for account transfer, default is 2 * NumCPU")
	rootCmd.Flags().StringVarP(&bankConfig.dbname, "dbname", "DB", defaultDbname, "db to operate")
	rootCmd.Flags().StringVarP(&bankConfig.user, "user", "U", defaultUser, "user to log in tidb")
	rootCmd.Flags().IntVarP(&bankConfig.numAccounts, "accounts-num", "A", defaultNumAccounts, "accounts num of one table, the balance of one account is 1000")
	rootCmd.Flags().IntVarP(&bankConfig.tableNum, "table-num", "T", defaultTableNum, "number of tables")
	rootCmd.Flags().StringVarP(&bankConfig.duration, "duration-time", "DT", defaultDuration, "the duration time of benchmark")
	rootCmd.Flags().StringVarP(&bankConfig.interval, "interval", "I", defaultInterval, "interval for STW")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
		os.Exit(1)
	}
}
