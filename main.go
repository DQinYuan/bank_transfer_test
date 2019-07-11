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
const defaultDump = "dump.log"
const defaultRecordFile = "bank.log"

type bankConfig struct {
	passwd      string
	concurrency int
	dbname      string
	user        string
	numAccounts int
	tableNum    int
	duration    string
	interval    string
	dump        string
	recordFile  string
}

var bConfig = &bankConfig{}

func main() {

	rootCmd := &cobra.Command{
		Use:   "bank db_ip:db_port",
		Short: "benchmark tool to mock transfer scene in bank",
		Args:  cobra.MinimumNArgs(1),
		Run:   action,
	}

	rootCmd.Flags().StringVarP(&bConfig.passwd, "passwd", "P", defaultPasswd, "db password");
	rootCmd.Flags().IntVarP(&bConfig.concurrency, "concurrency", "C", defaultConcurrency, "concurrency for account transfer, default is 2 * NumCPU")
	rootCmd.Flags().StringVarP(&bConfig.dbname, "dbname", "B", defaultDbname, "db to operate")
	rootCmd.Flags().StringVarP(&bConfig.user, "user", "U", defaultUser, "user to log in tidb")
	rootCmd.Flags().IntVarP(&bConfig.numAccounts, "accounts-num", "A", defaultNumAccounts, "accounts num of one table, the balance of one account is 1000")
	rootCmd.Flags().IntVarP(&bConfig.tableNum, "table-num", "T", defaultTableNum, "number of tables")
	rootCmd.Flags().StringVarP(&bConfig.duration, "duration-time", "D", defaultDuration, "the duration time of benchmark, except load data time")
	rootCmd.Flags().StringVarP(&bConfig.interval, "interval", "I", defaultInterval, "interval for STW")
	rootCmd.Flags().StringVar(&bConfig.dump, "dump", defaultDump, "dump path when verify fail")
	rootCmd.Flags().StringVarP(&bConfig.recordFile, "record-file", "R", defaultRecordFile, "file to record transfer, csv format")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
		os.Exit(1)
	}
}

func action(cmd *cobra.Command, args []string) {
	dbCtl, err := db.NewDb(args[0], bConfig.user, bConfig.passwd, bConfig.dbname)
	if err != nil {
		log.Fatalf("new db fail %+v", err)
	}

	rFile, err := os.OpenFile(bConfig.recordFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("create transfer record file %s error", bConfig.recordFile)
	}

	bCase := &bankCase{cfg: bConfig,
		dbctl: dbCtl,
		store: logstore.NewStore(bConfig.dbname, bConfig.tableNum),
	    recordFile:rFile}
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
}
