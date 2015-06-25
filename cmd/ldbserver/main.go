package main

import (
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/govlas/ldbserver"
	"github.com/govlas/logger"
)

func main() {
	logger.EnableColored()
	logger.SetFileName(logger.FileNameShort)

	arg_db := flag.String("db", "", "path to database")
	arg_net := flag.String("net", "unix", "network type (http,tcp,unix)")
	arg_host := flag.String("host", "/tmp/ldbserver.sock", "network host")
	arg_form := flag.String("form", "json", "format of marshaling (json,protobuf)")
	arg_usage := flag.Bool("h", false, "print usage")

	flag.Parse()

	if *arg_usage {
		flag.PrintDefaults()
		return
	}

	if len(*arg_db) == 0 {
		logger.Fatal("-db must be a valid path")
	}

	var mf ldbserver.MarshalingType
	switch *arg_form {
	case "json":
		mf = ldbserver.MarshalingTypeJson
	case "protobuf":
		mf = ldbserver.MarshalingTypeProtobuf
	default:
		logger.Fatal("-form must be 'json' or 'protobuf'")
	}

	logger.Info("---START---")

	db, err := ldbserver.NewDbServer(*arg_db)
	if err != nil {
		logger.FatalErr(err)
	}
	defer db.Close()
	ns := ldbserver.NewNetworkServer(*arg_net, *arg_host)

	if *arg_net == "unix" {
		defer os.Remove(*arg_host)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ns.ListenAndServe(db, mf)
		if err != nil && err != ldbserver.ErrStopped {
			logger.WarningErr(err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)

	<-c
	ns.Stop()
	wg.Wait()
	logger.Info("normal exit")
}
