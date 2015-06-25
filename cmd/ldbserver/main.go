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

	var (
		mf     ldbserver.MarshalingType
		config *Config
	)
	{
		arg_db := flag.String("db", "", "path to database")
		arg_net := flag.String("net", "unix", "network type (http,tcp,unix)")
		arg_host := flag.String("host", "/tmp/ldbserver.sock", "network host")
		arg_form := flag.String("form", "json", "format of marshaling (json,protobuf)")
		arg_usage := flag.Bool("h", false, "print usage")
		arg_config := flag.String("config", "", "json config (skips other flags)")

		flag.Parse()

		if *arg_usage {
			flag.PrintDefaults()
			return
		}

		if len(*arg_config) == 0 {

			config = &Config{
				Db:     *arg_db,
				Host:   *arg_host,
				Net:    *arg_net,
				Format: *arg_form,
			}
		} else {
			config = LoadConfig(*arg_config)
		}

	}

	if config == nil {
		logger.Fatal("no config for run server")
	}

	if len(config.Db) == 0 {
		logger.Fatal("-db must be a valid path")
	}

	switch config.Format {
	case "json":
		mf = ldbserver.MarshalingTypeJson
	case "protobuf":
		mf = ldbserver.MarshalingTypeProtobuf
	default:
		logger.Fatal("-form must be 'json' or 'protobuf'")
	}

	logger.Info("---START---")

	db, err := ldbserver.NewDbServer(config.Db)
	if err != nil {
		logger.FatalErr(err)
	}
	defer db.Close()
	ns := ldbserver.NewNetworkServer(config.Net, config.Host)

	if config.Net == "unix" {
		defer os.Remove(config.Host)
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
