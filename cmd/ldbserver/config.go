package main

import (
	"encoding/json"
	"os"

	"github.com/govlas/logger"
)

type Config struct {
	Db     string
	Host   string
	Net    string
	Format string
}

func LoadConfig(fname string) (ret *Config) {
	if file, err := os.Open(fname); !logger.ErrorErr(err) {
		defer file.Close()
		ret = new(Config)
		dec := json.NewDecoder(file)
		if logger.ErrorErr(dec.Decode(ret)) {
			return nil
		}

	}
	return
}
