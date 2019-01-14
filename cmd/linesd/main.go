package main

import (
	"flag"
	"fmt"
	"github.com/korovkin/linesd"
	"io"
	"log"
	"os"
)

var (
	config_filename = flag.String(
		"config",
		"config.json",
		"config file")

	version = flag.Bool(
		"version",
		false,
		"show long version string")

	address = flag.String(
		"address",
		":9400",
		"HTTP server address")

	env = flag.String(
		"env",
		"dev",
		"prod / dev")

	conc_limit = flag.Int(
		"conc_limit",
		20,
		"concurrency limit for processing the lines")

	is_show_lines = flag.Bool(
		"is_show_lines",
		false,
		"log lines to stdout",
	)

	is_show_batches = flag.Bool(
		"is_show_batches",
		false,
		"print the uploaded batches")

	is_overload_log = flag.Bool(
		"is_overload_log",
		true,
		"overload 'log' printers")

	batch_size_seconds = flag.Int(
		"batch_size_seconds",
		180,
		"batch size in seconds",
	)

	batch_size_lines = flag.Int(
		"batch_size_lines",
		5000,
		"batch size in lines",
	)

	progress = flag.Int(
		"progress",
		100,
		"progress debug prints",
	)

	timeout_seconds = flag.Int(
		"timeout_seconds",
		30,
		"upload timeout in seconds")

	log_folder = flag.String(
		"log_folder",
		"",
		"destination for local rotated logs")

	log_file_size_bytes = flag.Int(
		"log_file_size_bytes",
		100*1024,
		"rotated log files size in bytes")
)

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile | log.Lmicroseconds | log.Ldate)

	if *version {
		fmt.Println(linesd.VERSION_NUMBER)
		return
	}

	// create a config file
	config := &linesd.Config{}

	// read the config file:
	linesd.ReadJsonFile(*config_filename, config)

	// overwrite some parts:
	config.Env = *env
	config.Address = *address
	config.Progress = *progress
	config.IsShowLines = *is_show_lines
	config.IsShowBatches = *is_show_batches
	config.BatchSizeInSeconds = *batch_size_seconds
	config.BatchSizeInLines = *batch_size_lines
	config.ConcLimit = *conc_limit
	config.TimeoutSeconds = *timeout_seconds
	config.LogFilesFileSizeBytes = *log_file_size_bytes
	config.LogFilesFolder = *log_folder

	// create a linesd instance:
	s := &linesd.Server{}

	// load the config:
	s.Initialize(config)

	if *is_overload_log {
		log.SetOutput(io.MultiWriter(os.Stderr, s))
	}

	s.RunForever()
}
