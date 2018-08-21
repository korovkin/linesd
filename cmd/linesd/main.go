package main

import (
	"flag"
	"github.com/korovkin/linesd"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"log"
	"os"
)

func main() {
	flag.Parse()

	log.SetOutput(io.MultiWriter(os.Stderr, &lumberjack.Logger{
		Filename:   "/var/log/myapp/foo.log",
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	}))

	log.SetFlags(log.Ltime | log.Lshortfile | log.Lmicroseconds | log.Ldate)

	s := &linesd.Server{}
	s.Initialize()
	s.RunForever()
}
