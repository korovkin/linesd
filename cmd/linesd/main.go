package main

import (
	"flag"
	"github.com/korovkin/linesd"
	"log"
)

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile | log.Lmicroseconds | log.Ldate)

	s := &linesd.Server{}
	s.Initialize()
	s.RunForever()
}
