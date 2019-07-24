package main

import (
	"flag"
	"goyamq/broker"
)

var addr = flag.String("addr", "127.0.0.1:12345", "broker listen address")

func main() {
	flag.Parse()

	cfg := broker.NewDefaultConfig()
	cfg.Addr = *addr

	app, err := broker.NewAppWithConfig(cfg)
	if err != nil {
		panic(err)
	}

	app.Serve()
}
