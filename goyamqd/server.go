package main

import (
	"flag"
	"goyamq/broker"
)

var (
	configFile = flag.String("c", "conf.yaml", "配置文件路径，默认conf.yaml")
)

func main() {

	app, err := broker.NewApp(*configFile)
	if err != nil {
		panic(err)
	}

	app.Serve()
}
