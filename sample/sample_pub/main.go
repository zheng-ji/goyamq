package main

import (
	"flag"
	"goyamq/client"
)

var addr = flag.String("addr", "127.0.0.1:12345", "broker listen address")
var queue = flag.String("queue", "test_queue", "queue want to bind")
var msg = flag.String("msg", "hello world", "msg to publish")

func main() {
	flag.Parse()

	cfg := client.NewDefaultConfig()
	cfg.BrokerAddr = *addr

	c, err := client.NewClientWithConfig(cfg)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	_, err = c.PublishFanout(*queue, []byte(*msg))
	if err != nil {
		panic(err)
	}
}
