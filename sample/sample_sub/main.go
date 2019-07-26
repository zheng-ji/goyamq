package main

import (
	"flag"
	"goyamq/client"
)

var addr = flag.String("addr", "127.0.0.1:12345", "broker listen address")
var queue = flag.String("queue", "test_queue", "queue want to bind")

func main() {
	flag.Parse()

	// client->dial to gen conn->gen an queue channel bind channle
	cfg := client.NewDefaultConfig()
	cfg.BrokerAddr = *addr

	c, err := client.NewClientWithConfig(cfg)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	var conn *client.Conn
	conn, err = c.Get()
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	var ch *client.Channel
	ch, err = conn.Bind(*queue, "", false) // 不需要ack
	//ch, err = conn.Bind(*queue, "", true) //需要ack

	for {
		msg := ch.GetMsg()
		println("get msg: ", string(msg))
		//ch.Ack()
	}
}
