package broker

import (
	"encoding/binary"
	"fmt"
	"time"
)

type msg struct {
	id         int64
	ctime      int64
	pubType    uint8
	routingKey string
	body       []byte
}

func newMsg(id int64, pubType uint8, routingKey string, body []byte) *msg {
	m := new(msg)

	m.id = id
	m.ctime = time.Now().Unix()
	m.pubType = pubType
	m.routingKey = routingKey
	m.body = body

	return m
}
