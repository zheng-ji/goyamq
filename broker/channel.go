//use channel represent conn bind a queue
package broker

import (
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"goyamq/pb"
	"strconv"
)

type connMsgPusher struct {
	c *conn
}

func (p *connMsgPusher) Push(ch *channel, m *msg) error {
	np := &pb.Protocol{
		Method: proto.String(pb.Push),
		Queue:  proto.String(ch.q.name),
		Msgid:  proto.String(strconv.FormatInt(m.id, 10)),
		Body:   proto.String(string(m.body)),
	}

	err := p.c.writeProtocol(np)

	if err == nil && !ch.ack {
		log.Info("In [Push] %v", np)
		ch.Ack(m.id)
	} else {
		log.Errorf("connMsgPusher.writeProtocol p:%s, err:%v", np.String(), err)
	}

	return err
}

type channel struct {
	p          connMsgPusher
	q          *queue
	routingKey string
	ack        bool
}

func newChannel(p connMsgPusher, q *queue, routingKey string, ack bool) *channel {
	ch := new(channel)

	ch.p = p
	ch.q = q

	ch.routingKey = routingKey
	ch.ack = ack

	q.Bind(ch)
	return ch
}

func (c *channel) Reset(routingKey string, ack bool) {
	c.routingKey = routingKey
	c.ack = ack
}

func (c *channel) Close() {
	c.q.Unbind(c)
}

func (c *channel) Push(m *msg) error {
	return c.p.Push(c, m)
}

func (c *channel) Ack(msgId int64) {
	c.q.Ack(msgId)
}
