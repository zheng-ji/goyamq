package broker

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"goyamq/pb"
	"io"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type conn struct {
	sync.Mutex

	app *App

	c net.Conn

	lastUpdate int64

	channels map[string]*channel
}

func newConn(app *App, co net.Conn) *conn {
	c := new(conn)

	c.app = app
	c.c = co

	c.checkKeepAlive()

	c.channels = make(map[string]*channel)

	return c
}

func (c *conn) run() {
	c.onRead()

	c.unBindAll()

	c.c.Close()
}

func (c *conn) unBindAll() {
	for _, ch := range c.channels {
		ch.Close()
	}

	c.channels = map[string]*channel{}
}

func (c *conn) onRead() {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 1024)
			buf = buf[:runtime.Stack(buf, false)]
			log.Fatalf("crash %v:%s", err, buf)
		}
	}()

	/*
		for {
			buf := make([]byte, 1024)
			length, err := c.c.Read(buf)

			if err != nil {
				if err != io.EOF {
					log.Info("on read error %v", err)
				}
				return
			}

			p := &pb.Protocol{}
			err = proto.Unmarshal(buf[0:length], p)
			if err != nil {
				log.Error("Unmarsh fail")
				return
			}

			log.Infof("p:%s", p.String())

			switch p.GetMethod() {
			case pb.Publish:
				err = c.handlePublish(p)
			case pb.Bind:
				err = c.handleBind(p)
			case pb.UnBind:
				err = c.handleUnbind(p)
			case pb.Ack:
				err = c.handleAck(p)
			case pb.HeartBeat:
				c.lastUpdate = time.Now().Unix()
			default:
				log.Info("invalid protocol method %s", p.GetMethod())
				return
			}

			if err != nil {
				c.writeError(err)
			}
		}
	*/
	allbuf := make([]byte, 0)
	buffer := make([]byte, 1024)
	for {
		readLen, err := c.c.Read(buffer)

		if err == io.EOF {
			log.Infof("Client %s close connection", c.c.RemoteAddr().String())
			return
		}

		if err != nil {
			log.Errorf("Read Data from client:%s err:%s", c.c.RemoteAddr().String(), err.Error())
			return
		}
		allbuf = append(allbuf, buffer[:readLen]...)

		for {
			p := &pb.Protocol{}
			err = proto.Unmarshal(allbuf, p)
			if err != nil || proto.Size(p) == 0 {
				log.Errorf("proto size zero return p:%v, err:%v, len:%d", p, err, proto.Size(p))
				break
			}
			log.Infof("receive p:%s", p.String())

			switch p.GetMethod() {
			case pb.Publish:
				err = c.handlePublish(p)
			case pb.Bind:
				err = c.handleBind(p)
			case pb.UnBind:
				err = c.handleUnbind(p)
			case pb.Ack:
				err = c.handleAck(p)
			case pb.HeartBeat:
				c.lastUpdate = time.Now().Unix()
			default:
				log.Info("invalid protocol method %s", p.GetMethod())
				return
			}
			allbuf = allbuf[proto.Size(p):]
			break
		}
	}
}

func (c *conn) checkKeepAlive() {

	ticker := time.NewTicker(time.Duration(c.app.cfg.KeepAlive) * time.Second)

	go func() {
		for _ = range ticker.C {
			if time.Now().Unix()-c.lastUpdate > int64(float32(c.app.cfg.KeepAlive)) {
				log.Info("keepalive timeout")
				c.c.Close()
				return
			}
		}
	}()
}

func (c *conn) writeError(err error) {
	p := &pb.Protocol{
		Method: proto.String(pb.Error),
		Body:   proto.String(err.Error()),
	}

	c.writeProtocol(p)
}

func (c *conn) writeProtocol(p *pb.Protocol) error {
	log.Info("in [writeProtocol]", p.String())
	buf, err := proto.Marshal(p)
	if err != nil {
		return err
	}

	var n int
	c.Lock()
	n, err = c.c.Write(buf)
	c.Unlock()

	if err != nil {
		log.Errorf("writeProtocol Err:%v", err)
		c.c.Close()
		return err
	} else if n != len(buf) {
		log.Errorf("writeProtocol n:%d != len(buf):%d", n, len(buf))
		c.c.Close()
		return fmt.Errorf("write incomplete, %d less than %d", n, len(buf))
	} else {
		return nil
	}
}

func (app *App) saveMsg(queue string, routingKey string, tp string, message []byte) (*msg, error) {
	var t uint8 = 0
	if tp == pb.FanOut {
		t = 1
	}

	if app.cfg.MaxQueueSize > 0 {
		if n, err := app.ms.Len(queue); err != nil {
			return nil, err
		} else if n >= app.cfg.MaxQueueSize {
			if err = app.ms.Pop(queue); err != nil {
				return nil, err
			}
		}
	}

	id, err := app.ms.GenerateID()
	if err != nil {
		return nil, err
	}

	msg := newMsg(id, t, routingKey, message)

	if err := app.ms.Save(queue, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func (c *conn) handlePublish(p *pb.Protocol) error {
	tp := p.GetPubType()
	queue := p.GetQueue()
	routingKey := p.GetRoutingKey()

	message := p.GetBody()

	msg, err := c.app.saveMsg(queue, routingKey, tp, []byte(message))
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	q := c.app.qs.Get(queue)
	q.Push(msg)

	np := &pb.Protocol{
		Method: proto.String(pb.PublishOK),
		Msgid:  proto.String(strconv.FormatInt(msg.id, 10)),
	}

	c.writeProtocol(np)

	return nil
}

func (c *conn) handleAck(p *pb.Protocol) error {
	queue := p.GetQueue()
	ch, ok := c.channels[queue]
	if !ok {
		log.Errorf("invalide queue:%s", queue)
		return nil
	}

	msgId, err := strconv.ParseInt(p.GetMsgid(), 10, 64)
	if err != nil {
		return err
	}

	ch.Ack(msgId)

	return nil
}

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

func (c *conn) handleBind(p *pb.Protocol) error {
	queue := p.GetQueue()
	routingKey := p.GetRoutingKey()

	ack := p.GetAck()

	ch, ok := c.channels[queue]
	if !ok {
		q := c.app.qs.Get(queue)
		ch = newChannel(connMsgPusher{c}, q, routingKey, ack)
		c.channels[queue] = ch
	} else {
		ch.Reset(routingKey, ack)
	}

	np := &pb.Protocol{
		Method: proto.String(pb.BindOK),
		Queue:  proto.String(queue),
	}

	c.writeProtocol(np)

	return nil
}

func (c *conn) handleUnbind(p *pb.Protocol) error {
	queue := p.GetQueue()
	if len(queue) == 0 {
		c.unBindAll()

		np := &pb.Protocol{
			Method: proto.String(pb.UnBindOK),
			Queue:  proto.String(queue),
		}

		c.writeProtocol(np)
		return nil
	}

	if ch, ok := c.channels[queue]; ok {
		delete(c.channels, queue)
		ch.Close()
	}

	np := &pb.Protocol{
		Method: proto.String(pb.UnBindOK),
		Queue:  proto.String(queue),
	}

	c.writeProtocol(np)

	return nil
}

//use channel represent conn bind a queue

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
