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
