package client

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"goyamq/pb"
	"net"
	"strconv"
	"sync"
	"time"
)

type Conn struct {
	sync.Mutex

	writeLock sync.Mutex

	client *Client

	cfg *ClientConfig

	conn net.Conn

	wait chan *pb.Protocol

	closed bool

	lastHeartbeat int64

	channels map[string]*Channel
}

func newConn(client *Client) (*Conn, error) {
	c := new(Conn)

	c.client = client
	c.cfg = client.cfg

	var err error
	if c.conn, err = net.Dial("tcp", c.cfg.BrokerAddr); err != nil {
		return nil, err
	}

	c.channels = make(map[string]*Channel)

	c.wait = make(chan *pb.Protocol, 1)

	c.closed = false

	c.lastHeartbeat = 0

	go c.run()

	return c, nil
}

func (c *Conn) Close() {
	c.unbindAll()

	c.client.pushConn(c)
}

func (c *Conn) close() {
	c.conn.Close()
	c.closed = true
}

func (c *Conn) run() {
	defer func() {
		c.conn.Close()

		close(c.wait)

		c.closed = true
	}()

	for {
		buf := make([]byte, 1024)
		length, err := c.conn.Read(buf)
		p := &pb.Protocol{}
		err = proto.Unmarshal(buf[0:length], p)
		if err != nil {
			return
		}

		if p.GetMethod() == pb.Push {
			queueName := p.GetQueue()
			c.Lock()
			ch, ok := c.channels[queueName]
			if !ok {
				c.Unlock()
				return
			}
			c.Unlock()

			ch.pushMsg(p.GetMsgid(), []byte(p.GetBody()))
		} else {
			c.wait <- p
		}

	}
}

func (c *Conn) request(p *pb.Protocol, expectMethod string) (*pb.Protocol, error) {
	err := c.writeProtocol(p)

	if err != nil {
		return nil, err
	}

	rp, ok := <-c.wait
	if !ok {
		return nil, fmt.Errorf("wait channel closed")
	}

	if rp.GetMethod() == pb.Error {
		return nil, fmt.Errorf("error:%s", rp.GetBody())
	}

	if rp.GetMethod() != expectMethod {
		return nil, fmt.Errorf("expectMethod err expect:%s, get:%s", expectMethod, rp.GetMethod())
	}

	return rp, nil
}

func (c *Conn) writeProtocol(p *pb.Protocol) error {
	buf, err := proto.Marshal(p)
	if err != nil {
		return err
	}
	c.writeLock.Lock()
	n, err := c.conn.Write(buf)
	c.writeLock.Unlock()

	if err != nil {
		c.close()
		return err
	} else if n != len(buf) {
		c.close()
		return fmt.Errorf("write short %d != %d", n, len(buf))
	}

	return nil
}

func (c *Conn) Publish(queue string, routingKey string, body []byte, pubType string) (int64, error) {
	p := &pb.Protocol{
		Method:     proto.String(pb.Publish),
		RoutingKey: proto.String(routingKey),
		PubType:    proto.String(pubType),
		Queue:      proto.String(queue),
		Body:       proto.String(string(body)),
	}

	c.Lock()
	defer c.Unlock()

	np, err := c.request(p, pb.PublishOK)
	if err != nil {
		return 0, err
	}
	fmt.Printf("p:%s\n", p.String())
	fmt.Printf("Get np:%s\n", np.String())

	return strconv.ParseInt(string(np.GetMsgid()), 10, 64)
}

func (c *Conn) Bind(queue string, routingKey string, noAck bool) (*Channel, error) {
	c.Lock()
	defer c.Unlock()

	ch, ok := c.channels[queue]
	if !ok {
		ch = newChannel(c, queue, routingKey, noAck)
		c.channels[queue] = ch
	} else {
		ch.routingKey = routingKey
		ch.noAck = noAck
	}

	p := &pb.Protocol{
		Method:     proto.String(pb.Bind),
		RoutingKey: proto.String(routingKey),
		Queue:      proto.String(queue),
		Ack:        proto.Bool(noAck),
	}

	rp, err := c.request(p, pb.BindOK)

	if err != nil {
		return nil, err
	}

	if rp.GetQueue() != queue {
		return nil, fmt.Errorf("invalid bind response queue %s", rp.GetQueue())
	}

	return ch, nil
}

func (c *Conn) unbindAll() error {
	c.Lock()
	defer c.Unlock()

	c.channels = make(map[string]*Channel)

	p := &pb.Protocol{
		Method: proto.String(pb.UnBind),
		Queue:  proto.String(""),
	}

	_, err := c.request(p, pb.UnBindOK)
	return err
}

func (c *Conn) unbind(queue string) error {
	c.Lock()
	defer c.Unlock()

	_, ok := c.channels[queue]
	if !ok {
		return fmt.Errorf("queue %s not bind", queue)
	}

	delete(c.channels, queue)

	p := &pb.Protocol{
		Method: proto.String(pb.UnBind),
		Queue:  proto.String(queue),
	}

	rp, err := c.request(p, pb.UnBindOK)
	if err != nil {
		return err
	}

	if rp.GetQueue() != queue {
		return fmt.Errorf("invalid bind response queue %s", rp.GetQueue())
	}

	return nil
}

func (c *Conn) ack(queue string, msgId string) error {
	p := &pb.Protocol{
		Method: proto.String(pb.Ack),
		Queue:  proto.String(queue),
		Msgid:  proto.String(msgId),
	}

	return c.writeProtocol(p)
}

var ErrChannelClosed = errors.New("channel has been closed")

type channelMsg struct {
	ID   string
	Body []byte
}

type Channel struct {
	c          *Conn
	queue      string
	routingKey string
	noAck      bool

	msg    chan *channelMsg
	closed bool

	lastId string
}

func newChannel(c *Conn, queue string, routingKey string, noAck bool) *Channel {
	ch := new(Channel)

	ch.c = c
	ch.queue = queue
	ch.routingKey = routingKey
	ch.noAck = noAck

	ch.msg = make(chan *channelMsg, c.cfg.MaxQueueSize)

	ch.closed = false
	return ch
}

func (c *Channel) Close() error {
	c.closed = true

	return c.c.unbind(c.queue)
}

func (c *Channel) Ack() error {
	if c.closed {
		return ErrChannelClosed
	}

	return c.c.ack(c.queue, c.lastId)
}

func (c *Channel) GetMsg() []byte {
	if c.closed && len(c.msg) == 0 {
		return nil
	}

	msg := <-c.msg
	c.lastId = msg.ID
	return msg.Body
}

func (c *Channel) WaitMsg(d time.Duration) []byte {
	if c.closed && len(c.msg) == 0 {
		return nil
	}

	select {
	case <-time.After(d):
		return nil
	case msg := <-c.msg:
		c.lastId = msg.ID
		return msg.Body
	}
}

func (c *Channel) pushMsg(msgId string, body []byte) {
	for {
		select {
		case c.msg <- &channelMsg{msgId, body}:
			return
		default:
			<-c.msg
		}
	}
}
