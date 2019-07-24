package client

import (
	"container/list"
	"goyamq/pb"
	"sync"
)

type Client struct {
	sync.Mutex

	cfg *ClientConfig

	conns *list.List

	closed bool
}

func NewClientWithConfig(cfg *ClientConfig) (*Client, error) {
	c := new(Client)
	c.cfg = cfg

	c.conns = list.New()
	c.closed = false

	return c, nil
}

func NewClient(filepath string) (*Client, error) {
	cfg, err := parseConfigFile(filepath)
	if err != nil {
		return nil, err
	}

	return NewClientWithConfig(cfg)
}

func (c *Client) Close() {
	c.Lock()
	defer c.Unlock()

	c.closed = true

	for {
		if c.conns.Len() == 0 {
			break
		}

		e := c.conns.Front()
		c.conns.Remove(e)
		conn := e.Value.(*Conn)
		conn.close()
	}
}

func (c *Client) Get() (*Conn, error) {
	co := c.popConn()
	if co != nil {
		return co, nil
	} else {
		return newConn(c)
	}
}

func (c *Client) Publish(queue string, routingKey string, body []byte, pubType string) (int64, error) {
	conn, err := c.Get()
	if err != nil {
		return 0, err
	}

	defer conn.Close()

	return conn.Publish(queue, routingKey, body, pubType)
}

func (c *Client) PublishFanout(queue string, body []byte) (int64, error) {
	return c.Publish(queue, "", body, pb.FanOut)
}

func (c *Client) PublishDirect(queue string, routingKey string, body []byte) (int64, error) {
	return c.Publish(queue, routingKey, body, "Direct")
}

func (c *Client) popConn() *Conn {
	c.Lock()
	defer c.Unlock()

	for {
		if c.conns.Len() == 0 {
			return nil
		} else {
			e := c.conns.Front()
			c.conns.Remove(e)
			conn := e.Value.(*Conn)
			if !conn.closed {
				return conn
			}
		}
	}
}

func (c *Client) pushConn(co *Conn) {
	c.Lock()
	defer c.Unlock()

	if c.closed || c.conns.Len() >= c.cfg.IdleConns {
		co.close()
	} else {
		c.conns.PushBack(co)
	}
}
