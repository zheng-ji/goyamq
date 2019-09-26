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
	client := new(Client)
	client.cfg = cfg

	client.conns = list.New()
	client.closed = false

	return client, nil
}

func NewClient(filepath string) (*Client, error) {
	cfg, err := parseConfigFile(filepath)
	if err != nil {
		return nil, err
	}

	return NewClientWithConfig(cfg)
}

func (client *Client) Close() {
	client.Lock()
	defer client.Unlock()

	client.closed = true

	for {
		if client.conns.Len() == 0 {
			break
		}

		e := client.conns.Front()
		client.conns.Remove(e)
		conn := e.Value.(*Conn)
		conn.close()
	}
}

func (client *Client) Get() (*Conn, error) {
	co := client.popConn()
	if co != nil {
		return co, nil
	} else {
		return newConn(client.cfg)
	}
}

func (client *Client) popConn() *Conn {
	client.Lock()
	defer client.Unlock()

	for {
		if client.conns.Len() == 0 {
			return nil
		} else {
			e := client.conns.Front()
			client.conns.Remove(e)
			conn := e.Value.(*Conn)
			if !conn.closed {
				return conn
			}
		}
	}
}

func (client *Client) pushConn(co *Conn) {
	client.Lock()
	defer client.Unlock()

	if client.closed || client.conns.Len() >= client.cfg.IdleConns {
		co.close()
	} else {
		client.conns.PushBack(co)
	}
}

func (client *Client) Publish(queue string, routingKey string, body []byte, pubType string) (int64, error) {
	conn, err := client.Get()
	if err != nil {
		return 0, err
	}

	defer func() {
		conn.Close()
		client.pushConn(conn)
	}()

	return conn.Publish(queue, routingKey, body, pubType)
}

func (client *Client) PublishFanout(queue string, body []byte) (int64, error) {
	return client.Publish(queue, "", body, pb.FanOut)
}

func (client *Client) PublishDirect(queue string, routingKey string, body []byte) (int64, error) {
	return client.Publish(queue, routingKey, body, pb.Direct)
}
