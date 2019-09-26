package broker

import (
	"container/list"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
)

type queue struct {
	qs *queues

	app *App

	store Store

	name string

	channels *list.List

	ch chan func()

	waitingAcks map[*channel]struct{}
	lastPushId  int64
}

func newQueue(qs *queues, name string) *queue {
	rq := new(queue)

	rq.qs = qs
	rq.app = qs.app

	rq.store = qs.app.ms

	rq.name = name

	rq.channels = list.New()

	rq.lastPushId = -1

	rq.waitingAcks = make(map[*channel]struct{})

	rq.ch = make(chan func(), 32)

	go rq.run()

	return rq
}

func (rq *queue) run() {
	for {
		select {
		case f := <-rq.ch:
			f()
		case <-time.After(5 * time.Minute):
			if rq.channels.Len() == 0 {
				m, _ := rq.getMsg()
				if m == nil {
					rq.qs.Delete(rq.name)
					return
				}
			}
		}
	}
}

func (rq *queue) Bind(c *channel) {
	f := func() {
		log.Info("rq.ch func Bind")
		for e := rq.channels.Front(); e != nil; e = e.Next() {
			if e.Value.(*channel) == c {

				return
			}
		}

		rq.channels.PushBack(c)

		rq.push()
	}

	rq.ch <- f
}

func (rq *queue) Unbind(c *channel) {
	f := func() {
		var repush bool = false
		for e := rq.channels.Front(); e != nil; e = e.Next() {
			if e.Value.(*channel) == c {
				rq.channels.Remove(e)

				if _, ok := rq.waitingAcks[c]; ok {
					//conn not ack
					delete(rq.waitingAcks, c)

					if len(rq.waitingAcks) == 0 {
						//all waiting conn not send ack, repush
						repush = true
					}
				}
				break
			}
		}
		if repush {
			rq.lastPushId = -1
			rq.push()
		}
		log.Infof("rq.ch func UnBind, repush:%v", false)
	}

	rq.ch <- f
}

func (rq *queue) Ack(msgId int64) {
	f := func() {

		if msgId != rq.lastPushId {
			log.Info("Break here msgId:%v, lastPushId:%v", msgId, rq.lastPushId)
			return
		}

		rq.store.Delete(rq.name, msgId)

		rq.waitingAcks = map[*channel]struct{}{}
		rq.lastPushId = -1

		rq.push()
		log.Infof("rq.ch func Ack, msgId:%v, lastPushId:%v", msgId, rq.lastPushId)
	}

	rq.ch <- f
}

func (rq *queue) Push(m *msg) {
	f := func() {
		rq.push()
		log.Infof("rq.ch func Push, msg:%v", m)
	}

	rq.ch <- f
}

func (rq *queue) getMsg() (*msg, error) {
	var m *msg
	var err error
	for {
		m, err = rq.store.Front(rq.name)
		if err != nil {
			return nil, err
		} else if m == nil {
			return nil, nil
		}

		if rq.app.cfg.MessageTimeout > 0 {
			now := time.Now().Unix()
			if m.ctime+int64(rq.app.cfg.MessageTimeout) < now {
				if err := rq.store.Delete(rq.name, m.id); err != nil {
					return nil, err
				}
			} else {
				break
			}
		}
	}

	return m, nil
}

func (rq *queue) push() {
	if rq.lastPushId != -1 {
		return
	}

	if rq.channels.Len() == 0 {
		log.Info("rq.channels.Len == 0 push return")
		return
	}

	m, err := rq.getMsg()
	log.Infof("rq.getMsg:%v err:%v", m, err)
	if err != nil {
		return
	} else if m == nil {
		return
	}

	switch m.pubType {
	case 1:
		err = rq.pushFanout(m)
	default:
		err = rq.pushDirect(m)
	}

	if err == nil {
		rq.lastPushId = m.id
	}
}

func (rq *queue) pushMsg(done chan bool, m *msg, c *channel) {
	go func() {
		if err := c.Push(m); err == nil {
			//push suc
			done <- true
		} else {
			done <- false
		}
	}()
}

func (rq *queue) match(m *msg, c *channel) bool {
	pubKey := m.routingKey
	subKey := c.routingKey

	//now simple check same, later check regexp like rabbitmq
	return pubKey == subKey
}

func (rq *queue) pushDirect(m *msg) error {
	var c *channel = nil
	for e := rq.channels.Front(); e != nil; e = e.Next() {
		ch := e.Value.(*channel)
		if !rq.match(m, ch) {
			continue
		}
		rq.channels.Remove(e)
		rq.channels.PushBack(ch)

		c = ch
		break
	}

	if c == nil {
		//no channel match, discard msg and push next
		rq.store.Delete(rq.name, m.id)

		f := func() {
			rq.push()
			log.Infof("rq.ch func PushDirect, rq.name:%v id:%v", rq.name, m.id)
		}

		rq.ch <- f
		return fmt.Errorf("discard msg")
	}

	rq.waitingAcks[c] = struct{}{}

	done := make(chan bool, 1)

	rq.pushMsg(done, m, c)

	if r := <-done; r == true {
		return nil
	} else {
		return fmt.Errorf("push direct error")
	}
}

func (rq *queue) pushFanout(m *msg) error {
	done := make(chan bool, rq.channels.Len())

	log.Infof("In pushFanout m:%v, rq.channels.Len:%d", m, rq.channels.Len())
	for e := rq.channels.Front(); e != nil; e = e.Next() {
		c := e.Value.(*channel)
		rq.waitingAcks[c] = struct{}{}

		log.Infof("In child push m:%v", m)
		rq.pushMsg(done, m, c)
	}

	for i := 0; i < rq.channels.Len(); i++ {
		r := <-done
		if r == true {
			return nil
		}
	}

	return fmt.Errorf("push fanout error")
}
