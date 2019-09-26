package broker

import (
	"sync"
)

type queues struct {
	sync.RWMutex
	app *App

	qs map[string]*queue
}

func newQueues(app *App) *queues {
	qs := new(queues)

	qs.app = app
	qs.qs = make(map[string]*queue)

	return qs
}

func (qs *queues) Get(name string) *queue {
	qs.Lock()
	if r, ok := qs.qs[name]; ok {
		qs.Unlock()
		return r
	} else {
		r := newQueue(qs, name)
		qs.qs[name] = r
		qs.Unlock()
		return r
	}
}

func (qs *queues) Getx(name string) *queue {
	qs.RLock()
	r, ok := qs.qs[name]
	qs.RUnlock()

	if ok {
		return r
	} else {
		return nil
	}

}

func (qs *queues) Delete(name string) {
	qs.Lock()
	delete(qs.qs, name)
	qs.Unlock()
}
