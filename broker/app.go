package broker

import (
	"goyamq/pb"
	"net"
)

type App struct {
	cfg *BrokerConfig

	listener net.Listener

	ms Store

	qs *queues
}

func NewAppWithConfig(cfg *BrokerConfig) (*App, error) {
	app := new(App)

	app.cfg = cfg

	var err error

	app.listener, err = net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	app.qs = newQueues(app)

	app.ms, err = OpenStore(cfg.Store, cfg.StoreAddr)
	if err != nil {
		return nil, err
	}

	return app, nil
}

func NewApp(filepath string) (*App, error) {
	config, err := parseConfigFile(filepath)
	if err != nil {
		return nil, err
	}

	return NewAppWithConfig(config)
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

func (app *App) Close() {
	if app.listener != nil {
		app.listener.Close()
	}

	app.ms.Close()
}

func (app *App) Serve() {

	for {
		conn, err := app.listener.Accept()
		if err != nil {
			continue
		}

		co := newConn(app, conn)
		go co.run()
	}
}
