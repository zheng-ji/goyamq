package broker

import (
	"encoding/json"
	"net"
)

type App struct {
	cfg *Config

	listener net.Listener

	ms Store

	qs *queues
}

func NewAppWithConfig(cfg *Config) (*App, error) {
	app := new(App)

	app.cfg = cfg

	var err error

	app.listener, err = net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	app.qs = newQueues(app)

	app.ms, err = OpenStore(cfg.Store, cfg.StoreConfig)
	if err != nil {
		return nil, err
	}

	return app, nil
}

func NewApp(jsonConfig json.RawMessage) (*App, error) {
	cfg, err := parseConfigJson(jsonConfig)
	if err != nil {
		return nil, err
	}

	return NewAppWithConfig(cfg)
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
