package broker

import (
	//"encoding/json"
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
