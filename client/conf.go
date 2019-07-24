package client

import (
	"fmt"
	goyaml "gopkg.in/yaml.v3"
	"io/ioutil"
)

const defaultQueueSize int = 16

type ClientConfig struct {
	BrokerAddr   string `yaml:"broker_addr"`
	KeepAlive    int    `yaml:"keep_alive"`
	IdleConns    int    `yaml:"idle_conns"`
	MaxQueueSize int    `yaml:"max_queue_size"`
}

func NewDefaultConfig() *ClientConfig {
	cfg := new(ClientConfig)

	cfg.BrokerAddr = "127.0.0.1:11181"
	cfg.KeepAlive = 60
	cfg.IdleConns = 2
	cfg.MaxQueueSize = 16

	return cfg
}

func parseConfigFile(filepath string) (*ClientConfig, error) {

	clientConfig := new(ClientConfig)

	if config, err := ioutil.ReadFile(filepath); err == nil {

		if err = goyaml.Unmarshal(config, clientConfig); err != nil {
			return nil, err
		}

		return clientConfig, nil

	} else {
		fmt.Printf("fail to parse:%s", filepath)
		return nil, err
	}
	return nil, nil
}
