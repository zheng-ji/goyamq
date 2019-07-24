package broker

import (
	"fmt"
	goyaml "gopkg.in/yaml.v3"
	"io/ioutil"
)

// BrokerConfig Type
type BrokerConfig struct {
	Addr string `yaml:"addr"`

	KeepAlive      int `yaml:"keep_alive"`
	MaxMessageSize int `yaml:"max_msg_size"`
	MessageTimeout int `yaml:"msg_timeout"`
	MaxQueueSize   int `yaml:"max_queue_size"`

	Store     string `yaml:"store"`
	StoreAddr string `yaml:"store_addr"`
}

func NewDefaultConfig() *BrokerConfig {
	cfg := new(BrokerConfig)

	cfg.Addr = "127.0.0.1:11181"

	cfg.KeepAlive = 65

	cfg.MaxMessageSize = 1024
	cfg.MessageTimeout = 3600 * 24
	cfg.MaxQueueSize = 1024

	cfg.Store = "mem"
	cfg.StoreAddr = ""

	return cfg
}

func parseConfigFile(filepath string) (*BrokerConfig, error) {

	brokerConfig := new(BrokerConfig)

	if config, err := ioutil.ReadFile(filepath); err == nil {

		if err = goyaml.Unmarshal(config, brokerConfig); err != nil {
			return nil, err
		}

		return brokerConfig, nil

	} else {
		fmt.Printf("fail to parse:%s", filepath)
		return nil, err
	}
	return nil, nil
}
