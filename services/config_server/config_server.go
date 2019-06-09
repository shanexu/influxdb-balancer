package config_server

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/shanexu/logn"
	"time"
)

var log = logn.GetLogger("config_server")

type NamedClient struct {
	*clientv3.Client
	name string
}

func (nc *NamedClient) Name() string {
	return nc.name
}

var cli *NamedClient

func Init(name string, bindClient string) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{bindClient},
		DialTimeout: time.Second * 10,
	})
	if err != nil {
		log.Fatalf("init config server failed %s", err)
	}
	cli = &NamedClient{client, name}
}

func GetCli() *NamedClient {
	return cli
}
