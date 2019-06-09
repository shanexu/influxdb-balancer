package config_server

import (
	"github.com/coreos/etcd/embed"
	"github.com/shanexu/logn"
	"net/url"
	"time"
)

var log = logn.GetLogger("config_server")

type ConfigServer struct {
	ListenPeerURLs   []string
	ListenClientURLs []string
	Name             string
	Dir              string
	e                *embed.Etcd
	InitialCluster   string
	ClusterState     string
}

func (cs *ConfigServer) Run() error {
	cfg := embed.NewConfig()
	cfg.Name = cs.Name
	cfg.Dir = cs.Dir
	lpurls, err := toUrls(cs.ListenPeerURLs)
	if err != nil {
		return err
	}
	lcurls, err := toUrls(cs.ListenClientURLs)
	if err != nil {
		return err
	}
	cfg.LPUrls = lpurls
	cfg.LCUrls = lcurls
	cfg.APUrls = lpurls
	cfg.ACUrls = lcurls

	cfg.InitialCluster = cs.InitialCluster
	if cs.ClusterState != "" {
		cfg.ClusterState = cs.ClusterState
	}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatal(err)
	}
	cs.e = e
	// defer e.Close()
	select {
	case <-e.Server.ReadyNotify():
		log.Info("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		log.Info("Server took too long to start!")
	}
	go func() {
		log.Warn(<-e.Err())
	}()
	return nil
}

func (cs *ConfigServer) Stop() {
	cs.e.Close()
}

func toUrls(rawUrls []string) ([]url.URL, error) {
	us := make([]url.URL, len(rawUrls))

	for i, rawUrl := range rawUrls {
		u, err := url.Parse(rawUrl)
		if err != nil {
			return nil, err
		}
		us[i] = *u
	}
	return us, nil
}
