package udp

import (
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"github.com/shanexu/influxdb-balancer/relay"
)

func init() {
	relay.Register("udp", func(cfg *viper.Viper) (relay.Relay, error) {
		udpConfig := relay.UDPConfig{}
		if err := cfg.Unmarshal(&udpConfig, func(config *mapstructure.DecoderConfig) {
			config.TagName = "toml"
		}); err != nil {
			return nil, err
		}
		return relay.NewUDP(udpConfig)
	})
}
