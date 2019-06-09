package gin

import (
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"github.com/shanexu/influxdb-balancer/relay"
	"time"
)

func init() {
	relay.Register("gin", func(viper *viper.Viper) (relay.Relay, error) {
		hc := relay.HTTPConfig{
			SlowLogThreshold: time.Minute,
			QueryMaxRange:    time.Hour * 48,
		}
		if err := viper.Unmarshal(&hc, func(config *mapstructure.DecoderConfig) {
			config.TagName = "toml"
		}); err != nil {
			return nil, err
		}
		return NewHTTP(hc)
	})
}
