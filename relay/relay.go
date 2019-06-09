package relay

import (
	"errors"
	"fmt"
	"github.com/shanexu/logn"
	"github.com/spf13/viper"
	"net/http"
	"strconv"
	"sync"
)

var log = logn.GetLogger()

type ServiceConfig struct {
	PprofPort int `mapstructure:"pprof-port"`
}

type Service struct {
	relays map[string]Relay
}

type RelayConstructor = func(*viper.Viper) (Relay, error)

var constructors = make(map[string]RelayConstructor)

func Register(name string, constructor RelayConstructor) {
	if _, exist := constructors[name]; exist {
		panic(fmt.Sprintf(""))
	}
	constructors[name] = constructor
}

func NewService(config *viper.Viper) (*Service, error) {
	s := new(Service)
	s.relays = make(map[string]Relay)

	cfg := make(map[string]interface{})
	if err := config.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	fmt.Println(cfg)
	for k, icfgs := range cfg {
		if k == "pprof-port" {
			continue
		}
		constructor, exist := constructors[k]
		if !exist {
			panic(fmt.Sprintf("constructor %q not exists", k))
		}
		cfgs, ok := icfgs.([]interface{})
		if !ok {
			return nil, errors.New("icfgs is not []interface{}")
		}
		for _, icfg := range cfgs {
			cfg, ok := icfg.(map[string]interface{})
			if !ok {
				return nil, errors.New("icfg is not map[string]interface{}")
			}
			v := viper.New()
			if err := v.MergeConfigMap(cfg); err != nil {
				return nil, err
			}
			h, err := constructor(v)
			if err != nil {
				return nil, err
			}
			if s.relays[h.Name()] != nil {
				return nil, fmt.Errorf("duplicate relay: %q", h.Name())
			}
			s.relays[h.Name()] = h
		}
	}
	return s, nil
}

func (s *Service) Run() {
	var wg sync.WaitGroup
	wg.Add(len(s.relays))

	for k := range s.relays {
		relay := s.relays[k]
		go func() {
			defer wg.Done()

			if err := relay.Run(); err != nil {
				log.Errorf("Error running relay %q: %v", relay.Name(), err)
			}
		}()
	}

	wg.Wait()
}

func (s *Service) Stop() {
	for _, v := range s.relays {
		v.Stop()
	}
}

type Relay interface {
	Name() string
	Run() error
	Stop() error
}

type ResponseData struct {
	ContentType     string
	ContentEncoding string
	StatusCode      int
	Body            []byte
}

func (rd *ResponseData) Write(w http.ResponseWriter) {
	if rd.ContentType != "" {
		w.Header().Set("Content-Type", rd.ContentType)
	}

	if rd.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", rd.ContentEncoding)
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(rd.Body)))
	w.WriteHeader(rd.StatusCode)
	w.Write(rd.Body)
}

var ErrBufferFull = errors.New("retry buffer full")
