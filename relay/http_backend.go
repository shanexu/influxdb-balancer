package relay

import (
	"fmt"
	"golang.org/x/time/rate"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/rcrowley/go-metrics"
)

const (
	DefaultHTTPTimeout      = 10 * time.Second
	DefaultMaxDelayInterval = 10 * time.Second
	DefaultBatchSizeKB      = 512
	KB                      = 1024
	MB                      = 1024 * KB
)

type HttpBackend struct {
	Poster
	name         string
	location     string
	healthCheck  string
	host         string
	reverseProxy *httputil.ReverseProxy
	alive        bool
	cfg          HTTPOutputConfig
	_rl          *unsafe.Pointer
	registry     metrics.Registry
}

func (h *HttpBackend) Name() string {
	return h.name
}

func (h *HttpBackend) Alive() bool {
	return h.alive
}

func (h *HttpBackend) ReverseProxy() *httputil.ReverseProxy {
	return h.reverseProxy
}

func (h *HttpBackend) Host() string {
	return h.host
}

func (h *HttpBackend) UpdateConfig(cfg HTTPOutputConfig) error {
	if cfg.RateLimit == 0 {
		cfg.RateLimit = math.MaxFloat64
	}
	if cfg.RateLimit != math.MaxFloat64 && cfg.RateBurst == 0 {
		cfg.RateBurst = int(math.Ceil(cfg.RateLimit / 10))
	}
	if cfg.RateLimit != h.cfg.RateLimit || cfg.RateBurst != h.cfg.RateBurst {
		atomic.StorePointer(h._rl, unsafe.Pointer(rate.NewLimiter(rate.Limit(cfg.RateLimit), cfg.RateBurst)))
	}

	h.cfg = cfg
	return nil
}

func NewHTTPBackend(dataDir string, cfg *HTTPOutputConfig, registry metrics.Registry) (*HttpBackend, error) {
	if cfg.Name == "" {
		log.Fatal("Name should not be empty")
	}

	timeout := DefaultHTTPTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout '%v'", err)
		}
		timeout = t
	}

	var p Poster = NewSimplePoster(cfg.Name, cfg.WriteEndpoint, cfg.Location, timeout, cfg.SkipTLSVerification, registry)
	if cfg.RateLimit == 0 {
		cfg.RateLimit = math.MaxFloat64
	}
	if cfg.RateLimit != math.MaxFloat64 && cfg.RateBurst == 0 {
		cfg.RateBurst = int(math.Ceil(cfg.RateLimit / 10))
	}
	var _rl unsafe.Pointer
	atomic.StorePointer(&_rl, unsafe.Pointer(rate.NewLimiter(rate.Limit(cfg.RateLimit), cfg.RateBurst)))
	p = NewRateLimitPoster(&_rl, p)

	// If configured, create a retryBuffer per backend.
	// This way we serialize retries against each backend.
	if cfg.BufferSizeMB > 0 {
		max := DefaultMaxDelayInterval
		if cfg.MaxDelayInterval != "" {
			m, err := time.ParseDuration(cfg.MaxDelayInterval)
			if err != nil {
				return nil, fmt.Errorf("error parsing max retry time %v", err)
			}
			max = m
		}

		batch := DefaultBatchSizeKB * KB
		if cfg.MaxBatchKB > 0 {
			batch = cfg.MaxBatchKB * KB
		}
		rb := NewRetryBuffer(dataDir, cfg.Name, cfg.BufferSizeMB*MB, batch, max, p)
		rdbb := NewRocksDBBuffer(dataDir, cfg.Name, uint64(cfg.RocksDBBufferSizeMB*MB), rb)
		p = rdbb
		g := metrics.NewFunctionalGauge(func() int64 {
			return int64(rb.GetBufferSize())
		})
		metrics.Register(cfg.Name+"."+"retryBuffer"+".size", g)
	}

	var reverseProxy *httputil.ReverseProxy
	var host string

	if cfg.Location != "" {
		u, err := url.Parse(cfg.Location)
		if err != nil {
			return nil, err
		}
		reverseProxy = httputil.NewSingleHostReverseProxy(u)
		host = u.Host
	}

	return &HttpBackend{
		name:         cfg.Name,
		Poster:       p,
		reverseProxy: reverseProxy,
		alive:        true,
		location:     cfg.Location,
		healthCheck:  cfg.HealthCheck,
		host:         host,
		cfg:          *cfg,
		_rl:          &_rl,
		registry:     registry,
	}, nil
}

type HealthChecker struct {
	backend      *HttpBackend
	fails        int
	interval     time.Duration
	failsTimeout time.Duration
	ticker       *time.Ticker
}

func NewHealthChecker(backend *HttpBackend) *HealthChecker {
	return &HealthChecker{
		backend:      backend,
		fails:        1,
		interval:     5 * time.Second,
		failsTimeout: 10 * time.Second,
	}
}

func (c *HealthChecker) Run() {
	c.ticker = time.NewTicker(c.interval)
	fails := 0
	go func() {
		for ; true; <-c.ticker.C {
			healthCheck := c.backend.location + "/ping"
			if c.backend.healthCheck != "" {
				healthCheck = c.backend.healthCheck
			}
			resp, err := http.DefaultClient.Get(healthCheck)
			if err != nil || resp.StatusCode/100 != 2 {
				fails += 1
			}
			if fails >= c.fails {
				if c.backend.alive {
					log.Warnf("backend %s is down", c.backend.name)
				}
				c.backend.alive = false
				time.Sleep(c.failsTimeout)
				fails = c.fails - 1
			} else {
				fails = 0
				if !c.backend.alive {
					log.Warnf("backend %s is up", c.backend.name)
				}
				c.backend.alive = true
			}
		}
	}()
}

func (c *HealthChecker) Stop() {
	c.ticker.Stop()
}
