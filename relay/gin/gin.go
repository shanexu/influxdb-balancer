package gin

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/gin-gonic/gin"
	"github.com/influxdata/influxdb/models"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/shanexu/logn"

	"github.com/shanexu/influxdb-balancer/config_server"
	"github.com/shanexu/influxdb-balancer/relay"
)

var log = logn.GetLogger("http")

type HTTP struct {
	addr   string
	name   string
	schema string

	cert string
	rp   string

	srv    *http.Server
	locker sync.RWMutex

	outputs        map[string]relay.Poster
	globalOutputs  map[string]relay.Poster
	healthCheckers map[string]*relay.HealthChecker
	backends       map[string][]relay.Poster

	kafkaClusters  map[string]*relay.KafkaCluster
	kafkaOutputs   map[string]relay.Poster
	kafkaBackends  map[string][]relay.Poster
	kafkaConsumers map[string]*relay.KafkaConsumer

	cli *clientv3.Client
	cs  *config_server.ConfigServer

	UseKafkaOutput bool
	UseKafkaInput  bool
	KafkaGroup     string
	cfg            relay.HTTPConfig
	registry       metrics.Registry
	done           chan struct{}
	metricDb       string
}

func NewHTTP(cfg relay.HTTPConfig) (relay.Relay, error) {
	h := new(HTTP)
	h.metricDb = cfg.MetricDb
	h.done = make(chan struct{})
	h.cfg = cfg
	h.registry = metrics.NewRegistry()

	h.UseKafkaOutput = cfg.UseKafkaOutput
	h.UseKafkaInput = cfg.UseKafkaInput
	h.KafkaGroup = cfg.KafkaGroup
	if h.UseKafkaInput && h.KafkaGroup == "" {
		return nil, errors.New("kafka group should not be empty")
	}

	h.addr = cfg.Addr
	h.name = cfg.Name

	h.cert = cfg.SSLCombinedPem
	h.rp = cfg.DefaultRetentionPolicy

	cs := config_server.ConfigServer{
		ListenPeerURLs:   []string{cfg.ConfigServer.BindPeer},
		ListenClientURLs: []string{cfg.ConfigServer.BindClient},
		Name:             cfg.ConfigServer.Name,
		Dir:              cfg.ConfigServer.Dir,
		InitialCluster:   cfg.ConfigServer.InitialCluster,
		ClusterState:     cfg.ConfigServer.ClusterState,
	}
	if err := cs.Run(); err != nil {
		log.Fatal("start config server failed " + err.Error())
	}
	h.cs = &cs
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.ConfigServer.BindClient},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal("connect config server failed " + err.Error())
	}
	rep, err := cli.Get(context.Background(), "/"+cfg.Name, clientv3.WithPrefix())
	if err != nil {
		log.Fatal("fetch initial config failed " + err.Error())
	}
	h.cli = cli

	outputs := make([]relay.HTTPOutputConfig, 0)
	databases := make([]relay.DatabaseConfig, 0)

	kafkaOutputs := make([]relay.KafkaOutputConfig, 0)
	kafkaDatabases := make([]relay.DatabaseConfig, 0)
	kafkaClusters := make([]relay.KafkaClusterConfig, 0)

	for _, kv := range rep.Kvs {
		key := string(kv.Key)
		value := kv.Value
		switch {
		case strings.HasPrefix(key, "/"+cfg.Name+"/output"):
			output := relay.HTTPOutputConfig{}
			err := json.Unmarshal(value, &output)
			if err != nil {
				log.Error(err)
				continue
			}
			outputs = append(outputs, output)
		case strings.HasPrefix(key, "/"+cfg.Name+"/database"):
			database := relay.DatabaseConfig{}
			err := json.Unmarshal(value, &database)
			if err != nil {
				log.Error(err)
				continue
			}
			databases = append(databases, database)
		case strings.HasPrefix(key, "/"+cfg.Name+"/kafka_cluster"):
			cluster := relay.KafkaClusterConfig{}
			err := json.Unmarshal(value, &cluster)
			if err != nil {
				log.Error(err)
				continue
			}
			kafkaClusters = append(kafkaClusters, cluster)
		case strings.HasPrefix(key, "/"+cfg.Name+"/kafka_output"):
			kafkaOutput := relay.KafkaOutputConfig{}
			err := json.Unmarshal(value, &kafkaOutput)
			if err != nil {
				log.Error(err)
				continue
			}
			kafkaOutputs = append(kafkaOutputs, kafkaOutput)
		case strings.HasPrefix(key, "/"+cfg.Name+"/kafka_database"):
			database := relay.DatabaseConfig{}
			err := json.Unmarshal(value, &database)
			if err != nil {
				log.Error(err)
				continue
			}
			kafkaDatabases = append(kafkaDatabases, database)
		}
	}

	h.schema = "http"
	if h.cert != "" {
		h.schema = "https"
	}

	h.outputs = make(map[string]relay.Poster)
	h.globalOutputs = make(map[string]relay.Poster)
	var backends []*relay.HttpBackend
	for i := range outputs {
		backend, err := relay.NewHTTPBackend(cfg.DataDir, &outputs[i], h.registry)
		if err != nil {
			return nil, err
		}
		backends = append(backends, backend)
		h.outputs[backend.Name()] = backend
		if (&outputs[i]).Global {
			h.globalOutputs[backend.Name()] = backend
		}
	}

	h.backends = make(map[string][]relay.Poster)
	for _, d := range databases {
		var bs []relay.Poster
		for _, o := range d.Outputs {
			for _, b := range backends {
				if o == b.Name() {
					bs = append(bs, b)
					break
				}
			}
		}
		h.backends[d.Name] = bs
	}

	h.healthCheckers = make(map[string]*relay.HealthChecker)
	for i := range backends {
		b := backends[i]
		hc := relay.NewHealthChecker(b)
		h.healthCheckers[b.Name()] = hc
		hc.Run()
	}

	h.kafkaClusters = make(map[string]*relay.KafkaCluster)
	h.kafkaOutputs = make(map[string]relay.Poster)
	h.kafkaBackends = make(map[string][]relay.Poster)
	h.kafkaConsumers = make(map[string]*relay.KafkaConsumer)
	if h.UseKafkaOutput {
		for i := range kafkaClusters {
			cluster, err := relay.NewKafkaCluster(kafkaClusters[i])
			if err != nil {
				return nil, err
			}
			if err = cluster.Open(); err != nil {
				return nil, err
			}
			h.kafkaClusters[cluster.Name()] = cluster
		}

		var backends []*relay.KafkaBackend
		for i := range kafkaOutputs {
			kafkaOutputConfig := &kafkaOutputs[i]
			cluster, found := h.kafkaClusters[kafkaOutputConfig.Cluster]
			if !found {
				return nil, errors.New("not found kafka cluster for name: " + kafkaOutputConfig.Name)
			}
			backend, err := relay.NewKafkaBackend(kafkaOutputConfig, cluster)
			if err != nil {
				return nil, err
			}
			backends = append(backends, backend)
			h.kafkaOutputs[backend.Name()] = backend
		}

		for _, d := range kafkaDatabases {
			var bs []relay.Poster
			for _, o := range d.Outputs {
				for _, b := range backends {
					if o == b.Name() {
						bs = append(bs, b)
						break
					}
				}
			}
			h.kafkaBackends[d.Name] = bs
		}
	}

	if h.UseKafkaInput {
		for _, kafkaCluster := range kafkaClusters {
			consumer, _ := relay.NewKafkaConsumer(h.KafkaGroup, h, kafkaCluster)
			h.kafkaConsumers[consumer.Name()] = consumer
		}
		for _, o := range kafkaOutputs {
			if consumer, found := h.kafkaConsumers[o.Cluster]; found {
				if err := consumer.AddTopic(o); err != nil {
					log.Warnw("add topic failed", "topic", o.Topic, "cluster", o.Cluster, "error", err)
				}
			}
		}
		for cluster, consumer := range h.kafkaConsumers {
			if err := consumer.Open(); err != nil {
				log.Warnw("open consumer failed", "cluster", cluster)
			}
		}
	}

	go func() {
		//do watch
	WATCH:
		watcher := cli.Watch(context.Background(), "/"+cfg.Name, clientv3.WithPrefix(), clientv3.WithRev(rep.Header.Revision+1))
		for {
			select {
			case wresp, more := <-watcher:
				if !more {
					goto WATCH
				}
				for _, ev := range wresp.Events {
					log.Infof("%s %q : %q", ev.Type, ev.Kv.Key, ev.Kv.Value)
					key := string(ev.Kv.Key)
					value := ev.Kv.Value
					typo := ev.Type
					switch {
					case strings.HasPrefix(key, "/"+cfg.Name+"/kafka_cluster"):
						switch typo {
						case mvccpb.DELETE:
							name := key[len("/"+cfg.Name+"/kafka_cluster"):]
							if err := h.RemoveKafkaCluster(name); err != nil {
								log.Error(err)
							}
						case mvccpb.PUT:
							cluster := relay.KafkaClusterConfig{}
							if err := json.Unmarshal(value, &cluster); err != nil {
								log.Error(err)
								continue
							}
							if ev.IsCreate() {
								if err := h.AddKafkaCluster(cluster); err != nil {
									log.Error(err)
								}
							} else if ev.IsModify() {
								if err := h.ModifyKafkaCluster(cluster); err != nil {
									log.Error(err)
								}
							}
						}
					case strings.HasPrefix(key, "/"+cfg.Name+"/kafka_output"):
						switch typo {
						case mvccpb.DELETE:
							name := key[len("/"+cfg.Name+"/kafka_output"):]
							if err := h.RemoveKafkaOutput(name); err != nil {
								log.Error(err)
							}
						case mvccpb.PUT:
							output := relay.KafkaOutputConfig{}
							if err := json.Unmarshal(value, &output); err != nil {
								log.Error(err)
								continue
							}
							if ev.IsCreate() {
								if err := h.AddKafkaOutput(output); err != nil {
									log.Error(err)
								}
							} else if ev.IsModify() {
								if err := h.ModifyKafkaOutput(output); err != nil {
									log.Error(err)
								}
							}
						}
					case strings.HasPrefix(key, "/"+cfg.Name+"/kafka_database"):
						switch typo {
						case mvccpb.DELETE:
							name := key[len("/"+cfg.Name+"/kafka_database/"):]
							if err := h.RemoveKafkaDatabase(name); err != nil {
								log.Error(err)
							}
						case mvccpb.PUT:
							database := relay.DatabaseConfig{}
							if err := json.Unmarshal(value, &database); err != nil {
								log.Error(err)
								continue
							}
							if ev.IsCreate() {
								if err := h.AddKafkaDatabase(database); err != nil {
									log.Error(err)
								}
							} else if ev.IsModify() {
								if err := h.ModifyKafkaDatabase(database); err != nil {
									log.Error(err)
								}
							}
						}
					case strings.HasPrefix(key, "/"+cfg.Name+"/output/"):
						switch typo {
						case mvccpb.DELETE:
							name := key[len("/"+cfg.Name+"/output/"):]
							h.RemoveOutput(name)
						case mvccpb.PUT:
							output := relay.HTTPOutputConfig{}
							if err := json.Unmarshal(value, &output); err != nil {
								log.Error(err)
								continue
							}
							if ev.IsCreate() {
								h.AddOutput(output)
							} else if ev.IsModify() {
								h.ModifyOutput(output)
							}
						}
					case strings.HasPrefix(key, "/"+cfg.Name+"/database"):
						switch typo {
						case mvccpb.DELETE:
							name := key[len("/"+cfg.Name+"/database/"):]
							h.RemoveDatabase(name)
						case mvccpb.PUT:
							database := relay.DatabaseConfig{}
							if err := json.Unmarshal(value, &database); err != nil {
								log.Error(err)
								continue
							}
							if ev.IsCreate() {
								h.AddDatabase(database)
							} else if ev.IsModify() {
								h.ModifyDatabase(database)
							}
						}
					}

				}
			}
		}
	}()

	return h, nil
}

func (h *HTTP) AddOutput(outputConfig relay.HTTPOutputConfig) error {
	h.locker.Lock()
	defer h.locker.Unlock()
	name := outputConfig.Name
	if name == "" {
		return errors.New("name should not be empty")
	}
	if _, found := h.outputs[name]; found {
		return errors.New("duplicated name")
	}
	backend, err := relay.NewHTTPBackend(h.cfg.DataDir, &outputConfig, h.registry)
	if err != nil {
		return err
	}
	healthChecker := relay.NewHealthChecker(backend)
	h.outputs[name] = backend
	if outputConfig.Global {
		h.globalOutputs[name] = backend
	}
	h.healthCheckers[name] = healthChecker
	healthChecker.Run()
	return nil
}

func (h *HTTP) ModifyOutput(outputConfig relay.HTTPOutputConfig) {
	h.outputs[outputConfig.Name].(*relay.HttpBackend).UpdateConfig(outputConfig)
}

func (h *HTTP) RemoveOutput(name string) {
	// TODO: refine
}

func (h *HTTP) AddDatabase(databaseConfig relay.DatabaseConfig) error {
	h.locker.Lock()
	defer h.locker.Unlock()
	name := databaseConfig.Name
	if name == "" {
		return errors.New("name should not be empty")
	}
	if _, found := h.backends[name]; found {
		return errors.New("duplicated name \"" + name + "\"")
	}
	bs := make([]relay.Poster, 0)
	for _, o := range databaseConfig.Outputs {
		b, found := h.outputs[o]
		if found {
			bs = append(bs, b)
		}
	}
	h.backends[name] = bs
	return nil
}

func (h *HTTP) ModifyDatabase(databaseConfig relay.DatabaseConfig) error {
	h.locker.Lock()
	defer h.locker.Unlock()
	name := databaseConfig.Name
	if name == "" {
		return errors.New("name should not be empty")
	}
	if _, found := h.backends[name]; !found {
		return errors.New("not found name \"" + name + "\"")
	}
	bs := make([]relay.Poster, 0)
	for _, o := range databaseConfig.Outputs {
		b, found := h.outputs[o]
		if found {
			bs = append(bs, b)
		}
	}
	h.backends[name] = bs
	return nil
}

func (h *HTTP) RemoveDatabase(name string) {
	// TODO: refine
}

func (h *HTTP) AddKafkaCluster(kafkaClusterConfig relay.KafkaClusterConfig) error {
	h.locker.Lock()
	defer h.locker.Unlock()
	name := kafkaClusterConfig.Name
	if name == "" {
		return errors.New("name should not be empty")
	}
	if _, found := h.kafkaClusters[name]; found {
		return errors.New("duplicated name")
	}
	kafka, err := relay.NewKafkaCluster(kafkaClusterConfig)
	if err != nil {
		return err
	}
	h.kafkaClusters[kafkaClusterConfig.Name] = kafka
	consumer, _ := relay.NewKafkaConsumer(h.KafkaGroup, h, kafkaClusterConfig)
	h.kafkaConsumers[kafkaClusterConfig.Name] = consumer
	if err := consumer.Open(); err != nil {
		log.Warnw("open consumer failed", "error", err)
	}
	return nil
}

func (h *HTTP) ModifyKafkaCluster(kafkaClusterConfig relay.KafkaClusterConfig) error {
	return errors.New("modify kafka cluster config is not supported yet")
}

func (h *HTTP) RemoveKafkaCluster(name string) error {
	return errors.New("delete kafka cluster is not supported yet")
}

func (h *HTTP) AddKafkaOutput(outputConfig relay.KafkaOutputConfig) error {
	h.locker.Lock()
	defer h.locker.Unlock()
	name := outputConfig.Name
	if name == "" {
		return errors.New("name should not be empty")
	}
	if _, found := h.outputs[name]; found {
		return errors.New("duplicated name")
	}
	cluster, found := h.kafkaClusters[outputConfig.Cluster]
	if !found {
		return errors.New("not found kafka cluster")
	}
	if err := cluster.Open(); err != nil {
		return errors.Wrap(err, "cluster open failed")
	}
	backend, err := relay.NewKafkaBackend(&outputConfig, cluster)
	if err != nil {
		return err
	}
	h.kafkaOutputs[name] = backend
	consumer, found := h.kafkaConsumers[outputConfig.Cluster]
	if !found {
		return errors.New("not found kafka consumer")
	}
	if err := consumer.AddTopic(outputConfig); err != nil {
		return errors.Wrap(err, "add topic failed")
	}
	return nil
}

func (h *HTTP) ModifyKafkaOutput(outputConfig relay.KafkaOutputConfig) error {
	return errors.New("modify kafka output is not supported yet")
}

func (h *HTTP) RemoveKafkaOutput(name string) error {
	return errors.New("delete kafka output is not supported yet")
}

func (h *HTTP) AddKafkaDatabase(databaseConfig relay.DatabaseConfig) error {
	if !h.UseKafkaOutput {
		return nil
	}
	if !h.UseKafkaInput {
		return nil
	}
	h.locker.Lock()
	defer h.locker.Unlock()
	name := databaseConfig.Name
	if name == "" {
		return errors.New("name should not be empty")
	}
	if _, found := h.kafkaBackends[name]; found {
		return errors.New("duplicated name \"" + name + "\"")
	}
	bs := make([]relay.Poster, 0)
	for _, o := range databaseConfig.Outputs {
		b, found := h.kafkaOutputs[o]
		if found {
			bs = append(bs, b)
		}
	}
	h.kafkaBackends[name] = bs
	return nil
}

func (h *HTTP) ModifyKafkaDatabase(databaseConfig relay.DatabaseConfig) error {
	if !h.UseKafkaOutput {
		return nil
	}
	if !h.UseKafkaInput {
		return nil
	}
	h.locker.Lock()
	defer h.locker.Unlock()
	name := databaseConfig.Name
	if name == "" {
		return errors.New("name should not be empty")
	}
	if _, found := h.kafkaBackends[name]; !found {
		return errors.New("not found name \"" + name + "\"")
	}
	bs := make([]relay.Poster, 0)
	for _, o := range databaseConfig.Outputs {
		b, found := h.kafkaOutputs[o]
		if found {
			bs = append(bs, b)
		}
	}
	h.kafkaBackends[name] = bs
	return nil
}

func (h *HTTP) RemoveKafkaDatabase(name string) error {
	return errors.New("delete kafka database is not supported yet")
}

func (h *HTTP) Name() string {
	if h.name == "" {
		return fmt.Sprintf("%s://%s", h.schema, h.addr)
	}
	return h.name
}

func (h *HTTP) Run() error {
	r := gin.New()
	log := logn.GetLogger("web")
	accessLog := logn.GetLogger("access")

	r.Use(NewLogger(accessLog), gin.Recovery())
	h.initRouter(r)
	srv := &http.Server{
		Addr:    h.addr,
		Handler: r,
	}
	if h.cert != "" {
		cert, err := tls.LoadX509KeyPair(h.cert, h.cert)
		if err != nil {
			return err
		}
		srv.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}
	go func() {
		// service connections
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	go func() {
		// do output metric
		ip, err := GuessIP()
		if err != nil {
			log.Fatal(err)
		}
		commonTags := map[string]string{"host": ip}
		tick := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-h.done:
				return
			case now := <-tick.C:
				if h.metricDb == "" {
					continue
				}
				var points []models.Point
				h.registry.Each(func(s string, i interface{}) {
					m := relay.MeasurementKey(s)
					measurement := m.Measurement()
					tags := m.Tags()
					if tags == nil {
						tags = map[string]string{}
					}
					for k, v := range commonTags {
						tags[k] = v
					}
					switch i := i.(type) {
					case metrics.Counter:
						p, err := models.NewPoint(measurement, models.NewTags(tags), models.Fields{"value": i.Count()}, now)
						if err != nil {
							return
						}
						points = append(points, p)
						i.Clear()
					case metrics.Gauge:
						p, err := models.NewPoint(measurement, models.NewTags(tags), models.Fields{"value": i.Value()}, now)
						if err != nil {
							return
						}
						points = append(points, p)
					case metrics.GaugeFloat64:
						p, err := models.NewPoint(measurement, models.NewTags(tags), models.Fields{"value": i.Value()}, now)
						if err != nil {
							return
						}
						points = append(points, p)
					case metrics.Histogram:
						snap := i.Snapshot()
						p, err := models.NewPoint(measurement, models.NewTags(tags), models.Fields{
							"count":    snap.Count(),
							"max":      snap.Max(),
							"min":      snap.Min(),
							"mean":     snap.Mean(),
							"stdDev":   snap.StdDev(),
							"variance": snap.Variance(),
							"p99":      snap.Percentile(0.99),
							"p999":     snap.Percentile(0.999),
							"p95":      snap.Percentile(0.95),
							"middle":   snap.Percentile(0.5),
						}, now)
						if err != nil {
							return
						}
						points = append(points, p)
						i.Clear()
					case metrics.Meter:
						snap := i.Snapshot()
						p, err := models.NewPoint(measurement, models.NewTags(tags), models.Fields{
							"count":    snap.Count(),
							"rate1":    snap.Rate1(),
							"rate5":    snap.Rate5(),
							"rate15":   snap.Rate15(),
							"rateMean": snap.RateMean(),
						}, now)
						if err != nil {
							return
						}
						points = append(points, p)
					case metrics.Timer:
						snap := i.Snapshot()
						p, err := models.NewPoint(measurement, models.NewTags(tags), models.Fields{
							"count":    snap.Count(),
							"max":      snap.Max(),
							"min":      snap.Min(),
							"mean":     snap.Mean(),
							"stdDev":   snap.StdDev(),
							"variance": snap.Variance(),
							"p99":      snap.Percentile(0.99),
							"p999":     snap.Percentile(0.999),
							"p95":      snap.Percentile(0.95),
							"middle":   snap.Percentile(0.5),
							"rate1":    snap.Rate1(),
							"rate5":    snap.Rate5(),
							"rate15":   snap.Rate15(),
							"rateMean": snap.RateMean(),
						}, now)
						if err != nil {
							return
						}
						points = append(points, p)
					}
				})
				b := getBuf()
				for _, p := range points {
					b.WriteString(p.PrecisionString("ns"))
					b.WriteByte('\n')
					if b.Len() > 10*1024 {
						w := httptest.NewRecorder()
						req, _ := http.NewRequest("POST", "/write?db="+h.metricDb, b)
						r.ServeHTTP(w, req)
						if w.Result().StatusCode != 204 {
							log.Warnw("write metrics failed", "error", w.Body.String())
						}
						b.Reset()
					}
				}
				if b.Len() > 0 {
					w := httptest.NewRecorder()
					req, _ := http.NewRequest("POST", "/write?db="+h.metricDb, b)
					r.ServeHTTP(w, req)
					if w.Result().StatusCode != 204 {
						log.Warnw("write metrics failed", "error", w.Body.String())
					}
					b.Reset()
				}
				putBuf(b)
			}
		}

	}()
	return nil
}

func (h *HTTP) Stop() error {
	log.Info("stop")
	if h.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := h.srv.Shutdown(ctx); err != nil {
			return errors.New(fmt.Sprintf("server shutdown: %s", err))
		}
	}
	return nil
}

func (h *HTTP) resolveBackends(db string, useKafkaOutput bool) ([]relay.Poster, error) {
	h.locker.RLock()
	defer h.locker.RUnlock()
	var backends []relay.Poster
	if useKafkaOutput {
		backends, _ = h.kafkaBackends[db]
	} else {
		backends, _ = h.backends[db]
		for _, b := range h.globalOutputs {
			backends = append(backends, b)
		}
	}
	if len(backends) == 0 {
		return nil, errors.New("database not found: \"" + db + "\"")
	}
	return backends, nil
}

func (h *HTTP) Process(msg *sarama.ConsumerMessage, protocol relay.Protocol) error {
	query, auth, body, err := protocol.Decode(msg)
	if err != nil {
		log.Warnw("protocol decode failed", "error", err)
		return nil
	}

	if query == "" {
		log.Warnw("kafka message bad headers, no query head", "topic", msg.Topic)
		return nil
	}

	outBytes := body

	queryParams, err := url.ParseQuery(query)
	if err != nil {
		log.Warnw("kafka message bad header, query head parse failed", "topic", msg.Topic, "query", query)
		return nil
	}

	db := queryParams.Get("db")
	if db == "" {
		log.Warnw("kafka message bad header, query no db", "topic", msg.Topic, "query", query)
		return nil
	}

	bs, err := h.resolveBackends(db, false)
	if err != nil {
		log.Warn("resolve backends failed", "error", err)
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(bs))

	var responses = make(chan *relay.ResponseData, len(bs))

	for _, b := range bs {
		b := b
		go func() {
			defer wg.Done()
			resp, err := b.Post(outBytes, query, auth)
			if err != nil {
				log.Errorf("Problem posting to relay %q backend %+v: %v", h.Name(), b, err)
			} else {
				if resp.StatusCode/100 == 5 {
					log.Infof("5xx response for relay %q backend %+v: %v", h.Name(), b, resp.StatusCode)
				}
				responses <- resp
			}
		}()
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	var errResponse *relay.ResponseData

	for resp := range responses {
		switch resp.StatusCode / 100 {
		case 2:
			return nil

		case 4:
			// user error
			log.Warnf("user error %d", resp.StatusCode)
			return nil

		default:
			// hold on to one of the responses to return back to the client
			log.Warnf("some error %d", resp.StatusCode)
			errResponse = resp
		}
	}

	// no successful writes
	if errResponse == nil {
		// failed to make any valid request...
		return errors.New("unable to write points")
	}

	return nil
}

func (h *HTTP) ShouldRetry(_ error, fail int) bool {
	backoff := fail * 20
	if backoff > 100 {
		backoff = 100
	}
	time.Sleep(time.Millisecond * time.Duration(backoff))
	return fail < math.MaxInt64
}

func (h *HTTP) initRouter(r gin.IRouter) {
	t := reflect.TypeOf(h)
	n := t.NumMethod()
	for i := 0; i < n; i++ {
		m := t.Method(i)
		if strings.HasPrefix(m.Name, "Init") && strings.HasSuffix(m.Name, "Router") {
			log.Infof("call %q", m.Name)
			m.Func.Call([]reflect.Value{reflect.ValueOf(h), reflect.ValueOf(r)})
		}
	}
}

func ifElse(c bool, t string, f string) string {
	if c {
		return t
	} else {
		return f
	}
}

func GuessIP() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("no hostname", err)
	}
	ips, err := net.LookupIP(hostname)
	if err != nil {
		log.Fatal("lookup hostname failed", err)
	}
	for _, ip := range ips {
		if ip.To4() != nil {
			return ip.String(), nil
		}
	}
	return "", errors.New("cannot resolve ip by hostname")
}
