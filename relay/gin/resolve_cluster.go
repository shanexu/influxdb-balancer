package gin

import (
	"context"
	"github.com/pkg/errors"
	"math"
	"strings"
	"sync"

	"gopkg.in/resty.v1"

	"github.com/shanexu/influxdb-balancer/relay"
	"github.com/shanexu/influxdb-balancer/retry"
)

func (h *HTTP) fetchBalancers(ctx context.Context) ([]string, error) {
	result, err := h.cli.Cluster.MemberList(ctx)
	if err != nil {
		return nil, err
	}

	port := strings.Split(h.addr, ":")[1]
	balancers := make([]string, 0)
	for _, v := range result.Members {
		if len(v.ClientURLs) == 0 {
			continue
		}
		e := v.ClientURLs[0]
		idx := strings.LastIndex(e, ":")
		if idx != -1 {
			e = e[0:idx]
		}
		balancers = append(balancers, e+":"+port)
	}

	return balancers, nil
}

func (h *HTTP) fetchMetrics(balancers []string) (map[string]float64, error) {
	rates := make(map[string]float64)
	es := make([]error, len(balancers))
	var wg sync.WaitGroup
	var lock sync.Mutex
	wg.Add(len(balancers))
	for i, b := range balancers {
		b := b
		i := i
		go func() {
			defer wg.Done()
			metrics := make(map[string]float64)
			if err := retry.NewSimpleRetry(3).Execute(func() error {
				_, err := resty.R().SetResult(&metrics).Get(b + "/metrics")
				return err
			}); err != nil {
				es[i] = err
			}
			lock.Lock()
			defer lock.Unlock()
			for k, v := range metrics {
				if !strings.HasSuffix(k, ".meter.rate15") {
					continue
				}
				parts := strings.Split(k, ".")
				db := parts[len(parts)-3]
				rates[db] = rates[db] + v
			}
		}()
	}
	wg.Wait()
	for _, e := range es {
		if e != nil {
			return nil, e
		}
	}
	return rates, nil
}

type ClusterMetric struct {
	DbCount int     `json:"db_count"`
	Rate    float64 `json:"rate"`
}

func (cm1 *ClusterMetric) Less(cm2 *ClusterMetric) bool {
	if cm1 == nil {
		return false
	}
	if cm2 == nil {
		return true
	}
	if cm1.Rate == 0 && cm2.Rate == 0 {
		return cm1.DbCount < cm2.DbCount
	}
	if math.Abs(cm1.Rate - cm2.Rate) / (cm1.Rate + cm2.Rate) * 2 < 0.01 {
		return cm1.DbCount < cm2.DbCount
	}
	return cm1.Rate < cm2.Rate
}

func outputToCluster(name string) string {
	return strings.TrimRight(name, "0123456789")
}

func (h *HTTP) ClusterMetrics(ctx context.Context) (map[string]*ClusterMetric, error) {
	balancers, err := h.fetchBalancers(ctx)
	if err != nil {
		return nil, err
	}
	metrics, err := h.fetchMetrics(balancers)
	if err != nil {
		return nil, err
	}
	outputMetrics := make(map[string]float64)
	clusterMetrics := make(map[string]*ClusterMetric)
	h.locker.RLock()
	defer h.locker.RUnlock()
	for _, bi := range h.outputs {
		b, ok := bi.(*relay.HttpBackend)
		if !ok {
			continue
		}
		clusterMetrics[strings.TrimRight(b.Name(), "0123456789")] = &ClusterMetric{}
	}
	for _, bs := range h.backends {
		for _, bi := range bs {
			b, ok := bi.(*relay.HttpBackend)
			if !ok {
				continue
			}
			cm, found := clusterMetrics[outputToCluster(b.Name())]
			if !found {
				continue
			}
			cm.DbCount += 1
		}
	}

	for db, value := range metrics {
		bs, _ := h.backends[db]
		for _, bi := range bs {
			b, ok := bi.(*relay.HttpBackend)
			if !ok {
				continue
			}
			outputMetrics[b.Name()] += value
		}
	}

	for name, value := range outputMetrics {
		name = outputToCluster(name)
		cm, found := clusterMetrics[name]
		if !found {
			continue
		}
		cm.Rate += value
	}
	return clusterMetrics, nil
}

func (h *HTTP) ResolveCluster(ctx context.Context, db string, owner string) (string, error) {
	clusterMetrics, err := h.ClusterMetrics(ctx)
	if err != nil {
		return "", err
	}

	cluster := ""
	var value *ClusterMetric
	for k, v := range clusterMetrics {
		if v.Less(value) {
			cluster = k
			value = v
		}
	}

	if cluster == "" {
		return "", errors.New("cluster not resolved")
	}

	return cluster, nil
}
