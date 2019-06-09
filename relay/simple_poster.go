package relay

import (
	"bytes"
	"crypto/tls"
	"github.com/influxdata/influxdb/models"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type simplePoster struct {
	name     string
	client   *http.Client
	location string
	registry metrics.Registry
}

func NewSimplePoster(name string, writeEndpoint string, location string, timeout time.Duration, skipTLSVerification bool, registry metrics.Registry) *simplePoster {
	// Configure custom transport for http.Client
	// Used for support skip-tls-verification option
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: skipTLSVerification,
		},
	}

	location = location + "/write"
	if writeEndpoint != "" {
		location = writeEndpoint
	}

	return &simplePoster{
		name: name,
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		location: location,
		registry: registry,
	}
}

func (b *simplePoster) doMetric(buf []byte, query string) error {
	values, err := url.ParseQuery(query)
	if err != nil {
		return err
	}
	db := values.Get("db")
	if db == "" {
		return errors.New("db is empty")
	}
	precision := values.Get("precision")
	if precision == "" {
		precision = "ns"
	}
	points, err := models.ParsePointsWithPrecision(buf, time.Now(), precision)
	if err != nil {
		return err
	}

	fieldCount := 0
	now := time.Now()
	histogram := metrics.GetOrRegisterHistogram(
		string(NewMeasurementKey("latency", "name", b.name, "db", db)),
		b.registry,
		metrics.NewExpDecaySample(1028, 0.015),
	)
	for _, p := range points {
		fs, _ := p.Fields()
		fieldCount += len(fs)
		histogram.Update(int64(now.Sub(p.Time())))
	}
	if fieldCount > 0 {
		metrics.GetOrRegisterMeter(
			string(NewMeasurementKey("meter", "name", b.name, "db", db)),
			b.registry,
		).Mark(int64(fieldCount))
	}
	return nil
}

func (b *simplePoster) Post(buf []byte, query string, auth string) (*ResponseData, error) {
	if err := b.doMetric(buf, query); err != nil {
		log.Warnw("do metric failed", "query", query, "error", err)
	}
	req, err := http.NewRequest("POST", b.location, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	req.URL.RawQuery = query
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err = resp.Body.Close(); err != nil {
		return nil, err
	}

	return &ResponseData{
		ContentType:     resp.Header.Get("Conent-Type"),
		ContentEncoding: resp.Header.Get("Conent-Encoding"),
		StatusCode:      resp.StatusCode,
		Body:            data,
	}, nil
}
