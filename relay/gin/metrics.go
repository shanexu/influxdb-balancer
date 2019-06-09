package gin

import (
	"github.com/gin-gonic/gin"
	"github.com/rcrowley/go-metrics"
)

func (h *HTTP) InitMetricsRouter(r gin.IRouter) {

	GetMetrics := func(ctx *gin.Context) {

		snapshot := make(map[string]interface{})

		h.registry.Each(func(s string, i interface{}) {
			switch r := i.(type) {
			case metrics.Counter:
				snapshot[s+" value"] = r.Count()
			case metrics.Gauge:
				snapshot[s+" value"] = r.Value()
			case metrics.GaugeFloat64:
				snapshot[s+" value"] = r.Value()
			case metrics.Histogram:
				snap := r.Snapshot()
				snapshot[s+" count"] = snap.Count()
				snapshot[s+" max"] = snap.Max()
				snapshot[s+" min"] = snap.Min()
				snapshot[s+" mean"] = snap.Mean()
				snapshot[s+" stdDev"] = snap.StdDev()
				snapshot[s+" variance"] = snap.Variance()
				snapshot[s+" p99"] = snap.Percentile(0.99)
				snapshot[s+" p999"] = snap.Percentile(0.999)
				snapshot[s+" p95"] = snap.Percentile(0.95)
				snapshot[s+" middle"] = snap.Percentile(0.5)
			case metrics.Meter:
				snap := r.Snapshot()
				snapshot[s+" rate1"] = snap.Rate1()
				snapshot[s+" rate5"] = snap.Rate5()
				snapshot[s+" rate15"] = snap.Rate15()
				snapshot[s+" rateMean"] = snap.RateMean()
				snapshot[s+" count"] = snap.Count()
			case metrics.Timer:
				snap := r.Snapshot()
				snapshot[s+" count"] = snap.Count()
				snapshot[s+" max"] = snap.Max()
				snapshot[s+" min"] = snap.Min()
				snapshot[s+" mean"] = snap.Mean()
				snapshot[s+" stdDev"] = snap.StdDev()
				snapshot[s+" variance"] = snap.Variance()
				snapshot[s+" p99"] = snap.Percentile(0.99)
				snapshot[s+" p999"] = snap.Percentile(0.999)
				snapshot[s+" p95"] = snap.Percentile(0.95)
				snapshot[s+" middle"] = snap.Percentile(0.5)
				snapshot[s+" rate1"] = snap.Rate1()
				snapshot[s+" rate5"] = snap.Rate5()
				snapshot[s+" rate15"] = snap.Rate15()
				snapshot[s+" rateMean"] = snap.RateMean()
			}
		})

		SetResult(ctx, snapshot)
	}

	func(r gin.IRouter) {
		r.Use(JSONApi)
		r.GET("", GetMetrics)
	}(r.Group("/metrics"))
}
