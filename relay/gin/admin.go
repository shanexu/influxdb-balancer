package gin

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"github.com/shanexu/influxdb-balancer/relay"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
)

type databaseConfig struct {
	Name          string `toml:"name" json:"name" binding:"required"`
	Owner         string `toml:"owner" json:"owner" binding:"required"`
	Duration      string `toml:"duration" json:"duration" binding:"required"`
	ShardDuration string `toml:"shard-duration" json:"shard-duration"`
	Cluster       string `toml:"cluster" json:"cluster"`
}

func (h *HTTP) InitAdminRouter(r gin.IRouter) {

	CreateDatabase := func(ctx *gin.Context) {
		dbCfg := databaseConfig{}
		if err := ctx.BindJSON(&dbCfg); err != nil {
			ctx.Error(err)
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		if _, exist := h.backends[dbCfg.Name]; exist {
			ctx.Error(errors.New("duplicated database name"))
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		if dbCfg.Cluster == "" {
			var err error
			dbCfg.Cluster, err = h.ResolveCluster(ctx.Request.Context(), dbCfg.Name, dbCfg.Owner)
			if err != nil {
				ctx.Error(err)
				return
			}
		}
		duration, err := influxql.ParseDuration(dbCfg.Duration)
		if err != nil {
			ctx.Error(err)
			return
		}
		if dbCfg.ShardDuration != "" {
			shardDuration, err := influxql.ParseDuration(dbCfg.ShardDuration)
			if err != nil {
				ctx.Error(err)
				return
			}
			if duration < shardDuration {
				ctx.Error(fmt.Errorf("duration %v is less than shard duration %v", duration, shardDuration))
				return
			}
		}

		outputs := make([]string, 0)
		for _, output := range h.outputs {
			b, ok := output.(*relay.HttpBackend)
			if !ok {
				continue
			}
			if strings.HasPrefix(b.Name(), dbCfg.Cluster) {
				outputs = append(outputs, b.Name())
			}
		}
		if len(outputs) == 0 {
			ctx.Error(fmt.Errorf("cannot resolve instances for cluster %v", dbCfg.Cluster))
			return
		}
		if err := h.saveConfigDatabase(ctx.Request.Context(), relay.DatabaseConfig{
			Name:    dbCfg.Name,
			Owner:   dbCfg.Owner,
			Outputs: outputs,
		}); err != nil {
			ctx.Error(err)
			return
		}
		if err := h.createDatabase(dbCfg, outputs); err != nil {
			ctx.Error(err)
			return
		}
		if err := h.saveKafkaConfigDatabase(ctx.Request.Context(), relay.DatabaseConfig{
			Name:    dbCfg.Name,
			Owner:   dbCfg.Owner,
			Outputs: []string{dbCfg.Cluster},
		}); err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	ResolveCluster := func(ctx *gin.Context) {
		type Request struct {
			Db    string `json:"db" form:"db"`
			Owner string `json:"owner" form:"owner"`
		}
		request := Request{}
		if err := ctx.Bind(&request); err != nil {
			ctx.Error(err)
			return
		}
		cluster, err := h.ResolveCluster(ctx.Request.Context(), request.Db, request.Owner)
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, cluster)
	}

	ClusterMetrics := func(ctx *gin.Context) {
		result, err := h.ClusterMetrics(ctx.Request.Context())
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, result)
	}

	ListNodes := func(ctx *gin.Context) {
		response, err := h.cli.Cluster.MemberList(ctx.Request.Context())
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, response.Members)
	}

	AddNode := func(ctx *gin.Context) {
		peerAddrs, ok := ctx.GetPostFormArray("peerAddrs")
		if !ok {
			ctx.Error(errors.New("peerAddrs is empty"))
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		response, err := h.cli.MemberAdd(ctx.Request.Context(), peerAddrs)
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, response.Members)
	}

	DeleteNode := func(ctx *gin.Context) {
		memberId, err := strconv.ParseUint(ctx.Param("member_id"), 10, 64)
		if err != nil {
			ctx.Error(err)
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		response, err := h.cli.MemberRemove(ctx.Request.Context(), memberId)
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, response.Members)
	}

	UpdateNode := func(ctx *gin.Context) {
		memberId, err := strconv.ParseUint(ctx.Param("member_id"), 10, 64)
		if err != nil {
			ctx.Error(err)
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		peerAddrs, ok := ctx.GetPostFormArray("peerAddrs")
		if !ok {
			ctx.Error(errors.New("peerAddrs is empty"))
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		response, err := h.cli.MemberUpdate(ctx.Request.Context(), memberId, peerAddrs)
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, response.Members)
	}

	func(r gin.IRouter) {
		r.Use(JSONApi)
		r.POST("/database", CreateDatabase)
		r.GET("/resolve_cluster", ResolveCluster)
		r.GET("/cluster_metrics", ClusterMetrics)
		r.GET("/nodes", ListNodes)
		r.POST("/nodes", AddNode)
		r.DELETE("/nodes/:member_id", DeleteNode)
		r.PUT("/nodes/:member_id", UpdateNode)
	}(r.Group("/admin"))
}

type HttpError struct {
	Code int
	Body string
}

func (e *HttpError) Error() string {
	return fmt.Sprintf("%d %s", e.Code, e.Body)
}

func (h *HTTP) createDatabase(dbCfg databaseConfig, outputs []string) error {
	backends := make([]*relay.HttpBackend, 0)
	for _, name := range outputs {
		bi, _ := h.outputs[name]
		if bi == nil {
			return fmt.Errorf("no output for name %v", name)
		}
		b, ok := bi.(*relay.HttpBackend)
		if !ok {
			return fmt.Errorf("output %v is not *relay.HttpBackend", name)
		}
		backends = append(backends, b)
	}

	var q string
	if dbCfg.ShardDuration == "" {
		q = fmt.Sprintf(`CREATE DATABASE "%s" WITH DURATION %s REPLICATION 1 NAME "default"`, dbCfg.Name, dbCfg.Duration)
	} else {
		q = fmt.Sprintf(`CREATE DATABASE "%s" WITH DURATION %s REPLICATION 1 SHARD DURATION %s NAME "default"`, dbCfg.Name, dbCfg.Duration, dbCfg.ShardDuration)
	}
	values := make(url.Values)
	values.Add("q", q)
	bodyString := values.Encode()
	bodyReader := bytes.NewReader([]byte(bodyString))
	request, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:8088/query", bodyReader)
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	for _, b := range backends {
		if b.ReverseProxy() == nil {
			continue
		}
		bodyReader.Seek(0, 0)
		request.Host = b.Host()
		recorder := httptest.NewRecorder()
		b.ReverseProxy().ServeHTTP(recorder, request)
		if recorder.Code/100 != 2 {
			// copy error response
			data, _ := ioutil.ReadAll(recorder.Body)
			return &HttpError{recorder.Code, string(data)}
		}
	}
	return nil
}
