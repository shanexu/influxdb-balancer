package gin

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/gin-gonic/gin"
	"github.com/shanexu/influxdb-balancer/relay"
	"net/http"
)

func (h *HTTP) InitKafkaConfigRouter(r gin.IRouter) {

	RemoveCluster := func(ctx *gin.Context) {
		name := ctx.Param("name")
		if name == "" {
			SetStatusCode(ctx, http.StatusBadRequest)
			ctx.Error(errors.New("name should not be empty"))
			return
		}
		_, err := h.cli.Delete(ctx.Request.Context(), "/"+h.name+"/kafka_cluster/"+name)
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	SaveCluster := func(ctx *gin.Context) {
		data, err := ctx.GetRawData()
		if err != nil {
			ctx.Error(err)
			return
		}
		kc := relay.KafkaClusterConfig{}
		if err := json.Unmarshal(data, &kc); err != nil {
			ctx.Error(err)
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		name := kc.Name
		if name == "" {
			ctx.Error(errors.New("name should not be empty"))
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		data, _ = json.Marshal(kc)
		_, err = h.cli.Put(ctx.Request.Context(), "/"+h.name+"/kafka_cluster/"+name, string(data))
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	GetClusters := func(ctx *gin.Context) {
		rep, err := h.cli.Get(ctx.Request.Context(), "/"+h.name+"/kafka_cluster", clientv3.WithPrefix())
		if err != nil {
			ctx.Error(err)
			return
		}
		var clusters []relay.KafkaClusterConfig
		for i := range rep.Kvs {
			c := relay.KafkaClusterConfig{}
			if err := json.Unmarshal(rep.Kvs[i].Value, &c); err == nil {
				clusters = append(clusters, c)
			}
		}
		SetResult(ctx, clusters)
	}

	GetOutputs := func(ctx *gin.Context) {
		rep, err := h.cli.Get(ctx.Request.Context(), "/"+h.name+"/kafka_output", clientv3.WithPrefix())
		if err != nil {
			ctx.Error(err)
			return
		}
		var outputs []relay.KafkaOutputConfig
		for i := range rep.Kvs {
			c := relay.KafkaOutputConfig{}
			if e := json.Unmarshal(rep.Kvs[i].Value, &c); e == nil {
				outputs = append(outputs, c)
			}
		}
		SetResult(ctx, outputs)
	}

	SaveOutput := func(ctx *gin.Context) {
		data, err := ctx.GetRawData()
		if err != nil {
			ctx.Error(err)
			return
		}

		ko := relay.KafkaOutputConfig{}
		if err := json.Unmarshal(data, &ko); err != nil {
			ctx.Error(err)
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}

		name := ko.Name
		if name == "" {
			ctx.Error(errors.New("name should not be empty"))
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		data, _ = json.Marshal(ko)
		_, err = h.cli.Put(ctx.Request.Context(), "/"+h.name+"/kafka_output/"+name, string(data))
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	RemoveOutput := func(ctx *gin.Context) {
		name := ctx.Param("name")
		if name == "" {
			ctx.Error(errors.New("name should not be empty"))
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		if _, err := h.cli.Delete(ctx.Request.Context(), "/"+h.name+"/kafka_output/"+name); err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	GetDatabases := func(ctx *gin.Context) {
		rep, err := h.cli.Get(ctx.Request.Context(), "/"+h.name+"/kafka_database/", clientv3.WithPrefix())
		if err != nil {
			ctx.Error(err)
			return
		}
		var databases []relay.DatabaseConfig
		for i := range rep.Kvs {
			d := relay.DatabaseConfig{}
			if e := json.Unmarshal(rep.Kvs[i].Value, &d); e == nil {
				databases = append(databases, d)
			}
		}
		SetResult(ctx, databases)
	}

	SaveDatabase := func(ctx *gin.Context) {
		databaseConfig := relay.DatabaseConfig{}
		err := ctx.BindJSON(&databaseConfig)
		if err != nil {
			SetStatusCode(ctx, http.StatusBadRequest)
			ctx.Error(err)
			return
		}
		if err := h.saveKafkaConfigDatabase(ctx.Request.Context(), databaseConfig); err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	RemoveDatabase := func(ctx *gin.Context) {
		name := ctx.Param("name")
		if name == "" {
			ctx.Error(errors.New("name should not be empty"))
			SetStatusCode(ctx, http.StatusBadRequest)
			return
		}
		if _, err := h.cli.Delete(ctx.Request.Context(), "/"+h.name+"/kafka_database/"+name); err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	func(r gin.IRouter) {
		r.Use(JSONApi)
		func(r gin.IRouter) {
			r.GET("", GetClusters)
			r.DELETE("/:name", RemoveCluster)
			r.POST("", SaveCluster)
			r.PUT("", SaveCluster)
		}(r.Group("/clusters"))

		func(r gin.IRouter) {
			r.GET("", GetOutputs)
			r.DELETE("/:name", RemoveOutput)
			r.POST("", SaveOutput)
			r.PUT("", SaveOutput)
		}(r.Group("/outputs"))

		func(r gin.IRouter) {
			r.GET("", GetDatabases)
			r.DELETE("/:name", RemoveDatabase)
			r.POST("", SaveDatabase)
			r.PUT("", SaveDatabase)
		}(r.Group("/databases"))

	}(r.Group("/kafka_config"))
}

func (h *HTTP) saveKafkaConfigDatabase(ctx context.Context, databaseConfig relay.DatabaseConfig) error {
	name := databaseConfig.Name
	data, err := json.Marshal(databaseConfig)
	if err != nil {
		return err
	}
	_, err = h.cli.Put(ctx, "/"+h.name+"/kafka_database/"+name, string(data))
	return err
}
