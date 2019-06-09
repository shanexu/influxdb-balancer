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

func (h *HTTP) InitConfigRouter(r gin.IRouter) {

	GetOutputs := func(ctx *gin.Context) {
		rep, err := h.cli.Get(ctx.Request.Context(), "/"+h.name+"/output", clientv3.WithPrefix())
		if err != nil {
			ctx.Error(err)
			return
		}
		var outputs []relay.HTTPOutputConfig
		for i := range rep.Kvs {
			c := relay.HTTPOutputConfig{}
			if e := json.Unmarshal(rep.Kvs[i].Value, &c); e == nil {
				outputs = append(outputs, c)
			}
		}

		SetResult(ctx, outputs)
	}

	SaveOutput := func(ctx *gin.Context) {
		hc := relay.HTTPOutputConfig{}
		err := ctx.BindJSON(&hc)
		if err != nil {
			SetStatusCode(ctx, http.StatusBadRequest)
			ctx.Error(err)
			return
		}
		if hc.Name == "" {
			SetStatusCode(ctx, http.StatusBadRequest)
			ctx.Error(errors.New("name should not be empty"))
			return
		}
		data, _ := json.Marshal(hc)
		_, err = h.cli.Put(ctx.Request.Context(), "/"+h.name+"/output/"+hc.Name, string(data))
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	RemoveOutput := func(ctx *gin.Context) {
		name := ctx.Param("name")
		if name == "" {
			SetStatusCode(ctx, http.StatusBadRequest)
			ctx.Error(errors.New("name should not be empty"))
			return
		}
		_, err := h.cli.Delete(ctx.Request.Context(), "/"+h.name+"/output/"+name)
		if err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	GetDatabases := func(ctx *gin.Context) {
		rep, err := h.cli.Get(ctx.Request.Context(), "/"+h.name+"/database/", clientv3.WithPrefix())
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
		if err := h.saveConfigDatabase(ctx.Request.Context(), databaseConfig); err != nil {
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
		if _, err := h.cli.Delete(ctx.Request.Context(), "/"+h.name+"/database/"+name); err != nil {
			ctx.Error(err)
			return
		}
		SetResult(ctx, "ok")
	}

	func(r gin.IRouter) {

		r.Use(JSONApi)

		func(r gin.IRoutes) {
			r.GET("", GetOutputs)
			r.POST("", SaveOutput)
			r.PUT("", SaveOutput)
			r.DELETE("/:name", RemoveOutput)
		}(r.Group("/outputs"))

		func(r gin.IRouter) {
			r.GET("", GetDatabases)
			r.PUT("", SaveDatabase)
			r.POST("", SaveDatabase)
			r.DELETE("/:name", RemoveDatabase)
		}(r.Group("/databases"))

	}(r.Group("/config"))
}

func (h *HTTP) saveConfigDatabase(ctx context.Context, databaseConfig relay.DatabaseConfig) error {
	name := databaseConfig.Name
	data, err := json.Marshal(databaseConfig)
	if err != nil {
		return err
	}
	_, err = h.cli.Put(ctx, "/"+h.name+"/database/"+name, string(data))
	return err
}
