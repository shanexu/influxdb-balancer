package gin

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (h *HTTP) InitPingRouter(r gin.IRouter) {
	r.Any("/ping", func(ctx *gin.Context) {
		ctx.Header("Content-Type", "application/json")
		ctx.Header("X-Influxdb-Build", "OSS")
		ctx.Header("X-Influxdb-Version", "v1.7.2")
		ctx.Status(http.StatusNoContent)
	})
}
