package gin

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/shanexu/influxdb-balancer/relay"
)

func (h *HTTP) InitHealthRouter(r gin.IRouter) {
	func(r gin.IRouter) {
		r.GET("/outputs", func(ctx *gin.Context) {
			h.locker.RLock()
			defer h.locker.RUnlock()
			for name, o := range h.outputs {
				o := o.(*relay.HttpBackend)
				ctx.Writer.Write([]byte(fmt.Sprintf("%s is %s\n", name, ifElse(o.Alive(), "up", "down"))))
			}
			return
		})
		r.GET("/databases", func(ctx *gin.Context) {
			h.locker.RLock()
			defer h.locker.RUnlock()
			for d, bs := range h.backends {
				ctx.Writer.Write([]byte(d))
				ctx.Writer.Write([]byte{'\n'})
				for _, b := range bs {
					b := b.(*relay.HttpBackend)
					ctx.Writer.Write([]byte(fmt.Sprintf("%s is %s\n", b.Name(), ifElse(b.Alive(), "up", "down"))))
				}

			}
		})
	}(r.Group("/health"))
}
