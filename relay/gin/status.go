package gin

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (h *HTTP) InitStatusRouter(r gin.IRouter) {
	GetOk := func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	}

	func(r gin.IRouter) {
		r.Any("/ok", GetOk)
		r.Any("/ok.htm", GetOk)
		r.Any("/ok.html", GetOk)
	}(r)
}
