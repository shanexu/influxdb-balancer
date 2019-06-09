package gin

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func SetResult(c *gin.Context, result interface{}) {
	c.Set("result", result)
}

func GetResult(c *gin.Context) (interface{}, bool) {
	return c.Get("result")
}

func GetStatusCode(c *gin.Context) (int, bool) {
	_, exists := c.Get("status_code")
	return c.GetInt("status_code"), exists
}

func SetStatusCode(c *gin.Context, statusCode int) {
	c.Set("status_code", statusCode)
}

func JSONApi(c *gin.Context) {
	c.Next()

	statusCode, _ := GetStatusCode(c)

	if len(c.Errors) > 0 {
		if statusCode == 0 {
			statusCode = http.StatusInternalServerError
		}
		c.JSON(statusCode, c.Errors.JSON())
		return
	}

	result, _ := GetResult(c)
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	c.JSON(statusCode, result)
}
