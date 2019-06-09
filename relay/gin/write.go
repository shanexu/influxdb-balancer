package gin

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/influxdata/influxdb/models"
	"github.com/shanexu/influxdb-balancer/relay"
)

func jsonError(ctx *gin.Context, status int, err string) {
	ctx.JSON(status, gin.H{
		"error": err,
	})
}

var bufPool = sync.Pool{New: func() interface{} {
	return new(bytes.Buffer)
}}

func getBuf() *bytes.Buffer {
	if bb, ok := bufPool.Get().(*bytes.Buffer); ok {
		return bb
	}
	return new(bytes.Buffer)
}

func putBuf(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}

func (h *HTTP) InitWriteRouter(r gin.IRouter) {

	func(r gin.IRouter) {
		r.Any("", func(ctx *gin.Context) {

			start := time.Now()

			if ctx.Request.Method != http.MethodPost {
				ctx.Header("Allow", http.MethodPost)
				if ctx.Request.Method == "OPTIONS" {
					ctx.Status(http.StatusNoContent)
				} else {
					jsonError(ctx, http.StatusMethodNotAllowed, "invalid write method")
				}
				return
			}

			queryParams := ctx.Request.URL.Query()
			// fail early if we're missing the database
			db := queryParams.Get("db")
			if db == "" {
				jsonError(ctx, http.StatusBadRequest, `missing parameter: "db"`)
				log.Warn(`missing parameter: "db"`)
				return
			}

			if queryParams.Get("rp") == "" && h.rp != "" {
				queryParams.Set("rp", h.rp)
			}

			body := ctx.Request.Body

			if ctx.GetHeader("Content-Encoding") == "gzip" {
				b, err := gzip.NewReader(body)
				if err != nil {
					jsonError(ctx, http.StatusBadRequest, "unable to decode gzip body")
					log.Warnw("unable to decode gzip body", "db", db)
				}
				defer b.Close()
				body = b
			}

			bodyBuf := getBuf()
			_, err := bodyBuf.ReadFrom(body)
			if err != nil {
				putBuf(bodyBuf)
				jsonError(ctx, http.StatusInternalServerError, "problem reading request body")
				return
			}

			precision := queryParams.Get("precision")
			points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
			if err != nil {
				jsonError(ctx, http.StatusBadRequest, "unable to parse points")
				log.Warnw("unable to parse points", "db", db, "error", err, "body", fmt.Sprintf("%x", bodyBuf.Bytes()))
				putBuf(bodyBuf)
				return
			}

			outBuf := getBuf()
			for _, p := range points {
				if _, err = outBuf.WriteString(p.PrecisionString(precision)); err != nil {
					break
				}
				if err = outBuf.WriteByte('\n'); err != nil {
					break
				}
			}
			// done with the input points
			putBuf(bodyBuf)

			if err != nil {
				putBuf(outBuf)
				jsonError(ctx, http.StatusInternalServerError, "problem writing points")
				return
			}

			bs, err := h.resolveBackends(db, h.UseKafkaOutput)
			if err != nil {
				putBuf(outBuf)
				jsonError(ctx, http.StatusNotFound, "database not found: \""+db+"\"")
				return
			}

			// normalize query string
			query := queryParams.Encode()

			outBytes := outBuf.Bytes()

			// check for authorization performed via the header
			authHeader := ctx.GetHeader("Authorization")

			var wg sync.WaitGroup
			wg.Add(len(bs))

			var responses = make(chan *relay.ResponseData, len(bs))

			for _, b := range bs {
				b := b
				go func() {
					defer wg.Done()
					resp, err := b.Post(outBytes, query, authHeader)
					if err != nil {
						log.Infof("Problem posting to relay %q backend %+v: %v", h.Name(), b, err)
					} else {
						if resp.StatusCode/100 == 5 {
							log.Infof("5xx response for relay %q backend %+v: %v", h.Name(), b, resp.StatusCode)
						}
						responses <- resp
					}
				}()
			}

			go func() {
				wg.Wait()
				close(responses)
				putBuf(outBuf)
			}()

			var errResponse *relay.ResponseData

			for resp := range responses {
				switch resp.StatusCode / 100 {
				case 2:
					ctx.Status(http.StatusNoContent)
					return

				case 4:
					// user error
					resp.Write(ctx.Writer)
					return

				default:
					// hold on to one of the responses to return back to the client
					errResponse = resp
				}
			}

			// no successful writes
			if errResponse == nil {
				// failed to make any valid request...
				jsonError(ctx, http.StatusServiceUnavailable, "unable to write points")
				return
			}

			errResponse.Write(ctx.Writer)
		})
	}(r.Group("/write"))
}
