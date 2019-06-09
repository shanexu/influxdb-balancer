package gin

import (
	"bytes"
	"github.com/gin-gonic/gin"
	"github.com/influxdata/influxdb/models"
	iq "github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"github.com/shanexu/logn"
	"github.com/shanexu/influxdb-balancer/ql"
	"github.com/shanexu/influxdb-balancer/relay"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"
)

func uaLimitQuery(ctx *gin.Context) bool {
	ua := strings.ToLower(ctx.GetHeader("User-Agent"))
	switch {
	case ua == "":
		fallthrough
	case strings.Contains(ua, "mozilla"):
		fallthrough
	case strings.Contains(ua, "influxdbshell"):
		return true
	default:
		return false
	}
}

func isCq(stmt *influxql.SelectStatement) (cq bool) {
	defer func() {
		reason := recover()
		if reason != nil {
			cq = false
			log.Error(reason)
		}
	}()
	return strings.HasPrefix(stmt.Sources.Measurements()[0].RetentionPolicy, "cq")
}

var slowLog = logn.GetLogger("slow")

func (h *HTTP) InitQueryRouter(r gin.IRouter) {

	SlowLog := func(ctx *gin.Context) {
		start := time.Now()
		ctx.Next()
		end := time.Now()
		latency := end.Sub(start)
		statusCode := ctx.Writer.Status()
		if latency > h.cfg.SlowLogThreshold || statusCode/100 != 2 {
			slowLog.Warnw("",
				"status_code", statusCode,
				"q", ctx.GetString("q"),
				"db", ctx.GetString("db"),
				"bk", ctx.GetString("bk"),
				"ua", ctx.GetHeader("User-Agent"),
				"latency", latency,
			)
		}
	}

	Query := func(ctx *gin.Context) {
		data, err := ctx.GetRawData()
		if err != nil {
			ctx.JSON(http.StatusBadRequest, httpd.Response{Err: err})
			return
		}
		bodyReader := bytes.NewReader(data)
		ctx.Request.Body = ioutil.NopCloser(bodyReader)

		var qr io.Reader
		qp := strings.TrimSpace(ctx.Request.FormValue("q"))
		if qp == "" {
			ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New(`missing required parameter "q"`)})
			return
		}
		ctx.Set("q", qp)
		qr = strings.NewReader(qp)
		p := influxql.NewParser(qr)
		db := ctx.Request.FormValue("db")
		ctx.Set("db", db)

		// Parse query from query string.
		query, err := p.ParseQuery()
		if err != nil {
			ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.Errorf("error parsing query: %v", err)})
			return
		}

		sts := query.Statements
		resp := httpd.Response{Results: make([]*iq.Result, 0)}
		if len(sts) == 0 {
			ctx.JSON(http.StatusOK, resp)
			return
		}

		if len(sts) == 1 {
			st := sts[0]
			switch st.(type) {
			case *influxql.ShowDatabasesStatement:
				var values [][]interface{}
				for k := range h.backends {
					values = append(values, []interface{}{k})
				}

				rows := models.Rows{}
				resp.Results = append(resp.Results, &iq.Result{
					StatementID: 0,
					Series: append(rows, &models.Row{
						Name:    "databases",
						Columns: []string{"name"},
						Values:  values,
					}),
				})
				ctx.JSON(http.StatusOK, resp)
				return
			}
		}

		hasQuery := false
		hasSpecial := false

		for _, st := range sts {
			selectStatement, ok := st.(*influxql.SelectStatement)
			if ok && uaLimitQuery(ctx) && !isCq(selectStatement) {
				cs, err := ql.Refine(selectStatement)
				if err == nil {
					if cs.TimeRange.Min.IsZero() {
						ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New("min time should be set")})
						return
					}
					MaxRange := h.cfg.QueryMaxRange
					if time.Duration(cs.TimeRange.MaxTimeNano()-cs.TimeRange.MinTimeNano()) > MaxRange {
						ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.Errorf("time range should be less than %s", MaxRange)})
						return
					}
				}
			}

			if !ok {
				switch v := st.(type) {
				case *influxql.ShowMeasurementsStatement:
					hasQuery = true
				case *influxql.ShowTagValuesStatement:
					hasQuery = true
				case *influxql.ShowTagKeysStatement:
					hasQuery = true
				case *influxql.ShowSeriesStatement:
					hasQuery = true
				case *influxql.ShowFieldKeysStatement:
					hasQuery = true
				case *influxql.ShowRetentionPoliciesStatement:
					hasQuery = true
				case *influxql.CreateRetentionPolicyStatement:
					if db == "" {
						db = v.Database
					}
					if db != v.Database {
						ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New("contains multiple database names")})
						return
					}
					hasSpecial = true
				case *influxql.DropRetentionPolicyStatement:
					if db == "" {
						db = v.Database
					}
					if db != v.Database {
						ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New("contains multiple database names")})
						return
					}
					hasSpecial = true
				case *influxql.AlterRetentionPolicyStatement:
					if db == "" {
						db = v.Database
					}
					if db != v.Database {
						ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New("contains multiple database names")})
						return
					}
					hasSpecial = true
				case *influxql.CreateDatabaseStatement:
					if db == "" {
						db = v.Name
					}
					if db != v.Name {
						ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New("contains multiple database names")})
						return
					}
					hasSpecial = true
				case *influxql.CreateContinuousQueryStatement:
					if db == "" {
						db = v.Database
					}
					if db != v.Database {
						ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New("contains multiple database names")})
						return
					}
					hasSpecial = true
				case *influxql.ShowContinuousQueriesStatement:
					hasQuery = true
				case *influxql.DropContinuousQueryStatement:
					if db == "" {
						db = v.Database
					}
					if db != v.Database {
						ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New("contains multiple database names")})
						return
					}
					hasQuery = true
				default:
					ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New("contains unsupported statement")})
					return
				}
			}
		}

		if db == "" {
			for i := range sts {
				resp.Results = append(resp.Results, &iq.Result{
					StatementID: i,
					Err:         errors.New("database name required"),
				})
			}
			ctx.JSON(http.StatusOK, resp)
			return
		}

		if hasQuery && hasSpecial {
			ctx.JSON(http.StatusBadRequest, httpd.Response{Err: errors.New("mixing query and create is not supported yet")})
			return
		}

		if hasSpecial {
			if len(h.backends[db]) == 0 {
				ctx.JSON(http.StatusBadGateway, httpd.Response{Err: errors.New("no available backend")})
				return
			}
			reps := make([]*httptest.ResponseRecorder, 0)
			for _, b := range h.backends[db] {
				b := b.(*relay.HttpBackend)
				if b.ReverseProxy() == nil {
					continue
				}
				bodyReader.Seek(0, 0)
				ctx.Request.Host = b.Host()
				recorder := httptest.NewRecorder()
				reps = append(reps, recorder)
				b.ReverseProxy().ServeHTTP(recorder, ctx.Request)
				if recorder.Code/100 != 2 {
					// copy error response
					data, _ := ioutil.ReadAll(recorder.Body)
					copyHeader(ctx.Writer.Header(), recorder.Header())
					ctx.Header("X-Influxdb-Name", b.Name())
					ctx.Status(recorder.Code)
					ctx.Writer.Write(data)
					return
				}
			}
			if len(reps) > 0 {
				// copy ok response
				recorder := reps[0]
				data, _ := ioutil.ReadAll(recorder.Body)
				copyHeader(ctx.Writer.Header(), recorder.Header())
				ctx.Status(recorder.Code)
				ctx.Writer.Write(data)
			}
			// went wrong
			return
		}

		for _, ib := range h.backends[db] {
			b := ib.(*relay.HttpBackend)
			if b.Alive() {
				if b.ReverseProxy() == nil {
					continue
				}
				bodyReader.Seek(0, 0)
				ctx.Request.Host = b.Host()
				ctx.Set("bk", b.Name())
				b.ReverseProxy().ServeHTTP(ctx.Writer, ctx.Request)
				return
			}
		}

		ctx.JSON(http.StatusBadGateway, httpd.Response{Err: errors.New("no available backend")})
	}

	func(r gin.IRouter) {
		r.Use(SlowLog)
		r.GET("", Query)
		r.POST("", Query)
	}(r.Group("/query"))
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
