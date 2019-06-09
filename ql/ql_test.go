package ql

import (
	"fmt"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestRefine(t *testing.T) {
	s, _ := influxql.ParseStatement("select * from cq10s.cpu where time > now() - 1h")
	ss, _ := s.(*influxql.SelectStatement)
	assert.Equal(t, ss.Sources.Measurements()[0].RetentionPolicy, "cq10s")
	cs, err := Refine(ss)
	assert.Nil(t, err)
	assert.True(t, (cs.TimeRange.Max.UnixNano()-cs.TimeRange.Min.UnixNano()+1) == int64(time.Hour))
	assert.Equal(t, ss.Sources.Measurements()[0].RetentionPolicy, "cq10s")
}

type H struct {
	_u unsafe.Pointer
}

func (h *H) P() {
	fmt.Println((*rate.Limiter)(atomic.LoadPointer(&h._u)))
}

func TestRefine2(t *testing.T) {
	s, _ := influxql.ParseStatement("select * from cpu where time < now() - 1h")
	ss, _ := s.(*influxql.SelectStatement)
	cs, err := Refine(ss)
	assert.Nil(t, err)
	fmt.Println(cs.TimeRange.Min.IsZero())

	var _u unsafe.Pointer
	atomic.StorePointer(&_u, unsafe.Pointer(rate.NewLimiter(100.0, 100)))
	h := H{_u}
	fmt.Println((*rate.Limiter)(atomic.LoadPointer(&_u)))
	h.P()
	atomic.StorePointer(&_u, unsafe.Pointer(rate.NewLimiter(200.0, 200)))
	fmt.Println((*rate.Limiter)(atomic.LoadPointer(&_u)))
	h.P()

}
