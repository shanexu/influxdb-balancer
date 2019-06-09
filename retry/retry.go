package retry

import (
	"time"
)

type Retry struct {
	fails       int
	ShouldRetry func(err error, t int) bool
	BackOff     time.Duration
	MaxBackOff  time.Duration
	Factor      float64
}

func NewSimpleRetry(maxRetry int) *Retry {
	return &Retry{
		BackOff:    time.Millisecond * 100,
		MaxBackOff: time.Second * 3,
		Factor:     1,
		ShouldRetry: func(err error, t int) bool {
			return t <= maxRetry
		},
	}
}

func (r *Retry) Execute(fun func() error) error {
	err := fun()
	if err == nil {
		return nil
	}
	r.fails += 1
	if !r.ShouldRetry(err, r.fails) {
		return err
	}
	r.BackOff = time.Duration(float64(r.BackOff) * r.Factor)
	if r.BackOff > r.MaxBackOff {
		r.BackOff = r.MaxBackOff
	}
	time.Sleep(r.BackOff)
	return r.Execute(fun)
}
