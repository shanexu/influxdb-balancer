package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/shanexu/logn"
	"github.com/shanexu/logn/core"
	"github.com/spf13/viper"
	"github.com/shanexu/influxdb-balancer/relay"
	_ "github.com/shanexu/influxdb-balancer/relay/gin"
)

var (
	configFile = flag.String("config", "", "Configuration file to use")
	// deprecated
	logFile = flag.String("log", "", "Log file path to use")
)

var (
	log        = logn.GetLogger()
	metricsLog = logn.GetLogger("metrics")
)

type metricLogger struct {
	core.Logger
}

func (ml *metricLogger) Printf(format string, v ...interface{}) {
	if format[len(format)-1] == '\n' {
		format = format[0 : len(format)-1]
	}
	ml.Logger.Infof(format, v...)
}

func main() {
	flag.Parse()

	viper.SetConfigFile(*configFile)
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	r, err := relay.NewService(viper.GetViper())
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		pprofPort := viper.GetInt("pprof-port")
		log.Info(http.ListenAndServe(":"+strconv.Itoa(pprofPort), nil))
	}()

	go func() {
		log.Info("starting relays...")
		r.Run()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	<-sigChan
	r.Stop()
	relay.OnShutdownWg.Wait()
	log.Info("Bye!")

}
