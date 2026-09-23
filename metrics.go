package confsource

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

var metricLabels = []string{"service"}

type metrics struct {
	loadedRevision prometheus.Gauge
	degraded       prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer, service string) (*metrics, error) {
	loaded, err := registerGaugeVec(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "confsource_loaded_revision",
		Help: "etcd ModRevision of the config currently committed by confsource",
	}, metricLabels))
	if err != nil {
		return nil, err
	}
	degraded, err := registerGaugeVec(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "confsource_degraded",
		Help: "1 when running on local snapshot without confirming against etcd",
	}, metricLabels))
	if err != nil {
		return nil, err
	}
	return &metrics{
		loadedRevision: loaded.WithLabelValues(service),
		degraded:       degraded.WithLabelValues(service),
	}, nil
}

// registerGaugeVec 注册指标；同一进程里多个 Source 共用注册器时复用已注册的 collector
func registerGaugeVec(reg prometheus.Registerer, vec *prometheus.GaugeVec) (*prometheus.GaugeVec, error) {
	if err := reg.Register(vec); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(*prometheus.GaugeVec); ok {
				return existing, nil
			}
		}
		return nil, err
	}
	return vec, nil
}
