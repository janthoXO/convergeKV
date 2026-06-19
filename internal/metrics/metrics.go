// Package metrics exposes the node's operational metrics as a Prometheus
// registry plus pprof, served on the admin HTTP endpoint. All metrics are
// pull-based collectors reading the live components — the hot paths carry
// only the atomic counters they already had.
package metrics

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/janthoXO/convergeKV/internal/antientropy"
	"github.com/janthoXO/convergeKV/internal/cluster"
	"github.com/janthoXO/convergeKV/internal/replication"
	"github.com/janthoXO/convergeKV/internal/transfer"
)

type Sources struct {
	Fanout   *replication.Fanout
	AE       *antientropy.Engine
	Transfer *transfer.Manager
	Cluster  *cluster.Cluster
}

// Serve starts the admin endpoint (metrics + pprof) on addr and returns the
// bound address. It shuts down when ctx ends.
func Serve(ctx context.Context, addr string, src Sources, log *slog.Logger) (string, error) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors(src)...)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("admin endpoint started", "addr", ln.Addr().String())
	return ln.Addr().String(), nil
}

func collectors(src Sources) []prometheus.Collector {
	var cs []prometheus.Collector

	counterFunc := func(name, help string, f func() float64) {
		cs = append(cs, prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "convergekv", Name: name, Help: help,
		}, f))
	}
	gaugeFunc := func(name, help string, f func() float64) {
		cs = append(cs, prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "convergekv", Name: name, Help: help,
		}, f))
	}

	if f := src.Fanout; f != nil {
		counterFunc("deltas_dropped_total",
			"Replication deltas dropped (queue overflow, retry attempts, or age); owned by the AE backstop.",
			func() float64 { return float64(f.Dropped()) })
		cs = append(cs, &peerGauge{
			desc: prometheus.NewDesc("convergekv_retry_queue_depth",
				"Per-peer replication retry queue depth.", []string{"peer"}, nil),
			values: func() map[string]float64 {
				out := map[string]float64{}
				for p, d := range f.QueueDepths() {
					out[p] = float64(d)
				}
				return out
			},
		}, &peerGauge{
			desc: prometheus.NewDesc("convergekv_delta_lag_seconds",
				"Age of the replication delta currently being delivered, per peer.", []string{"peer"}, nil),
			values: func() map[string]float64 {
				out := map[string]float64{}
				for p, d := range f.Lag() {
					out[p] = d.Seconds()
				}
				return out
			},
		})
	}
	if ae := src.AE; ae != nil {
		counterFunc("ae_keys_repaired_total",
			"Documents changed by anti-entropy repairs.",
			func() float64 { return float64(ae.KeysRepaired()) })
		counterFunc("ae_root_checks_total",
			"Anti-entropy root comparisons performed.",
			func() float64 { return float64(ae.RootChecks()) })
		counterFunc("ae_leaf_fetches_total",
			"Anti-entropy leaf-vector fetches (only on root mismatch).",
			func() float64 { return float64(ae.LeafFetches()) })
	}
	if tr := src.Transfer; tr != nil {
		counterFunc("transfer_bytes_total",
			"Document bytes received via bootstrap snapshot transfer.",
			func() float64 { return float64(tr.Bytes()) })
		counterFunc("transfers_started_total",
			"Bootstrap transfers begun by this node.",
			func() float64 { return float64(tr.Started()) })
	}
	if cl := src.Cluster; cl != nil {
		gaugeFunc("membership_members",
			"Alive cluster members in this node's view.",
			func() float64 { return float64(len(cl.Members())) })
		gaugeFunc("membership_generation",
			"This node's gossiped generation (start time, ms).",
			func() float64 { return float64(cl.Self().Generation) })
	}
	return cs
}

// peerGauge is a label-per-peer gauge collector over a snapshot function.
type peerGauge struct {
	desc   *prometheus.Desc
	values func() map[string]float64
}

func (g *peerGauge) Describe(ch chan<- *prometheus.Desc) { ch <- g.desc }

func (g *peerGauge) Collect(ch chan<- prometheus.Metric) {
	for peer, v := range g.values() {
		ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, v, peer)
	}
}
