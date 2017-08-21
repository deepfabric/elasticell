package raftstore

import (
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	labelRequestWaitting = "waitting"
	labelRequestProposal = "proposal"
	labelRequestRaft     = "raft"
	labelRequestStored   = "stored"
	labelRequestResponse = "response"
)

var (
	requestDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "request_duration_seconds",
			Help:      "Bucketed histogram of request cycle time duration",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		}, []string{"stage"})

	requestBatchSizeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "request_batch_size",
			Help:      "Total size of propose batch size.",
		})
)

func initMetricsForRequest() {
	prometheus.MustRegister(requestDurationHistogram)
	prometheus.MustRegister(requestBatchSizeGauge)
}

func observeRequestWaitting(cmd *cmd) {
	observeRequestWithlabel(cmd, labelRequestWaitting)
}

func observeRequestProposal(cmd *cmd) {
	observeRequestWithlabel(cmd, labelRequestProposal)
}

func observeRequestRaft(cmd *cmd) {
	observeRequestWithlabel(cmd, labelRequestRaft)
}

func observeRequestStored(cmd *cmd) {
	observeRequestWithlabel(cmd, labelRequestStored)
}

func observeRequestResponse(cmd *cmd) {
	now := time.Now()
	for _, req := range cmd.req.Requests {
		requestDurationHistogram.WithLabelValues(labelRequestResponse).Observe(now.Sub(time.Unix(0, req.StartAt)).Seconds())
	}
}

func observeRequestWithlabel(cmd *cmd, label string) {
	now := time.Now()
	for _, req := range cmd.req.Requests {
		requestDurationHistogram.WithLabelValues(label).Observe(now.Sub(time.Unix(0, req.LastStageAt)).Seconds())
		stage(req, now.UnixNano())
	}
}

func stage(req *raftcmdpb.Request, now int64) {
	req.LastStageAt = now
}
