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
	labelRequestInQueue  = "in-queue"

	labelQueueReport      = "report"
	labelQueueReq         = "req"
	labelQueueBatchSize   = "batch-size"
	labelQueueBatch       = "batch"
	labelQueueTick        = "tick"
	labelQueueApplyResult = "apply-result"
	labelQueueStep        = "step"
	labelQueueMsgs        = "msgs"
	labelQueueSnaps       = "snaps"
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

	queueGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "queue_size",
			Help:      "Total size of queue size.",
		}, []string{"type"})
)

func initMetricsForRequest() {
	prometheus.MustRegister(requestDurationHistogram)
	prometheus.MustRegister(queueGauge)

}

func observeRequestInQueue(start time.Time) {
	requestDurationHistogram.WithLabelValues(labelRequestInQueue).Observe(time.Now().Sub(start).Seconds())
}

func observeRequestWaitting(c *cmd) {
	observeRequestWithlabel(c, labelRequestWaitting)
}

func observeRequestProposal(c *cmd) {
	observeRequestWithlabel(c, labelRequestProposal)
}

func observeRequestRaft(c *cmd) {
	observeRequestWithlabel(c, labelRequestRaft)
}

func observeRequestStored(c *cmd) {
	observeRequestWithlabel(c, labelRequestStored)
}

func observeRequestResponse(c *cmd) {
	now := time.Now()
	for _, req := range c.req.Requests {
		requestDurationHistogram.WithLabelValues(labelRequestResponse).Observe(now.Sub(time.Unix(0, req.StartAt)).Seconds())
	}
}

func observeRequestWithlabel(c *cmd, label string) {
	now := time.Now()
	for _, req := range c.req.Requests {
		requestDurationHistogram.WithLabelValues(label).Observe(now.Sub(time.Unix(0, req.LastStageAt)).Seconds())
		stage(req, now.UnixNano())
	}
}

func stage(req *raftcmdpb.Request, now int64) {
	req.LastStageAt = now
}
