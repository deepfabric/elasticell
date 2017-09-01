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
	labelQueueBatch       = "batch"
	labelQueuePropose     = "propose"
	labelQueueTick        = "tick"
	labelQueueApplyResult = "apply-result"
	labelQueueStep        = "step"
	labelQueueNotify      = "notify"
	labelQueueMsgs        = "msgs"
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
