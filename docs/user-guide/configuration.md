## Configuration
### PD
|field|comments|
|--|--|
|name|The name of this pd|
|dataDir|The meta data storage dir|
|leaseSecsTTL|Lease time for PD leader|
|rpcAddr|Export grpc address for External components|
|embedEtcd:clientUrls|Etcd address for internal etcd client|
|embedEtcd:peerUrls|Etcd address for internal etcd peer|
|embedEtcd:initialCluster|PD cluster info|
|embedEtcd:initialClusterState|Iniial etcd cluster state|
|schedule:maxReplicas|The max replicas number for each cell|
|schedule:locationLabels|The location label for store, pd use this select best store for cell replicas|
|schedule:maxSnapshotCount|The max snapshot count for store, pd use this select best store for cell replicas|
|schedule:maxStoreDownTimeMs|The max time of store does not send heartbeat for PD. PD will not schedule cell replicas to this store|
|schedule:leaderScheduleLimit|The max count of leader cells at every store|
|schedule:cellScheduleLimit|The max count of cells at every store|
|schedule:replicaScheduleLimit|The max operator count of create cell replica at every store|

### Cell
|command args|comments|
|--|--|
|clusterid|The Elasticell cluster id, 0 means not join the cluster|
|pd|PD addresses|
|addr|Internal address|
|addr-cli|KV client address|
|data|The data dir|
|zone|Zone label|
|rack|Rack label|
|buffer-cli-read|Buffer(bytes): bytes of KV client read|
|buffer-cli-write|Buffer(bytes): bytes of KV client write|
|batch-cli-resps|Batch: Max count of responses in a write operation|
|capacity-cell|Capacity(MB): cell|
|interval-heartbeat-store|Interval(sec): Store heartbeat|
|interval-heartbeat-cell|Interval(sec): Cell heartbeat|
|interval-split-check|Interval(sec): Split check|
|interval-compact|Interval(sec): Compact raft log|
|interval-report-metric|Interval(sec): Report cell metric|
|interval-raft-tick|Interval(ms): Raft tick|
|limit-peer-down|Limit(sec): Max peer downtime|
|limit-compact-count|Limit: Count of raft logs, if reach this limit, leader will compact [first,applied], otherwise [first, minimum replicated]|
|limit-compact-bytes|Limit(MB): Total bytes of raft logs, if reach this limit, leader will compact [first,applied], otherwise [first, minimum replicated]|
|limit-compact-lag|Limit: Max count of lag log, leader will compact [first, compact - lag], avoid send snapshot file to a little lag peer|
|limit-raft-msg-count|Limit: Max count of in-flight raft append messages|
|limit-raft-msg-bytes|Limit(MB): Max bytes per raft msg|
|limit-raft-entry-bytes|Limit(MB): Max bytes of raft log entry|
|threshold-compact|Threshold: Raft Log compact, count of [first, replicated]|
|threshold-split-check|Threshold(bytes): Start split check, bytes that the cell has bean stored|
|threshold-raft-election|Threshold: Raft election, after this ticks|
|threshold-raft-heartbeat|Threshold: Raft heartbeat, after this ticks|
|batch-size-proposal|Batch: Max commands in a proposal|
|batch-size-sent|Batch: Max size of send msgs|
|worker-count-sent|Worker count: sent internal messages|
|worker-count-apply|Worker count: apply raft log|
|enable-metrics-request|Enable: request metrics|
|enable-sync-raftlog|Enable: sync to disk while append the raft log|
|metric-job|prometheus job name|
|metric-address|prometheus proxy address|
|interval-metric-sync|Interval(sec): metric sync|

### Proxy
|field|comments|
|--|--|
|addr|Address to serve redis|
|pdAddrs|GRPC address for PD cluster|
|maxRetries|Max times for retry send msg to backend servers|
|retryDuration|Duration interval for retry|
|supportCMDs|Support redis commands|