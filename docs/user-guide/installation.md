## Installation
Elasticell is consists of pd, cell and redis-proxy(optional). [More about Elasticell components](../architecture/concept.md)

### Configuration
For example, we will install Elasticell cluster as below: 

|Components|Nodes|
|--|--|
|PD|192.168.1.101, 192.168.1.102, 192.168.1.103|
|Cell|192.168.1.201, 192.168.1.202, 192.168.1.203|
|Redis-Proxy|192.168.1.91|

On each node, use `/apps/deepfabric` as base folder, and create configuration file `/apps/deepfabric/cfg/cfg.json`, and create data folder `/apps/deepfabric/data`, and create log folder `/apps/deepfabric/log`

#### PD
##### Node: 192.168.1.101
```json
{
    "name": "pd1",
    "dataDir": "/apps/deepfabric/data",
    "leaseSecsTTL": 5,
    "rpcAddr": "192.168.1.101:20800",
    "embedEtcd": {
        "clientUrls": "http://192.168.1.101:2379",
        "peerUrls": "http://192.168.1.101:2380",
        "initialCluster": "pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380",
        "initialClusterState": "new"
    },
    "Schedule": {
        "maxReplicas": 3,
        "locationLabels": ["zone", "rack"],
        "maxSnapshotCount": 3,
        "maxStoreDownTimeMs": 3600000,
        "leaderScheduleLimit": 16,
        "cellScheduleLimit": 12,
        "replicaScheduleLimit": 16
    }
}
```

##### Node: 192.168.1.102
```json
{
    "name": "pd2",
    "dataDir": "/apps/deepfabric/data",
    "leaseSecsTTL": 5,
    "rpcAddr": "192.168.1.102:20800",
    "embedEtcd": {
        "clientUrls": "http://192.168.1.102:2379",
        "peerUrls": "http://192.168.1.102:2380",
        "initialCluster": "pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380",
        "initialClusterState": "new"
    },
    "Schedule": {
        "maxReplicas": 3,
        "locationLabels": ["zone", "rack"],
        "maxSnapshotCount": 3,
        "maxStoreDownTimeMs": 3600000,
        "leaderScheduleLimit": 16,
        "cellScheduleLimit": 12,
        "replicaScheduleLimit": 16
    }
}
```

##### Node: 192.168.1.103
```json
{
    "name": "pd3",
    "dataDir": "/apps/deepfabric/data",
    "leaseSecsTTL": 5,
    "rpcAddr": "192.168.1.103:20800",
    "embedEtcd": {
        "clientUrls": "http://192.168.1.103:2379",
        "peerUrls": "http://192.168.1.103:2380",
        "initialCluster": "pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380",
        "initialClusterState": "new"
    },
    "Schedule": {
        "maxReplicas": 3,
        "locationLabels": ["zone", "rack"],
        "maxSnapshotCount": 3,
        "maxStoreDownTimeMs": 3600000,
        "leaderScheduleLimit": 16,
        "cellScheduleLimit": 12,
        "replicaScheduleLimit": 16
    }
}
```

#### Cell
##### Node: 192.168.1.201
```json
{
    "redis": {
        "listen": "192.168.1.201:6379",
        "readBufferSize": 512,
        "writeBufferSize": 512,
        "writeBatchLimit": 64
    },

    "node": {
        "clusterID": 0,
        "labels": [
            {
                "key": "zone",
                "value": "zone-1"
            },
            {
                "key": "rack",
                "value": "rack-1"
            }
        ],
        "pdRPCAddr": [
            "192.168.1.101:20800",
            "192.168.1.102:20800",
            "192.168.1.103:20800"
        ],
        "raftStore": {
            "storeAddr": "192.168.1.201:10800",
            "storeAdvertiseAddr": "192.168.1.201:10800",
            "storeDataPath": "/apps/deepfabric/data",
            "storeHeartbeatIntervalMs": 2000,
            "cellHeartbeatIntervalMs": 1000,
            "maxPeerDownSec": 300,
            "splitCellCheckIntervalMs": 10000,
            "reportCellIntervalMs": 1000,
            "raftGCLogIntervalMs": 10000,
            "raftLogGCCountLimit": 49152,
            "raftLogGCSizeLimit": 50331648,
            "raftProposeBatchLimit": 256,
            "raftMessageSendBatchLimit": 64,
            "raftMessageWorkerCount": 16,
            "raftLogGCThreshold": 50,
            "raftLogGCLagThreshold": 1024,
            "cellCheckSizeDiff": 8388608,
            "cellMaxSize": 83886080,
            "cellSplitSize": 67108864,
            
            "raft": {
                "electionTick": 10,
                "heartbeatTick": 2,
                "maxSizePerMsg": 1048576,
                "maxInflightMsgs": 256,
                "maxSizePerEntry": 8388608,
                "baseTick": 1000
            },
            "applyWorkerCount": 8,
            "enableRequestMetrics": true
        }
    },

    "metric": {
        "job": "cluster-0",
        "address": "xxx.xxx.xxx:9091",
        "intervalSec": 1
    }
}
```

##### Node: 192.168.1.202
```json
{
    "redis": {
        "listen": "192.168.1.202:6379",
        "readBufferSize": 512,
        "writeBufferSize": 512,
        "writeBatchLimit": 64
    },

    "node": {
        "clusterID": 0,
        "labels": [
            {
                "key": "zone",
                "value": "zone-1"
            },
            {
                "key": "rack",
                "value": "rack-1"
            }
        ],
        "pdRPCAddr": [
            "192.168.1.101:20800",
            "192.168.1.102:20800",
            "192.168.1.103:20800"
        ],
        "raftStore": {
            "storeAddr": "192.168.1.202:10800",
            "storeAdvertiseAddr": "192.168.1.202:10800",
            "storeDataPath": "/apps/deepfabric/data",
            "storeHeartbeatIntervalMs": 2000,
            "cellHeartbeatIntervalMs": 1000,
            "maxPeerDownSec": 300,
            "splitCellCheckIntervalMs": 10000,
            "reportCellIntervalMs": 1000,
            "raftGCLogIntervalMs": 10000,
            "raftLogGCCountLimit": 49152,
            "raftLogGCSizeLimit": 50331648,
            "raftProposeBatchLimit": 256,
            "raftMessageSendBatchLimit": 64,
            "raftMessageWorkerCount": 16,
            "raftLogGCThreshold": 50,
            "raftLogGCLagThreshold": 1024,
            "cellCheckSizeDiff": 8388608,
            "cellMaxSize": 83886080,
            "cellSplitSize": 67108864,
            
            "raft": {
                "electionTick": 10,
                "heartbeatTick": 2,
                "maxSizePerMsg": 1048576,
                "maxInflightMsgs": 256,
                "maxSizePerEntry": 8388608,
                "baseTick": 1000
            },
            "applyWorkerCount": 8,
            "enableRequestMetrics": true
        }
    },

    "metric": {
        "job": "cluster-0",
        "address": "xxx.xxx.xxx:9091",
        "intervalSec": 1
    }
}
```

##### Node: 192.168.1.203
```json
{
    "redis": {
        "listen": "192.168.1.203:6379",
        "readBufferSize": 512,
        "writeBufferSize": 512,
        "writeBatchLimit": 64
    },

    "node": {
        "clusterID": 0,
        "labels": [
            {
                "key": "zone",
                "value": "zone-1"
            },
            {
                "key": "rack",
                "value": "rack-1"
            }
        ],
        "pdRPCAddr": [
            "192.168.1.101:20800",
            "192.168.1.102:20800",
            "192.168.1.103:20800"
        ],
        "raftStore": {
            "storeAddr": "192.168.1.203:10800",
            "storeAdvertiseAddr": "192.168.1.203:10800",
            "storeDataPath": "/apps/deepfabric/data",
            "storeHeartbeatIntervalMs": 2000,
            "cellHeartbeatIntervalMs": 1000,
            "maxPeerDownSec": 300,
            "splitCellCheckIntervalMs": 10000,
            "reportCellIntervalMs": 1000,
            "raftGCLogIntervalMs": 10000,
            "raftLogGCCountLimit": 49152,
            "raftLogGCSizeLimit": 50331648,
            "raftProposeBatchLimit": 256,
            "raftMessageSendBatchLimit": 64,
            "raftMessageWorkerCount": 16,
            "raftLogGCThreshold": 50,
            "raftLogGCLagThreshold": 1024,
            "cellCheckSizeDiff": 8388608,
            "cellMaxSize": 83886080,
            "cellSplitSize": 67108864,
            
            "raft": {
                "electionTick": 10,
                "heartbeatTick": 2,
                "maxSizePerMsg": 1048576,
                "maxInflightMsgs": 256,
                "maxSizePerEntry": 8388608,
                "baseTick": 1000
            },
            "applyWorkerCount": 8,
            "enableRequestMetrics": true
        }
    },

    "metric": {
        "job": "cluster-0",
        "address": "xxx.xxx.xxx:9091",
        "intervalSec": 1
    }
}
```

#### Redis-Proxy
##### Node: 192.168.1.91
```json
{
    "addr": "192.168.1.91:6379",
    "pdAddrs": [
        "192.168.1.101:20800",
        "192.168.1.102:20800",
        "192.168.1.103:20800"
    ],
    "maxRetries": 3,
    "retryDuration": 2000,
    "supportCMDs": [
        "ping",
        "set",
        "get",
        "mset",
        "mget",
        "incrby",
        "decrby",
        "getset",
        "append",
        "setnx",
        "strLen",
        "incr",
        "decr",
        "setrange",
        "msetnx",
        "hset",
        "hget",
        "hdel",
        "hexists",
        "hkeys",
        "hvals",
        "hgetall",
        "hlen",
        "hmget",
        "hmset",
        "hsetnx",
        "hstrlen",
        "hincrby",
        "lindex",
        "linsert",
        "llen",
        "lpop",
        "lpush",
        "lpushx",
        "lrange",
        "lrem",
        "lset",
        "ltrim",
        "rpop",
        "rpoplpush",
        "rpush",
        "rpushx",
        "sadd",
        "scard",
        "srem",
        "smembers",
        "sismember",
        "spop",
        "zadd",
        "zcard",
        "zcount",
        "zincrby",
        "zlexcount",
        "zrange",
        "zrangebylex",
        "zrangebyscore",
        "zrank",
        "zrem",
        "zremrangebylex",
        "zremrangebyrank",
        "zremrangebyscore",
        "zscore"
    ]
}
```


### Install from docker
```bash
docker pull deepfabric/pd
docker pull deepfabric/cell
docker pull deepfabric/proxy
```

#### Run PD
Execute the following command on each machine(192.168.1.101,192.168.1.102,192.168.1.103)

```bash
docker run -d -v /apps/deepfabric/cfg:/apps/deepfabric/cfg -v /apps/deepfabric/data:/apps/deepfabric/data deepfabric/pd
```

#### Run Cell
Execute the following command on each machine(192.168.1.201,192.168.1.202,192.168.1.203)

```bash
docker run -d -v /apps/deepfabric/cfg:/apps/deepfabric/cfg -v /apps/deepfabric/data:/apps/deepfabric/data deepfabric/cell
```

#### Run Redis-Proxy
Execute the following command on 192.168.1.91

```bash
docker run -d -v /apps/deepfabric/cfg:/apps/deepfabric/cfg -v /apps/deepfabric/data:/apps/deepfabric/data deepfabric/proxy
```

### Install from source
Elasticell use RocksDB as storage engine, so need install some dependency package and some go package that wrapped RocksDB C-API. We highly recommended use docker to build Elasticell.

#### Pull Elasticell Image
```bash
docker pull deepfabric/elasticell-build
```

##### Create dist folder
```bash
/apps/deepfabric
```

##### Build PD
```bash
docker run -it --rm -v /apps/deepfabric/dist:/apps/deepfabric/dist -e ELASTICELL_BUILD_TARGET=pd -e ELASTICELL_BUILD_VERSION=master deepfabric/elasticell-build 
```

##### Build Cell
```bash
docker run -it --rm -v /apps/deepfabric/dist:/apps/deepfabric/dist -e ELASTICELL_BUILD_TARGET=cell -e ELASTICELL_BUILD_VERSION=master deepfabric/elasticell-build 
```

##### Build Redis-Proxy
```bash
docker run -it --rm -v /apps/deepfabric/dist:/apps/deepfabric/dist -e ELASTICELL_BUILD_TARGET=proxy -e ELASTICELL_BUILD_VERSION=master deepfabric/elasticell-build 
```

##### Build all binary
```bash
docker run -it --rm -v /apps/deepfabric/dist:/apps/deepfabric/dist -e ELASTICELL_BUILD_TARGET=all -e ELASTICELL_BUILD_VERSION=master deepfabric/elasticell-build 
```

##### Install some package on Cell node
```bash
apt-get update
apt-get -y install libsnappy-dev  
apt-get -y install zlib1g-dev 
apt-get -y install libbz2-dev 
apt-get -y install libgtest-dev 
apt-get -y install libjemalloc-dev
```

##### Run PD
Execute the following command on each machine(192.168.1.101,192.168.1.102,192.168.1.103)

```bash
/apps/deepfabric/pd --cfg=/apps/deepfabric/cfg/cfg.json --log-level=debug --log-file=/apps/deepfabric/log/pd.log
```

##### Run Cell
Execute the following command on each machine(192.168.1.201,192.168.1.202,192.168.1.203)

```bash
/apps/deepfabric/cell --cfg=/apps/deepfabric/cfg/cfg.json --log-level=debug --log-file=/apps/deepfabric/log/pd.log
```

##### Run Redis-Proxy
Execute the following command on 192.168.1.91

```bash
/apps/deepfabric/redis-proxy --cfg=/apps/deepfabric/cfg/cfg.json --log-level=debug --log-file=/apps/deepfabric/log/pd.log
```