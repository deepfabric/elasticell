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
        "writeBufferSize": 512
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
            "raftGCLogIntervalMs": 10000,
            "raftLogGCCountLimit": 49152,
            "raftLogGCSizeLimit": 50331648,
            "raftLogGCThreshold": 50,
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
            }
        }
    }
}
```

##### Node: 192.168.1.202
```json
{
    "redis": {
        "listen": "192.168.1.202:6379",
        "readBufferSize": 512,
        "writeBufferSize": 512
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
            "raftGCLogIntervalMs": 10000,
            "raftLogGCCountLimit": 49152,
            "raftLogGCSizeLimit": 50331648,
            "raftLogGCThreshold": 50,
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
            }
        }
    }
}
```

##### Node: 192.168.1.203
```json
{
    "redis": {
        "listen": "192.168.1.203:6379",
        "readBufferSize": 512,
        "writeBufferSize": 512
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
            "raftGCLogIntervalMs": 10000,
            "raftLogGCCountLimit": 49152,
            "raftLogGCSizeLimit": 50331648,
            "raftLogGCThreshold": 50,
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
            }
        }
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
docker pull deepfabric/redis-proxy
```

#### Run PD
Execute the following command on each machine(192.168.1.101,192.168.1.102,192.168.1.103)

```bash
docker run -d -v /apps/deepfabric:/apps/deepfabric deepfabric/pd --log-level=debug --log-file=/apps/deepfabric/log/pd.log
```

#### Run Cell
Execute the following command on each machine(192.168.1.201,192.168.1.202,192.168.1.203)

```bash
docker run -d -v /apps/deepfabric:/apps/deepfabric deepfabric/cell --log-level=debug --log-file=/apps/deepfabric/log/cell.log
```

#### Run Redis-Proxy
Execute the following command on 192.168.1.91

```bash
docker run -d -v /apps/deepfabric:/apps/deepfabric deepfabric/redis-proxy --log-level=debug --log-file=/apps/deepfabric/log/proxy.log
```

### Install from source
Elasticell use RocksDB as storage engine, so need install some dependency package and some go package that wrapped RocksDB C-API. We highly recommended use docker to build Elasticell.

#### Pull Elasticell Image
```bash
docker pull deepfabric/elasticell-dev
```

#### Clone Elasticell
```bash
git clone https://github.com/deepfabric/elasticell.git
```

#### Clone Redis-Proxy
```bash
git clone https://github.com/deepfabric/elasticell-proxy.git
```

#### Build
For example, put the Elasticell components source to `/source/deepfabric`

##### Run docker image
```bash
docker run -it --rm -v /go/src/github.com/deepfabric/elasticell:/source/deepfabric/elasticell -v /go/src/github.com/deepfabric/elasticell-proxy:/source/deepfabric/elasticell-proxy deepfabric/elasticell-dev
```

##### Build PD
```bash
cd /go/src/github.com/deepfabric/elasticell/cmd/pd
go build -ldflags "-w -s" pd.go
```

##### Build Cell
```bash
cd /go/src/github.com/deepfabric/elasticell/cmd/cell
go build -ldflags "-w -s" cell.go
```

##### Build Redis-Proxy
```bash
cd /go/src/github.com/deepfabric/elasticell-proxy/cmd/redis
go build -ldflags "-w -s" redis-proxy.go
```

##### Move binary to node
You can use `docker cp` command to get the binary package of Elasticell components, and move these to the target node.

##### Install some package on each node
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