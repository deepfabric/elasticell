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
###### Configuration
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

###### Run
```bash
pd --cfg=pd.json --log-level=info
```

##### Node: 192.168.1.102
###### Configuration
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

###### Run
```bash
pd --cfg=pd.json --log-level=info
```

##### Node: 192.168.1.103
###### Configuration
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

###### Run
```bash
pd --cfg=pd.json --log-level=info
```

#### Cell
##### Node: 192.168.1.201
```bash
cell --log-level=info --pd=192.168.1.101:20800,192.168.1.102:20800,192.168.1.103:20800 --addr=192.168.1.201:10800 --addr-cli=192.168.1.201:6379 --zone=zone-1 --rack=rack-1 --data=/apps/deepfabric/data
```

##### Node: 192.168.1.202
```bash
cell --log-level=info --pd=192.168.1.101:20800,192.168.1.102:20800,192.168.1.103:20800 --addr=192.168.1.202:10800 --addr-cli=192.168.1.202:6379 --zone=zone-2 --rack=rack-2 --data=/apps/deepfabric/data
```

##### Node: 192.168.1.203
```bash
cell --log-level=info --pd=192.168.1.101:20800,192.168.1.102:20800,192.168.1.103:20800 --addr=192.168.1.203:10800 --addr-cli=192.168.1.203:6379 --zone=zone-3 --rack=rack-3 --data=/apps/deepfabric/data
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