## Installation
Elasticell is consists of pd, cell and proxy(optional). [More about Elasticell components](../architecture/concept.md)

### Configuration
For example, we will install Elasticell cluster as below: 

|Components|Nodes|
|--|--|
|PD|192.168.1.101, 192.168.1.102, 192.168.1.103|
|Cell|192.168.1.201, 192.168.1.202, 192.168.1.203|
|Proxy|192.168.1.91|

On each node, use `/apps/deepfabric` as base folder, and create data folder `/apps/deepfabric/data`, and create log folder `/apps/deepfabric/log`

#### PD
##### Node: 192.168.1.101
```bash
./pd --log-level=info --log-file=/apps/deepfabric/log/pd.log --name=pd1 --data=/apps/deepfabric/data --addr-rpc=192.168.1.101:20800 --urls-client=http://192.168.1.101:2379 --urls-peer=http://192.168.1.101:2380 --initial-cluster=pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380
```

##### Node: 192.168.1.102
```bash
./pd --log-level=info --log-file=/apps/deepfabric/log/pd.log --name=pd2 --data=/apps/deepfabric/data --addr-rpc=192.168.1.102:20800 --urls-client=http://192.168.1.102:2379 --urls-peer=http://192.168.1.102:2380 --initial-cluster=pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380
```

##### Node: 192.168.1.103
```bash
./pd --log-level=info --log-file=/apps/deepfabric/log/pd.log --name=pd3 --data=/apps/deepfabric/data --addr-rpc=192.168.1.103:20800 --urls-client=http://192.168.1.103:2379 --urls-peer=http://192.168.1.103:2380 --initial-cluster=pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380
```

#### Cell
##### Node: 192.168.1.201
```bash
./cell --log-level=info --log-file=/apps/deepfabric/log/cell.log --pd=192.168.1.101:20800,192.168.1.102:20800,192.168.1.103:20800 --addr=192.168.1.201:10800 --addr-cli=192.168.1.201:6379 --zone=zone-1 --rack=rack-1 --data=/apps/deepfabric/data
```

##### Node: 192.168.1.202
```bash
./cell --log-level=info --log-file=/apps/deepfabric/log/cell.log --pd=192.168.1.101:20800,192.168.1.102:20800,192.168.1.103:20800 --addr=192.168.1.202:10800 --addr-cli=192.168.1.202:6379 --zone=zone-2 --rack=rack-2 --data=/apps/deepfabric/data
```

##### Node: 192.168.1.203
```bash
cell --log-level=info --log-file=/apps/deepfabric/log/cell.log --pd=192.168.1.101:20800,192.168.1.102:20800,192.168.1.103:20800 --addr=192.168.1.203:10800 --addr-cli=192.168.1.203:6379 --zone=zone-3 --rack=rack-3 --data=/apps/deepfabric/data
```

#### Proxy
##### Node: 192.168.1.91
Create a json file in the folder `/apps/deepfabric/cfg/cfg.json` as below:

```json
{
    "addr": "192.168.1.91:6379",
    "addrNotify": "192.168.1.91:9998",
    "watcherHeartbeatSec": 5,
    "pdAddrs": [
        "192.168.1.101:20800",
        "192.168.1.102:20800",
        "192.168.1.103:20800"
    ],
    "maxRetries": 100,
    "retryDuration": 2000,
    "workerCount": 2,
    "supportCMDs": [
        "query",
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

Run proxy:
```bash
./redis-proxy --log-level=info --log-file=/apps/deepfabric/log/proxy.log --cfg=/apps/deepfabric/cfg/cfg.json
```

### Install from docker
```bash
docker pull deepfabric/pd
docker pull deepfabric/cell
docker pull deepfabric/proxy
```

#### Run PD
##### Node: 192.168.1.101
```bash
docker run -d --net=host -v /apps/deepfabric/data:/apps/deepfabric/data deepfabric/pd --log-level=info --log-file=/apps/deepfabric/log/pd.log --name=pd1 --data=/apps/deepfabric/data --addr-rpc=192.168.1.101:20800 --urls-client=http://192.168.1.101:2379 --urls-peer=http://192.168.1.101:2380 --initial-cluster=pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380
```

##### Node: 192.168.1.102
```bash
docker run -d --net=host -v /apps/deepfabric/data:/apps/deepfabric/data deepfabric/pd --log-level=info --log-file=/apps/deepfabric/log/pd.log --name=pd2 --data=/apps/deepfabric/data --addr-rpc=192.168.1.102:20800 --urls-client=http://192.168.1.102:2379 --urls-peer=http://192.168.1.102:2380 --initial-cluster=pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380
```

##### Node: 192.168.1.103
```bash
docker run -d --net=host -v /apps/deepfabric/data:/apps/deepfabric/data deepfabric/pd --log-level=info --log-file=/apps/deepfabric/log/pd.log --name=pd3 --data=/apps/deepfabric/data --addr-rpc=192.168.1.103:20800 --urls-client=http://192.168.1.103:2379 --urls-peer=http://192.168.1.103:2380 --initial-cluster=pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380
```

#### Run Cell
##### Node: 192.168.1.201
```bash
docker run -d --net=host -v /apps/deepfabric/data:/apps/deepfabric/data deepfabric/cell --log-level=info --log-file=/apps/deepfabric/log/cell.log --pd=192.168.1.101:20800,192.168.1.102:20800,192.168.1.103:20800 --addr=192.168.1.201:10800 --addr-cli=192.168.1.201:6379 --zone=zone-1 --rack=rack-1 --data=/apps/deepfabric/data
```

##### Node: 192.168.1.202
```bash
docker run -d --net=host -v /apps/deepfabric/data:/apps/deepfabric/data deepfabric/cell --log-level=info --log-file=/apps/deepfabric/log/cell.log --pd=192.168.1.101:20800,192.168.1.102:20800,192.168.1.103:20800 --addr=192.168.1.202:10800 --addr-cli=192.168.1.202:6379 --zone=zone-1 --rack=rack-1 --data=/apps/deepfabric/data
```

##### Node: 192.168.1.203
```bash
docker run -d --net=host -v /apps/deepfabric/data:/apps/deepfabric/data deepfabric/cell --log-level=info --log-file=/apps/deepfabric/log/cell.log --pd=192.168.1.101:20800,192.168.1.102:20800,192.168.1.103:20800 --addr=192.168.1.203:10800 --addr-cli=192.168.1.203:6379 --zone=zone-1 --rack=rack-1 --data=/apps/deepfabric/data
```

#### Run Proxy
Execute the following command on 192.168.1.91, the json configuration file is the same as the previous.

```bash
docker run -d --net=host -v /apps/deepfabric/cfg:/apps/deepfabric/cfg deepfabric/proxy --log-level=info --log-file=/apps/deepfabric/log/proxy.log --cfg=/apps/deepfabric/cfg/cfg.json
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

##### Build Proxy
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