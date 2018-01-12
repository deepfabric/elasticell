## 概念
为了更好的理解Elasticell的架构，需要理解Elasticell的一些基本概念.

### Store
Store是一个运行在一个独立磁盘上的服务。一个Store管理多个`Cell`，为Cell提供存储，分发外部请求到对应的Cell上。

### Cell
一个Cell管理所有在[start,end)范围的数据，每个cell在不同的store上有多个副本，我们把这些副本称为`Peer`。一个Cell的所有副本组成一个`Raft Group`。一个Cell的Peer在会被PD调度到其他的Store上去，或者从store上移除。

一个Cell有3个元数据:

1. ID, 一个Cell的标识

2. Data range, Cell管理数据的范围

3. Peers, Cell的副本

4. Epoch, Cell的版本，每当Cell发生分裂或者Peer变更的时候，这个值会单调递增

在一个Raft Group中，Cell的副本数需要设置为基数，通常建议最少设置为3

### PD (Placement Driver)
PD is the managing component of the entire cluster and is in charge of the following two operations:

1. Storing the metadata of the cluster such as the cell location of a specific key.

2. Scheduling and load balancing cells in the store cluster, including but not limited to data migration and Raft group leader transfer.

As a cluster, PD needs to be deployed to an odd number of nodes. Usually it is recommended to deploy to 3 online nodes at least.

### Proxy (optional component)
The Proxy server is used for proxy the read and write operation for external application. The proxy is in charge of the following three operations:

1. Sync cells metadata to local from PD

2. Receive read and write operation from external application, and forward these operations to the correct store according to metadata and the key.

3. Receive the response from the store, and forwarding it to the external application. Resynchronize the metadata from PD when an raft error occurs, and retry forward the operation.

The proxy is stateless and can be scaled.