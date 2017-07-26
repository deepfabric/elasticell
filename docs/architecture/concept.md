## Concept
To better understand Elasticell’s architecture, you need to understand the Elasticell concept.

### Store
Store is a service that runs on a single disk. A store manage many cells, and provide data storage for cells, and dispatch messages to cells.

### Cell
A cell manage data in range of [start, end), every cell has some replication on different stores, we called these replications Peer. All cell replications with has same id makes up a raft group. Cell's peer maybe scheduled by PD move to another store, or removed from the store.

A cell has three meta data:

1. ID, The uniquely identifies of a cell

2. Data range, Decide which data is managed by the cell

3. Peers, Cell replications

4. Epoch, The version of the cell, it will increase every time the peers changes or cell split

In the raft group, number of cell replications needs to be set an odd number. Usually it is recommended to set to 3 at least.

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