## Architecture
To better understand Elasticell’s features, you need to understand the Elasticell architecture.

![architecture](../imgs/architecture.png)

The Elasticell has threee components: PD server, cell server, and proxy server.

### __Compatible with Redis protocol__
Use Elasticell as Redis. You can replace Redis with Elasticell to power your application without changing a single line of code in most cases([unsupport-redis-commands](./unsupport-command.md)).

### __Horizontal scalability__
Grow Elasticell as your business grows. You can increase the capacity simply by adding more machines.

### __Strong consistent persistence storage__
Elasticell put your data on multiple machines as replication without worrying about consistency. Elasticell makes your application use redis as a database and not just only the cache.

### __High availability__
All of the three components, PD, Cell and Proxy, can tolerate the failure of some instances without impacting the availability of the entire cluster.