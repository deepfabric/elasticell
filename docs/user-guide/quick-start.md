## Quick Start
You can quickly test Elasticell with Docker on a machine.

### Install Docker
To install Docker on your system, you can read the document on https://docs.docker.com/

### Pull Elasticell
```bash
docker pull deepfabric/quickstart
```

### Run Elasticell with Docker
```bash
docker run -d --name elasticell -p 6379:6379 deepfabric/quickstart
```

### Test
You can use redis-cli to connect to Elasticell. 