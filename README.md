# neo4j-starter

A basic setup to get started with local neo4j development / experimentation.

## Roadmap

 * [x] add Makefile for easy initialization
 * [x] add some go starter code for neo4j connectivity
 * [ ] convert this to a nix flake (?)

## Ways to run

### Using Kind on docker

This requires `docker`, `kubernetes` and `kind` being available on your machine.

Go to `localhost:7474` in browser, and login using credentials in neo4j.yaml

#### Start cluster and neo4j service
```sh
make cluster-start
make neo4j-start
```

#### Portforward neo4j for browser

```sh
make neo4j-forward
```

### Using docker image

```sh
docker run --rm \
  --env NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
  --env NEO4J_AUTH=neo4j/password \
  --publish=7687:7687 \
  --health-cmd 'cypher-shell -u neo4j -p password "RETURN 1"' \
  --health-interval 5s \
  --health-timeout 5s \
  --health-retries 5 \
  neo4j:4.4-enterprise
```

Once the server is running, you run integration tests like this:

```sh
TEST_NEO4J_HOST="localhost" TEST_NEO4J_VERSION=4.4 \
  go test ./neo4j/test-integration/...
```

### Using Neo4j Desktop (recommended as its slightly faster)

Requires installation of Java SDK

https://neo4j.com/download/
