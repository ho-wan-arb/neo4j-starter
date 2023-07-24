# neo4j-starter

A basic setup to get started with local neo4j development / experimentation.
This requires `docker`, `kubernetes` and `kind` being available on your machine.

## To run

### Start cluster and neo4j service
```sh
make cluster-start
make neo4j-start
```

### Portforward neo4j for browser

```sh
make neo4j-forward
```

Go to `localhost:7474` in browser, and login using credentials in neo4j.yaml

## Roadmap

 * [x] add Makefile for easy initialization
 * [ ] add some go starter code for neo4j connectivity
 * [ ] convert this to a nix flake (?)
