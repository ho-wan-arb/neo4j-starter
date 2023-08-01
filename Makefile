.PHONY: cluster-start
cluster-start:
	@./scripts/cluster.sh


.PHONY: neo4j-start
neo4j-start:
	@kubectl apply -k ./deployment/kind

.PHONY: neo4j-forward
neo4j-forward:
	kubectl port-forward svc/neo4jsvc 7474:7474

.PHONY: cluster-use
cluster-use:
	@kubectl config use-context kind-dev-cluster

.PHONY: docker-start
docker-start:
	@docker start dev-cluster-control-plane

.PHONY: docker-stop
docker-stop:
	@docker stop dev-cluster-control-plane
