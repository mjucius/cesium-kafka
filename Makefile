# cesium-kafka — convenience entry points. `make help` lists targets.
# The demo/up/down targets need Docker; build/test need a JDK (21 is auto-provisioned).
COMPOSE := docker compose -f config/docker-compose.yaml

.DEFAULT_GOAL := help
.PHONY: help demo up down logs build test image

help: ## List targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

demo: ## Run the self-driving demo: schedule out-of-order delayed events, watch them arrive on time
	$(COMPOSE) --profile demo down -v          # start clean so arrivals are watched live
	$(COMPOSE) up -d --build
	$(COMPOSE) --profile demo run --rm --no-deps demo
	@echo ""
	@echo "The stack is still running. Explore metrics: curl -s localhost:8081/metrics | grep '^cesium_'"
	@echo "Tear it down (and wipe data) with: make down"

up: ## Bring up the relay stack (detached) for manual experimentation
	$(COMPOSE) up -d --build
	@echo "cesium is starting on http://localhost:8081 (health: /health/ready, metrics: /metrics)."

down: ## Stop the stack and delete its data volumes
	$(COMPOSE) --profile demo down -v

logs: ## Follow cesium's logs
	$(COMPOSE) logs -f cesium

build: ## Compile + unit tests (no Docker)
	./gradlew build

test: ## Integration tests against real Kafka (needs Docker)
	./gradlew :cesium-kafka-it:integrationTest

image: ## Build the Docker image only
	docker build -t cesium-kafka:local .
