# CalmDB Docker Cluster Makefile

.PHONY: help build up down restart status logs clean health mysql

help:
	@echo "CalmDB Docker Cluster"
	@echo ""
	@echo "  make build    - Build Docker image"
	@echo "  make up       - Start 8-node cluster"
	@echo "  make down     - Stop cluster"
	@echo "  make restart  - Restart cluster"
	@echo "  make status   - Show status"
	@echo "  make logs     - Show logs"
	@echo "  make logs-1   - Show node-1 logs"
	@echo "  make clean    - Stop and remove all data"
	@echo "  make health   - Health check"
	@echo "  make mysql    - Connect MySQL"
	@echo "  make scale-4  - Scale to 4 nodes"

build:
	./cluster.sh build

up:
	./cluster.sh up

down:
	./cluster.sh down

restart:
	./cluster.sh restart

status:
	./cluster.sh status

logs:
	./cluster.sh logs

logs-%:
	./cluster.sh logs calm-node-$*

clean:
	./cluster.sh clean

health:
	./cluster.sh health

mysql:
	./cluster.sh mysql

scale-%:
	./cluster.sh scale $*
