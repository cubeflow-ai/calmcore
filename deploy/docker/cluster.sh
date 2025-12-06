#!/bin/bash
# CalmDB Cluster Management Script
# Usage: ./cluster.sh [command]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}           ${GREEN}CalmDB Cluster Management${NC}                       ${BLUE}║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
}

print_usage() {
    echo ""
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  build       Build Docker image"
    echo "  up          Start the cluster (8 nodes)"
    echo "  down        Stop the cluster"
    echo "  restart     Restart the cluster"
    echo "  status      Show cluster status"
    echo "  logs        Show logs (use: logs [node-name])"
    echo "  clean       Stop cluster and remove all data"
    echo "  clean-data  Remove data volumes only (cluster must be stopped)"
    echo "  scale       Scale cluster (use: scale <num>)"
    echo "  exec        Execute command in container (use: exec <node> <cmd>)"
    echo "  mysql       Connect to MySQL on node-1"
    echo "  health      Check cluster health"
    echo ""
    echo "Examples:"
    echo "  $0 build              # Build the Docker image"
    echo "  $0 up                 # Start 8-node cluster"
    echo "  $0 logs calm-node-1   # View logs for node-1"
    echo "  $0 scale 4            # Scale to 4 nodes"
    echo "  $0 mysql              # Connect to MySQL"
    echo "  $0 clean              # Stop and remove all data"
    echo ""
}

cmd_build() {
    echo -e "${YELLOW}Building CalmDB Docker image...${NC}"
    cd "$PROJECT_ROOT"
    docker build -t calmdb:latest -f deploy/docker/Dockerfile .
    echo -e "${GREEN}✓ Build complete${NC}"
}

cmd_up() {
    echo -e "${YELLOW}Starting CalmDB cluster (8 nodes)...${NC}"
    
    # Check if image exists
    if ! docker image inspect calmdb:latest &> /dev/null; then
        echo -e "${YELLOW}Image not found, building...${NC}"
        cmd_build
    fi
    
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" up -d
    
    echo ""
    echo -e "${GREEN}✓ Cluster started${NC}"
    echo ""
    echo "Node endpoints:"
    echo "  Node 1: GraphQL=localhost:9567, ES=localhost:9200, MySQL=localhost:3307"
    echo "  Node 2: GraphQL=localhost:9568, ES=localhost:9201, MySQL=localhost:3308"
    echo "  Node 3: GraphQL=localhost:9569, ES=localhost:9202, MySQL=localhost:3309"
    echo "  Node 4: GraphQL=localhost:9570, ES=localhost:9203, MySQL=localhost:3310"
    echo "  Node 5: GraphQL=localhost:9571, ES=localhost:9204, MySQL=localhost:3311"
    echo "  Node 6: GraphQL=localhost:9572, ES=localhost:9205, MySQL=localhost:3312"
    echo "  Node 7: GraphQL=localhost:9573, ES=localhost:9206, MySQL=localhost:3313"
    echo "  Node 8: GraphQL=localhost:9574, ES=localhost:9207, MySQL=localhost:3314"
    echo ""
    echo "Connect to MySQL: mysql -h 127.0.0.1 -P 3307 -u root -pcalm"
}

cmd_down() {
    echo -e "${YELLOW}Stopping CalmDB cluster...${NC}"
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" down
    echo -e "${GREEN}✓ Cluster stopped${NC}"
}

cmd_restart() {
    echo -e "${YELLOW}Restarting CalmDB cluster...${NC}"
    cmd_down
    cmd_up
}

cmd_status() {
    echo -e "${YELLOW}Cluster status:${NC}"
    echo ""
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" ps
    echo ""
    
    # Check health of each node
    echo -e "${YELLOW}Health check:${NC}"
    for i in {1..8}; do
        port=$((9566 + i))
        if curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/" | grep -q "200\|404"; then
            echo -e "  Node $i: ${GREEN}✓ Healthy${NC} (GraphQL: $port)"
        else
            echo -e "  Node $i: ${RED}✗ Unhealthy${NC} (GraphQL: $port)"
        fi
    done
}

cmd_logs() {
    local node="${1:-}"
    cd "$SCRIPT_DIR"
    
    if [ -z "$node" ]; then
        echo -e "${YELLOW}Showing logs for all nodes (Ctrl+C to exit)...${NC}"
        docker-compose -f "$COMPOSE_FILE" logs -f
    else
        echo -e "${YELLOW}Showing logs for $node (Ctrl+C to exit)...${NC}"
        docker-compose -f "$COMPOSE_FILE" logs -f "$node"
    fi
}

cmd_clean() {
    echo -e "${RED}WARNING: This will stop the cluster and remove ALL data!${NC}"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Stopping cluster and removing data...${NC}"
        cd "$SCRIPT_DIR"
        docker-compose -f "$COMPOSE_FILE" down -v
        echo -e "${GREEN}✓ Cluster stopped and data removed${NC}"
    else
        echo "Cancelled."
    fi
}

cmd_clean_data() {
    echo -e "${RED}WARNING: This will remove ALL data volumes!${NC}"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Removing data volumes...${NC}"
        
        # Remove data volumes
        for i in {1..8}; do
            docker volume rm "docker_calm-data-$i" 2>/dev/null || true
            docker volume rm "docker_calm-logs-$i" 2>/dev/null || true
        done
        
        echo -e "${GREEN}✓ Data volumes removed${NC}"
    else
        echo "Cancelled."
    fi
}

cmd_scale() {
    local num="${1:-}"
    
    if [ -z "$num" ] || ! [[ "$num" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Error: Please specify number of nodes (1-8)${NC}"
        exit 1
    fi
    
    if [ "$num" -lt 1 ] || [ "$num" -gt 8 ]; then
        echo -e "${RED}Error: Number of nodes must be between 1 and 8${NC}"
        exit 1
    fi
    
    echo -e "${YELLOW}Scaling cluster to $num nodes...${NC}"
    
    # Stop nodes that exceed the target
    cd "$SCRIPT_DIR"
    for i in $(seq $((num + 1)) 8); do
        docker-compose -f "$COMPOSE_FILE" stop "calm-node-$i" 2>/dev/null || true
    done
    
    # Start nodes up to the target
    for i in $(seq 1 $num); do
        docker-compose -f "$COMPOSE_FILE" up -d "calm-node-$i"
    done
    
    echo -e "${GREEN}✓ Cluster scaled to $num nodes${NC}"
}

cmd_exec() {
    local node="${1:-}"
    shift
    local cmd="$@"
    
    if [ -z "$node" ]; then
        echo -e "${RED}Error: Please specify node name${NC}"
        exit 1
    fi
    
    if [ -z "$cmd" ]; then
        cmd="/bin/bash"
    fi
    
    docker exec -it "$node" $cmd
}

cmd_mysql() {
    echo -e "${YELLOW}Connecting to MySQL on node-1...${NC}"
    mysql -h 127.0.0.1 -P 3307 -u root -pcalm
}

cmd_health() {
    echo -e "${YELLOW}Checking cluster health...${NC}"
    echo ""
    
    local healthy=0
    local total=8
    
    for i in {1..8}; do
        local graphql_port=$((9566 + i))
        local es_port=$((9199 + i))
        local mysql_port=$((3306 + i))
        
        local graphql_status="${RED}✗${NC}"
        local es_status="${RED}✗${NC}"
        local mysql_status="${RED}✗${NC}"
        
        # Check GraphQL
        if curl -s -o /dev/null -w "%{http_code}" "http://localhost:$graphql_port/" 2>/dev/null | grep -q "200\|404"; then
            graphql_status="${GREEN}✓${NC}"
        fi
        
        # Check Elasticsearch
        if curl -s -o /dev/null -w "%{http_code}" "http://localhost:$es_port/_cluster/health" 2>/dev/null | grep -q "200"; then
            es_status="${GREEN}✓${NC}"
            ((healthy++)) || true
        fi
        
        # Check MySQL (just try to connect)
        if mysql -h 127.0.0.1 -P $mysql_port -u root -pcalm -e "SELECT 1" &>/dev/null; then
            mysql_status="${GREEN}✓${NC}"
        fi
        
        echo -e "Node $i: GraphQL=$graphql_status ES=$es_status MySQL=$mysql_status"
    done
    
    echo ""
    echo -e "Healthy nodes: $healthy/$total"
}

# Main
print_header

case "${1:-}" in
    build)
        cmd_build
        ;;
    up|start)
        cmd_up
        ;;
    down|stop)
        cmd_down
        ;;
    restart)
        cmd_restart
        ;;
    status|ps)
        cmd_status
        ;;
    logs)
        cmd_logs "${2:-}"
        ;;
    clean)
        cmd_clean
        ;;
    clean-data)
        cmd_clean_data
        ;;
    scale)
        cmd_scale "${2:-}"
        ;;
    exec)
        shift
        cmd_exec "$@"
        ;;
    mysql)
        cmd_mysql
        ;;
    health)
        cmd_health
        ;;
    *)
        print_usage
        ;;
esac
