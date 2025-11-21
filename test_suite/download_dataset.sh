#!/bin/bash

# 下载开放数据集用于测试
#
# 支持的数据集：
#   1. nyc-taxi - 纽约出租车数据（推荐）
#   2. github-events - GitHub 事件数据
#   3. stackoverflow - Stack Overflow 帖子数据
#
# 使用方法：
#   ./download_dataset.sh [dataset_name] [size]
#
# 参数：
#   dataset_name: nyc-taxi | github-events | stackoverflow
#   size: small | medium | large

set -e

DATASET=${1:-nyc-taxi}
SIZE=${2:-small}

# 颜色
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}=== Dataset Downloader ===${NC}\n"

# 创建数据目录
mkdir -p datasets
cd datasets

case $DATASET in
    nyc-taxi)
        echo -e "${BLUE}📦 Downloading NYC Taxi dataset...${NC}"
        echo "Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
        echo
        
        case $SIZE in
            small)
                # 2024年1月的数据（约100万条）
                FILE="yellow_tripdata_2024-01.parquet"
                URL="https://d37ci6vzurychx.cloudfront.net/trip-data/${FILE}"
                ROWS="~1M rows"
                ;;
            medium)
                # 2024年1-3月的数据（约300万条）
                FILES=("yellow_tripdata_2024-01.parquet" "yellow_tripdata_2024-02.parquet" "yellow_tripdata_2024-03.parquet")
                ROWS="~3M rows"
                ;;
            large)
                # 2024年全年数据（约1200万条）
                FILES=()
                for month in {01..12}; do
                    FILES+=("yellow_tripdata_2024-${month}.parquet")
                done
                ROWS="~12M rows"
                ;;
        esac
        
        echo "Size: $SIZE ($ROWS)"
        echo
        
        if [ "$SIZE" = "small" ]; then
            echo "Downloading: $FILE"
            if [ ! -f "$FILE" ]; then
                curl -L -o "$FILE" "$URL"
                echo -e "${GREEN}✓ Downloaded: $FILE${NC}"
            else
                echo -e "${YELLOW}File already exists: $FILE${NC}"
            fi
        else
            for FILE in "${FILES[@]}"; do
                URL="https://d37ci6vzurychx.cloudfront.net/trip-data/${FILE}"
                echo "Downloading: $FILE"
                if [ ! -f "$FILE" ]; then
                    curl -L -o "$FILE" "$URL"
                    echo -e "${GREEN}✓ Downloaded: $FILE${NC}"
                else
                    echo -e "${YELLOW}File already exists: $FILE${NC}"
                fi
            done
        fi
        
        echo
        echo -e "${GREEN}✓ NYC Taxi dataset ready${NC}"
        echo
        echo "Dataset info:"
        echo "  - Pickup/dropoff timestamps"
        echo "  - Location IDs"
        echo "  - Trip distance, fare amount"
        echo "  - Passenger count"
        echo
        echo "Next steps:"
        echo "  python3 ../load_dataset.py nyc-taxi"
        ;;
        
    github-events)
        echo -e "${BLUE}📦 Downloading GitHub Events dataset...${NC}"
        echo "Source: https://www.gharchive.org/"
        echo
        
        # GitHub Archive 数据（每小时一个文件）
        case $SIZE in
            small)
                # 1天的数据
                DATE="2024-01-01"
                ROWS="~100K events"
                ;;
            medium)
                # 1周的数据
                ROWS="~700K events"
                ;;
            large)
                # 1个月的数据
                ROWS="~3M events"
                ;;
        esac
        
        echo "Size: $SIZE ($ROWS)"
        echo
        echo -e "${YELLOW}Note: GitHub Events dataset requires processing${NC}"
        echo "We'll use a pre-processed sample instead"
        echo
        
        # 创建示例数据
        cat > github_events_sample.json << 'EOF'
{"id":"123","type":"PushEvent","actor":"user1","repo":"org/repo","created_at":"2024-01-01T00:00:00Z"}
{"id":"124","type":"IssueEvent","actor":"user2","repo":"org/repo2","created_at":"2024-01-01T00:01:00Z"}
EOF
        
        echo -e "${GREEN}✓ Sample data created${NC}"
        ;;
        
    stackoverflow)
        echo -e "${BLUE}📦 Stack Overflow dataset...${NC}"
        echo "Source: https://archive.org/details/stackexchange"
        echo
        echo -e "${YELLOW}Note: Full dataset is very large (>50GB)${NC}"
        echo "We recommend using NYC Taxi dataset instead"
        echo
        echo "Alternative: Use the sample data generator"
        ;;
        
    *)
        echo -e "${RED}Unknown dataset: $DATASET${NC}"
        echo
        echo "Available datasets:"
        echo "  - nyc-taxi (recommended)"
        echo "  - github-events"
        echo "  - stackoverflow"
        exit 1
        ;;
esac

echo
echo -e "${BLUE}=== Download Complete ===${NC}"
