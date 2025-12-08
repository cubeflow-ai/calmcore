#!/bin/bash
# Build CalmCore Docker image
# Usage: ./build.sh [tag]

set -e

TAG=${1:-calmcore:latest}

echo "🐳 Building CalmCore Docker image: $TAG"
echo "📂 Build context: project root"
echo "📦 Using Docker BuildKit cache"

# Navigate to project root (2 levels up from this script)
cd "$(dirname "$0")/../.."

# Enable BuildKit for cache support
export DOCKER_BUILDKIT=1

# Build with Dockerfile in deploy/docker/
docker build --network=host \
    -f deploy/docker/Dockerfile \
    -t "$TAG" \
    .

echo "✅ Build complete: $TAG"
echo ""
echo "Run with:"
echo "  docker run -p 9567:9567 -p 9200:9200 -p 3307:3307 $TAG"
