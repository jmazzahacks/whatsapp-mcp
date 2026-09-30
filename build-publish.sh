#!/usr/bin/env bash
# Build and publish MCP Docker image
# Usage: ./build-publish.sh [--no-cache]

# Anchor to the script's directory so VERSION, Dockerfile, and `.` all
# resolve to the project root — not the caller's cwd. Without this,
# running as `./myapp/build-publish.sh` from a parent directory writes
# a stray VERSION in the parent, uses the parent as build context (which
# in a multi-repo workspace can sweep sibling projects into the image),
# and corrupts this project's version tracking.
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

REGISTRY="ghcr.io/jmazzahacks/whatsapp-mcp"

NO_CACHE=""
for arg in "$@"; do
    if [ "$arg" = "--no-cache" ]; then
        NO_CACHE="--no-cache"
    fi
done

# Seed at 0, not 1: the version published is CURRENT+1, so seeding
# at 1 makes the very first image :2 and leaves :1 permanently missing
# from the registry.
if [ ! -f VERSION ]; then
    echo "0" > VERSION
fi

CURRENT_VERSION=$(cat VERSION)

case "$CURRENT_VERSION" in
    ''|*[!0-9]*)
        echo "ERROR: VERSION file contains non-numeric value: $CURRENT_VERSION"
        exit 1
        ;;
esac

NEXT_VERSION=$((CURRENT_VERSION + 1))

echo "Building ${REGISTRY}:${NEXT_VERSION}..."

docker build \
    --platform linux/amd64 \
    $NO_CACHE \
    -t "${REGISTRY}:${NEXT_VERSION}" \
    .

if [ $? -ne 0 ]; then
    echo "ERROR: Docker build failed"
    exit 1
fi

docker tag "${REGISTRY}:${NEXT_VERSION}" "${REGISTRY}:latest"
if [ $? -ne 0 ]; then
    echo "ERROR: Docker tag failed"
    exit 1
fi

echo "Pushing ${REGISTRY}:${NEXT_VERSION}..."
docker push "${REGISTRY}:${NEXT_VERSION}"
if [ $? -ne 0 ]; then
    echo "ERROR: Push failed"
    exit 1
fi

echo "Pushing ${REGISTRY}:latest..."
docker push "${REGISTRY}:latest"
if [ $? -ne 0 ]; then
    echo "ERROR: Push failed"
    exit 1
fi

echo "$NEXT_VERSION" > VERSION
echo "Published ${REGISTRY}:${NEXT_VERSION} and :latest"
