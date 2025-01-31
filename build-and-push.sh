#!/bin/bash
set -euo pipefail

# Function for error handling
handle_error() {
    echo "Error occurred in build process"
    exit 1
}
trap handle_error ERR

# Validate environment
if ! command -v dotenv &> /dev/null; then
    echo "dotenv is not installed"
    exit 1
fi

if [ ! -f .env ]; then
    echo ".env file not found"
    exit 1
fi

# Check for uncommitted changes
if [[ -n $(git status -s) ]]; then
    echo "Warning: You have uncommitted changes"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Setup build arguments
BUILD_ARGS="--pull --no-cache=false"
if [[ "${1:-}" == "--no-cache" ]]; then
    BUILD_ARGS="--pull --no-cache=true"
fi

# Get the git hash
GIT_HASH=$(git rev-parse --short HEAD)
IMAGE_NAME="ghcr.io/${GITHUB_USER:-$USER}/importer"

echo "Building image with tag: $GIT_HASH"

# Build the image with database secret
dotenv docker build $BUILD_ARGS \
    --secret id=DATABASE_URL,env=DATABASE_URL \
    -t $IMAGE_NAME:latest \
    -t $IMAGE_NAME:$GIT_HASH .

echo "Pushing images to registry..."

# Push both tags
docker push $IMAGE_NAME:latest
docker push $IMAGE_NAME:$GIT_HASH

echo "Successfully built and pushed: $IMAGE_NAME:$GIT_HASH"
