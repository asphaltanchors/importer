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

# Default settings
BUILD_ARGS="--pull"
SHOULD_PUSH=true
BUILD_MODE="local"
USE_CACHE=true

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cache)
            USE_CACHE=false
            shift
            ;;
        --build-only)
            SHOULD_PUSH=false
            shift
            ;;
        --local)
            BUILD_MODE="local"
            shift
            ;;
        --prod)
            BUILD_MODE="prod"
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 [--no-cache] [--build-only] [--local|--prod]"
            exit 1
            ;;
    esac
done

# Set up build arguments based on mode and cache settings
if [ "$USE_CACHE" = false ]; then
    BUILD_ARGS="$BUILD_ARGS --no-cache=true"
else
    BUILD_ARGS="$BUILD_ARGS --no-cache=false"
fi

if [ "$BUILD_MODE" = "prod" ]; then
    BUILD_ARGS="$BUILD_ARGS --platform linux/amd64"
    echo "Building for production (AMD64)"
else
    echo "Building for local architecture"
fi

# Configuration
readonly IMAGE_NAME="ghcr.io/asphaltanchors/importer"

# Get the git hash
GIT_HASH=$(git rev-parse --short HEAD)

echo "Building image with tag: $GIT_HASH"

# Build the image with database secret
dotenv docker build $BUILD_ARGS \
    --secret id=DATABASE_URL,env=DATABASE_URL \
    -t $IMAGE_NAME:latest \
    -t $IMAGE_NAME:$GIT_HASH \
    --build-arg TARGETARCH=$([ "$BUILD_MODE" = "prod" ] && echo "amd64" || echo "arm64") .

if $SHOULD_PUSH; then
    echo "Pushing images to registry..."

    # Push both tags
    docker push $IMAGE_NAME:latest
    docker push $IMAGE_NAME:$GIT_HASH

    echo "Successfully built and pushed: $IMAGE_NAME:$GIT_HASH"
else
    echo "Build completed successfully. Skipping push as --build-only was specified."
fi
