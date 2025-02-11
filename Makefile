VERSION := $(shell poetry version -s)  # Extract version from pyproject.toml
REGISTRY := ghcr.io/asphaltanchors
IMAGE_NAME := py-importer

.PHONY: build tag push all version-patch version-minor version-major

build:
	docker build --platform linux/amd64 -t $(IMAGE_NAME):$(VERSION) .

tag:
	docker tag $(IMAGE_NAME):$(VERSION) $(REGISTRY)/$(IMAGE_NAME):$(VERSION)
	docker tag $(IMAGE_NAME):$(VERSION) $(REGISTRY)/$(IMAGE_NAME):latest

push:
	docker push $(REGISTRY)/$(IMAGE_NAME):$(VERSION)
	docker push $(REGISTRY)/$(IMAGE_NAME):latest

# Version bumping targets
version-patch:
	poetry version patch
	@echo "Bumped to version: $(shell poetry version -s)"

version-minor:
	poetry version minor
	@echo "Bumped to version: $(shell poetry version -s)"

version-major:
	poetry version major
	@echo "Bumped to version: $(shell poetry version -s)"

# Build and push all
all: build tag push

# Help target
help:
	@echo "Available targets:"
	@echo "  build         - Build Docker image with current version"
	@echo "  tag          - Tag Docker image for registry"
	@echo "  push         - Push Docker image to registry"
	@echo "  all          - Build, tag, and push Docker image"
	@echo "  version-patch - Bump patch version (0.1.0 -> 0.1.1)"
	@echo "  version-minor - Bump minor version (0.1.0 -> 0.2.0)"
	@echo "  version-major - Bump major version (0.1.0 -> 1.0.0)"
	@echo ""
	@echo "Current version: $(VERSION)"
