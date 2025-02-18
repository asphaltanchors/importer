VERSION := $(shell python3 -c "import re;print(re.search('version=\"(.+?)\"', open('setup.py').read()).group(1))")
REGISTRY := ghcr.io/asphaltanchors
IMAGE_NAME := py-importer

.PHONY: build build-local tag push all

build:
	docker build --platform linux/amd64 -t $(IMAGE_NAME):$(VERSION) .

build-local:
	docker build -t $(IMAGE_NAME):$(VERSION) .

tag:
	docker tag $(IMAGE_NAME):$(VERSION) $(REGISTRY)/$(IMAGE_NAME):$(VERSION)
	docker tag $(IMAGE_NAME):$(VERSION) $(REGISTRY)/$(IMAGE_NAME):latest

push:
	docker push $(REGISTRY)/$(IMAGE_NAME):$(VERSION)
	docker push $(REGISTRY)/$(IMAGE_NAME):latest

# Build and push all
all: build tag push

# Help target
help:
	@echo "Available targets:"
	@echo "  build         - Build Docker image for linux/amd64"
	@echo "  build-local   - Build Docker image for local architecture"
	@echo "  tag          - Tag Docker image for registry"
	@echo "  push         - Push Docker image to registry"
	@echo "  all          - Build, tag, and push Docker image"
	@echo ""
	@echo "Current version: $(VERSION)"
