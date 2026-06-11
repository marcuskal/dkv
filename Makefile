.PHONY: build test test-short run clean cluster-clean \
        docker-build docker-push \
        kind-up kind-down kind-load \
        k8s-apply k8s-delete \
        helm-install helm-upgrade helm-uninstall helm-template helm-lint

# --- Variables ---
IMAGE_REPO   ?= ghcr.io/marcuskal/dkv
IMAGE_TAG    ?= dev
IMAGE        := $(IMAGE_REPO):$(IMAGE_TAG)
VERSION      ?= 0.7.0
COMMIT       := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
KIND_CLUSTER ?= dkv
NAMESPACE    ?= dkv
RELEASE      ?= dkv
HELM_CHART_DIR := ./internal/deploy/helm/dkv

# --- Build ---
build:
	go build -o bin/dkv ./cmd/dkv

# --- Test ---
test:
	go test -v -race -count=1 ./...

test-short:
	go test -v -race -short ./...

# --- Run (single node, dev) ---
run: build
	./bin/dkv --config dkv.yaml

# --- Local 3-node cluster ---
run-node1: build
	./bin/dkv --config dkv.yaml

run-node2: build
	./bin/dkv --config dkv-node2.yaml

run-node3: build
	./bin/dkv --config dkv-node3.yaml

cluster-clean:
	rm -rf /tmp/dkv/node-*

clean:
	rm -rf bin/
	rm -rf /tmp/dkv

# ========================================================================
# Container & Kubernetes targets
# ========================================================================

# --- Docker ---
docker-build:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		-t $(IMAGE) \
		.

docker-push: docker-build
	docker push $(IMAGE)

# --- kind (local K8s for dev) ---
# https://kind.sigs.k8s.io/
kind-up:
	kind create cluster --name $(KIND_CLUSTER) --config deploy/kind-config.yaml || \
	kind create cluster --name $(KIND_CLUSTER)

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

# Load the locally-built image into kind nodes (avoids pushing to a registry).
kind-load: docker-build
	kind load docker-image $(IMAGE) --name $(KIND_CLUSTER)

# --- Raw kubectl ---
k8s-apply:
	kubectl apply -f deploy/k8s/dkv.yaml

k8s-delete:
	kubectl delete -f deploy/k8s/dkv.yaml --ignore-not-found
	kubectl -n $(NAMESPACE) delete pvc -l app=dkv --ignore-not-found

# --- Helm ---
helm-lint:
	helm lint deploy/helm/dkv

helm-template:
	helm template $(RELEASE) deploy/helm/dkv \
		--namespace $(NAMESPACE) \
		--set image.tag=$(IMAGE_TAG)

helm-install:
	helm install dkv $(HELM_CHART_DIR) --namespace dkv --create-namespace --set image.tag=$(IMAGE_TAG) --wait --timeout 5m

helm-upgrade:
	helm upgrade $(RELEASE) deploy/helm/dkv \
		--namespace $(NAMESPACE) \
		--set image.tag=$(IMAGE_TAG) \
		--wait \
		--timeout 5m

helm-uninstall:
	helm uninstall $(RELEASE) --namespace $(NAMESPACE)
	@echo "PVCs are NOT auto-deleted. To remove them:"
	@echo "  kubectl -n $(NAMESPACE) delete pvc -l app.kubernetes.io/instance=$(RELEASE)"

# --- Convenience: full local round-trip ---
# Build image, load into kind, install via Helm.
deploy-local: kind-load helm-install
	@echo "DKV deployed. Watch with:"
	@echo "  kubectl -n $(NAMESPACE) get pods -w"