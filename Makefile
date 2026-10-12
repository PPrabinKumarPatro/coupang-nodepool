SHELL := /usr/bin/bash
.SHELLFLAGS := -eu -o pipefail -c
GO ?= go
KUBECTL ?= kubectl
IMG ?= coupang-nodepool:dev
CONTROLLER_GEN := ./bin/controller-gen

.PHONY: help build test test-e2e demo fmt vet lint manifests generate envtest docker-build require-kubeconfig install deploy undeploy uninstall

help:
	@printf '%s\n' \
	  'build          Build a static local manager (does not generate files)' \
	  'test           Run unit and isolated envtest tests; KUBEBUILDER_ASSETS required' \
	  'lint           Check gofmt and run go vet without installing tools' \
	  'generate       Regenerate DeepCopy using the existing controller-gen' \
	  'manifests      Regenerate CRD/RBAC using the existing controller-gen' \
	  'envtest        Explicitly obtain checksum-verified, pinned envtest assets' \
	  'docker-build   Build the local controller image; no base image download/push' \
	  'test-e2e       Disposable three-node QA; requires explicit opt-in' \
	  'demo           Alias for the narrated, asserted disposable QA demonstration' \
	  'install/deploy/uninstall/undeploy require an explicit KUBECONFIG file'

build:
	CGO_ENABLED=0 $(GO) build -mod=readonly -trimpath -o bin/manager ./cmd

test:
	@test -n "$${KUBEBUILDER_ASSETS:-}" || { echo 'Set KUBEBUILDER_ASSETS; see make envtest.' >&2; exit 2; }
	USE_EXISTING_CLUSTER=false KUBECONFIG=/dev/null $(GO) test -mod=readonly -race -count=1 -timeout=4m ./...

fmt:
	gofmt -w api cmd internal test

vet:
	$(GO) vet -mod=readonly ./...

lint:
	@test -z "$$(gofmt -l api cmd internal test)" || { gofmt -l api cmd internal test; exit 1; }
	$(MAKE) vet

generate:
	@test -x "$(CONTROLLER_GEN)" || { echo 'controller-gen v0.22.0 is required; no automatic install.' >&2; exit 2; }
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt,year=2026 paths=./api/...

manifests:
	@test -x "$(CONTROLLER_GEN)" || { echo 'controller-gen v0.22.0 is required; no automatic install.' >&2; exit 2; }
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd paths=./... output:crd:artifacts:config=config/crd/bases

envtest:
	@bash hack/envtest-assets.sh

docker-build: build
	docker build --network=none --pull=false -t "$(IMG)" .

test-e2e:
	bash hack/e2e.sh

demo:
	bash hack/demo.sh

require-kubeconfig:
	@test -n "$${KUBECONFIG:-}" && test -f "$$KUBECONFIG" || { echo 'An explicit, existing KUBECONFIG file is required.' >&2; exit 2; }

install: require-kubeconfig
	$(KUBECTL) --kubeconfig "$$KUBECONFIG" apply -k config/crd

deploy: require-kubeconfig
	$(KUBECTL) --kubeconfig "$$KUBECONFIG" apply -k config/default

undeploy: require-kubeconfig
	$(KUBECTL) --kubeconfig "$$KUBECONFIG" delete deployment coupang-nodepool-controller-manager -n coupang-nodepool-system --ignore-not-found

uninstall: require-kubeconfig
	@echo 'Refusing automatic CRD removal: finish all pool finalizers first, then remove the CRD explicitly.' >&2
	@exit 2
