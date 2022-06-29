
check-webhook-url-configured:
ifndef WEBHOOK_URL
	$(WEBHOOK_URL ENV is undefined)
endif

.PHONY: all
all: build
	
##@ Build

.PHONY: build
build: fmt vet ## Build controller binary.
	go build -o controller .

.PHONY: run 
run: check-webhook-url-configured fmt vet ## Run a controller from your host.
	go run . --kubeconfig=$(kubeconfig)

.PHONY: docker-build
docker-build: ## Build docker image .
	docker build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker imager.
	docker push ${IMG}

##@ Development

.PHONY: manifest
manifest: 
	kubectl kustomize ./manifests

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

##@ Deployment

.PHONY: deploy
deploy:  ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	kubectl kustomize ./manifests | kubectl apply -f -

.PHONY: undeploy
undeploy: ## Undeploy controller from the K8s cluster specified in ~/.kube/config.
	kubectl kustomize ./manifests | kubectl delete  -f -