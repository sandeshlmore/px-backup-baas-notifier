# PX-Backup-Notifier

 PX-Backup-Notifier manages notifications of px-backup deployment status sent to central UI.

## Dev Setup

### Build

Running `make build` will create a binary named `controller` at the root of the project.

The docker image can be built using `make docker-build`.

### Running locally

set environment variable locally: export webhookUrl=http://localhost:8000 

To run the controller locally, run `make run kubeconfig=(PATH_TO_KUBECONFIG_FILE)`

### Deploy on k8s cluster

To get manifest run `make manifest`

Running `make deploy` will deploy the controller to the cluster.

make sure latest `image`  and `webhookUrl` environment variable is set in `manifest/controller.yaml` before running `make deploy` command