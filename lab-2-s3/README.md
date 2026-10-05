# Lab 2: Object storage with S3

Upload the datasets generated in [Lab 1](../README.md) (`users.csv`, `orders.csv`) to the bronze layer of the personal
S3 bucket, from a Kubernetes Job running on the Onyxia platform.

## Contents

| File | Description |
| --- | --- |
| [`job-upload-bronze.yaml`](job-upload-bronze.yaml) | Job running the AWS CLI image, which copies the two CSV files to `s3://$LAB_BUCKET_NAME/bronze/` |
| [`answers.md`](answers.md) | Answers to the questions at the end of the lab |

## How it works

- The datasets are provided to the pod by the `datasets` **ConfigMap**, mounted read-only in `/data`.
- The non-sensitive S3 settings (endpoint, region, bucket name) are in the `s3-config` **ConfigMap**.
- The credentials are in the `s3-credentials` **Secret**.
- Both `s3-config` and `s3-credentials` are injected as environment variables with `envFrom`.
- The container runs `aws s3 cp` for each dataset, then lists the bronze layer.

## Reproduce

```bash
# Environment
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile 'default' || echo "https://$AWS_S3_ENDPOINT")
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"

# Datasets (from the root of the repository)
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv

# ConfigMaps and Secret
kubectl create configmap datasets --from-file=users.csv --from-file=orders.csv
kubectl create configmap s3-config \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="$(aws configure get region --profile 'default')" \
  --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME"
aws configure export-credentials --profile 'default' --format env-no-export \
  | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > s3.env
kubectl create secret generic s3-credentials --from-env-file=s3.env
rm s3.env

# Job
kubectl apply -f lab-2-s3/job-upload-bronze.yaml
kubectl wait --for=condition=complete job/upload-bronze --timeout=120s
kubectl logs job/upload-bronze
```

Cleanup of the Kubernetes objects (the objects of the bronze layer are kept for the next modules):

```bash
kubectl delete -f lab-2-s3/job-upload-bronze.yaml
kubectl delete configmap datasets s3-config
kubectl delete secret s3-credentials
```

## Result

The two objects are in the bronze layer, with the expected sizes:

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
#> 2026-10-04 18:16:34     331568 bronze/orders.csv
#> 2026-10-04 18:16:14       7351 bronze/users.csv
```

## Issues encountered

The Job stayed in `Running 0/1` with no pod. `kubectl describe job upload-bronze` showed `FailedCreate` events from the
Job controller, caused by the namespace `ResourceQuota` (`onyxia-quota`).

1. **`must specify limits.cpu`**: the quota applies to CPU and memory limits, so every container must declare both. The
   manifest of the lab only set `limits.memory`. Fix: add `limits.cpu: 200m` (already done in
   [`job-upload-bronze.yaml`](job-upload-bronze.yaml)).
2. **`exceeded quota` (`limits.cpu` 4/4, `limits.memory` 4000Mi/4Gi)**: a service from a previous session
   (`vscode-python-782290`) was stuck in `Pending` and still counted in the quota, although it did not appear in the
   Onyxia portal. Fix: remove the stale Helm release.

```bash
   kubectl describe resourcequota onyxia-quota
   helm list --namespace "$KUBERNETES_NAMESPACE"
   helm uninstall vscode-python-782290 --namespace "$KUBERNETES_NAMESPACE"
```

A pod in `Pending` consumes quota because the quota is computed from the declared limits of existing pods, not from
the resources actually used. Services that are no longer needed should be deleted at the end of each session.