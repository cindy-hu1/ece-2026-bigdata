# Lab 2: Answers

## 1. Why are the credentials stored in a Secret and not in the ConfigMap?

A ConfigMap is meant for non-sensitive configuration: its content is plain text, readable by anyone allowed to read
ConfigMaps in the namespace, and shown by `kubectl describe` and in exports. A Secret is a dedicated resource type for
sensitive data. It can be protected with its own RBAC rules, it is hidden from `kubectl describe`, it is kept in memory
(tmpfs) when mounted in a pod, and encryption at rest can be enabled for it.

A Secret is only base64-encoded by default, which is not encryption: the real protection comes from restricting who can
read it and from encryption at rest. In this lab, the endpoint, region and bucket name are not sensitive and go in the
`s3-config` ConfigMap, while the access key, secret key and session token go in the `s3-credentials` Secret.

## 2. What happens if the Job is executed again tomorrow? How would a production platform provide credentials?

The credentials of Onyxia are temporary. The Secret contains a frozen copy of them, so tomorrow they are expired: the pod
starts, but `aws s3 cp` fails with an `ExpiredToken` (or `InvalidAccessKeyId`) error. With `backoffLimit: 2`, the Job
retries twice and then ends in a `Failed` state. The only fix is to renew the credentials manually and recreate the
Secret.

A production platform never relies on keys copied by hand. The usual approaches are:

- **Workload identity**: the pod gets short-lived credentials from its Kubernetes ServiceAccount (IAM Roles for Service
  Accounts or EKS Pod Identity on AWS, Workload Identity on GCP). No secret is stored in the cluster.
- **A secrets manager** (for example HashiCorp Vault with dynamic secrets), synchronized into the cluster by the
  External Secrets Operator or the Secrets Store CSI driver. The credentials are short-lived and rotated automatically.
- **Least privilege**: the policy attached to the identity only allows what the job needs (for example writing under the
  `bronze/` prefix).

## 3. How would you turn this Job into a daily ingestion?

Replace the `Job` by a `CronJob`: the pod template moves under `jobTemplate` and a `schedule` is added.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: upload-bronze
spec:
  schedule: "0 2 * * *" # every day at 02:00
  concurrencyPolicy: Forbid # never run two ingestions in parallel
  startingDeadlineSeconds: 3600
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 2
      template:
        spec: {} # same pod spec as in job-upload-bronze.yaml
```

For a real ingestion, a few more points are needed:

- read the data from the source system instead of a ConfigMap (limited to 1 MiB);
- make the job idempotent and write to a partitioned key per run (for example `bronze/orders/date=2026-10-05/`), so a
  retry does not create duplicates or overwrite other days;
- provide credentials with a workload identity (see question 2) and monitor failures with alerts;
- if several jobs depend on each other, use an orchestrator such as Airflow rather than a chain of CronJobs.