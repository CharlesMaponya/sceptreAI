# Legacy Kustomize resources

These manifests remain a low-level development reference for the stateful
dependencies. They are not the supported complete application installer.

Use [`infra/helm/sceptre`](../helm/sceptre/README.md) for portable installation
of PostgreSQL, MinIO, MLflow, migrations, API, UI, RBAC, and workload settings.
Cluster-specific image import and port-forward commands belong outside the
application in the Helm installation guide.
