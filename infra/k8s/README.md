# Legacy Kustomize resources

These manifests remain a low-level development reference for the stateful
dependencies. Their Sceptre images use `0.0.0` placeholders; they are not the
supported complete application installer.

Use [`infra/helm/sceptre`](../helm/sceptre/README.md) for portable installation
of PostgreSQL, SeaweedFS, MLflow, migrations, API, UI, RBAC, and workload settings.
Cluster-specific image import and port-forward commands belong outside the
application in the Helm installation guide.
