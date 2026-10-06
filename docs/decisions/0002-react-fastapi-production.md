# ADR 0002: React/FastAPI production architecture

Status: Accepted for the implemented architecture, 2026-09-24. Supersedes ADR 0001's Streamlit frontend decision. Production qualification remains separate.

The shipped frontend is React/Vite, served by Nginx. FastAPI owns authentication, authorization, metadata and durable workflow commands. PostgreSQL stores workflow state; object storage stores datasets and artifacts. A separate reconciler submits fenced ephemeral KubeRay workloads. Ray Data and Polars handle distributed preparation. Candidate training currently mixes Ray task execution and candidate-local search; the production guide's full Tune/Train/refit/evaluation contract remains work in progress.

Helm is the supported complete deployment boundary. Legacy Kustomize and partial Compose configurations are evaluation/development aids. The browser must not access PostgreSQL or Kubernetes directly. Authorized direct-to-object-store upload uses scoped transfer instructions from the API.

This decision records the existing implementation and supersedes obsolete frontend documentation; it does not approve large-data capacity, cloud spend, security exceptions or a production release. Those decisions require the dated evidence and approvals in the production-readiness guide.
