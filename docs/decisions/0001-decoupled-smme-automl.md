# ADR 0001: Decoupled SMME-Safe AutoML Platform

## Status

Accepted

## Context

The platform must serve small teams on constrained shared Kubernetes clusters. It needs clear isolation, low operational overhead, and a UI that does not require users to operate Kubernetes or MLOps tools directly.

## Decision

Use a decoupled architecture:

- Streamlit for the analytical frontend
- FastAPI for business logic, RBAC, metadata, and orchestration
- PostgreSQL for relational metadata
- Object storage for datasets, model artifacts, diagnostics, and logs
- Kubernetes Jobs for ephemeral training and explainability workloads

Project UUIDs are the primary isolation boundary across relational data and object-storage prefixes.

## Consequences

The API becomes the only trusted service boundary. The Streamlit app must not read the database, object storage, or Kubernetes API directly.

Training workloads can fail independently without taking down the UI or API.

The schema must denormalize `project_id` onto isolated records so every query can enforce project authorization cheaply and consistently.

## Kubernetes Priority Note

The requested behavior is that AutoML jobs yield to production workloads. A low numeric `PriorityClass` achieves that. The base manifest uses `preemptionPolicy: Never` so AutoML jobs do not evict other workloads while still being candidates for preemption by higher-priority business workloads.
