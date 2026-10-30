# Phase 1 command-family idempotency coverage

This records current branch evidence for P1-W04 and P1-W05. It does not certify the full phase or all deployed/native and historical behaviors.

The inventory compares explicit handler names and registered route metadata with the entire application's OpenAPI header set. Header validation uses each actual annotation through FastAPI before handler execution. The production source scan covers repository Python under `apps` and `packages`, honoring ignored vendor directories, and checks constructors, explicit SQL inserts and named key readers. Injected extra constructors and raw SQL are detected by its controls.

## Public header boundaries

| Handler | Method and path | Required |
| --- | --- | --- |
| `contracts.post_experiment_spec` | `POST /api/v1/projects/{project_id}/experiment-specs` | Yes |
| `contracts.post_feature_contract` | `POST /api/v1/projects/{project_id}/feature-contracts` | Yes |
| `contracts.post_scope` | `POST /api/v1/projects/{project_id}/training/evaluation-scopes` | Yes |
| `contracts.post_scope_cancel` | `POST /api/v1/projects/{project_id}/training/evaluation-scopes/{scope_id}/cancel` | Yes |
| `contracts.post_scope_member` | `POST /api/v1/projects/{project_id}/training/evaluation-scopes/{scope_id}/members` | Yes |
| `contracts.post_search_objective` | `POST /api/v1/projects/{project_id}/search-objectives` | Yes |
| `contracts.post_search_space` | `POST /api/v1/projects/{project_id}/feature-search-spaces` | Yes |
| `datasets.abort_resumable_upload` | `POST /api/v1/projects/{project_id}/datasets/uploads/{session_id}/abort` | Yes |
| `datasets.begin_resumable_upload` | `POST /api/v1/projects/{project_id}/datasets/uploads` | Yes |
| `datasets.complete_resumable_upload` | `POST /api/v1/projects/{project_id}/datasets/uploads/{session_id}/complete` | Yes |
| `datasets.create_upload_instruction` | `POST /api/v1/projects/{project_id}/datasets/uploads/{session_id}/instructions` | Yes |
| `datasets.replay_upload_cleanup` | `POST /api/v1/projects/{project_id}/datasets/uploads/{session_id}/cleanup/replay` | Yes |
| `datasets.upload_dataset` | `POST /api/v1/projects/{project_id}/datasets/upload` | Yes |
| `monitoring.create_deployment_metric` | `POST /api/v1/projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/metrics` | Yes |
| `monitoring.create_governance_report` | `POST /api/v1/projects/{project_id}/operations/deployments/{deployment_run_id}/governance/reports` | Yes |
| `operations.cleanup` | `POST /api/v1/projects/{project_id}/operations/cleanup` | Yes |
| `operations.deploy` | `POST /api/v1/projects/{project_id}/operations/registry/{entry_id}/deployments` | Yes |
| `operations.drift` | `POST /api/v1/projects/{project_id}/operations/registry/{entry_id}/drift` | Yes |
| `training.estimate` | `POST /api/v1/projects/{project_id}/training/estimate` | Only when reserving capacity |
| `training.launch` | `POST /api/v1/projects/{project_id}/training/runs` | Yes |
| `validation.explain_model` | `POST /api/v1/projects/{project_id}/training/runs/{training_run_id}/explanations` | Yes |
| `validation.validate_model` | `POST /api/v1/projects/{project_id}/training/runs/{training_run_id}/validations` | Yes |

Every listed header uses minimum length 1, maximum length 255 and a non-whitespace constraint. The explicit inventory includes upload cleanup replay, which was missing from the old count assertion.

## Request identity and replay evidence

The command table scopes keys by project and operation. The only explicit production constructor is `workflow_state.begin_command`: it computes the canonical request hash, stores the request and resolves an insert race by comparing the committed record before allowing replay.

New HTTP/PostgreSQL tests stop at the first runtime effect callback after inspecting the actual inserted command. The failed callback rolls back. A separately committed pending command with that exact captured request then returns a stable pending response for identical requests. Changing a meaningful body field, file bytes or resource path produces a distinct stable reuse conflict before the callback can execute again. This covers buffered upload, reserved training estimate, training launch, validation, explainability, drift, deployment, governance, monitoring ingestion and cleanup. Authentication is overridden to an owned test user; project authorization, handlers and SQL are real. Runtime planning/effects are test doubles.

Existing complete-response, authorization, legacy, resumable upload and worker replay suites supplement those pending-response controls. Passing a suite does not establish every historical protocol or every deployed ingress/provider behavior.

## Explicit key lookup boundaries

| Source | Function | Validation/replay behavior | Qualification suites |
| --- | --- | --- | --- |
| `apps/api/automl_api/services/inference_identity.py` | `bound_peer` | Original peer/key/request hash and scope checks before serving credentials. | `test_inference_identity.py` |
| `apps/api/automl_api/services/scope_allocation.py` | `_command` | Canonical allocation intent and stored hash; central authority readiness remains a separate requirement. | `test_scope_allocation.py` |
| `apps/api/automl_api/services/training.py` | `_replay_derived_training_launch` | Legacy explicit action fields and lineage; current outer action commands also use begin_command. | `test_training_action_replay.py` |
| `apps/api/automl_api/services/training_metadata.py` | `_command` | Anchored metadata intent; unowned matching-key history is refused. | `test_training_tracking_metadata_api.py` |
| `apps/api/automl_api/services/training_tracking.py` | `_known_command` | Original tracking request/hash/source receipt validation. | `test_training_tracking_api.py` |
| `apps/api/automl_api/services/training_tracking.py` | `control_training_tracking` | Existing unowned matching-key history is refused rather than adopted. | `test_training_tracking_api.py` |
| `apps/api/automl_api/services/upload_admission.py` | `_lookup` | Incoming and stored hashes, original actor and project-scoped operation. | `test_upload_admission_workflow.py` |
| `apps/api/automl_api/services/upload_admission_cleanup.py` | `_state` | Original cleanup and approval-chain request hashes/source anchors. | `test_upload_cleanup_replay.py` |
| `apps/api/automl_api/services/upload_admission_cleanup.py` | `replay_cleanup` | Fresh owner, reason/session identity and authenticated original approval chain; no second epoch on replay. | `test_upload_cleanup_replay.py` |
| `apps/api/automl_api/services/upload_instructions.py` | `_lookup` | Original actor/session, full stored hash and logical instruction request. | `test_upload_instruction_workflow.py` |
| `apps/api/automl_api/services/workflow_state.py` | `_command_by_key` | Canonical hash comparison in begin_command, including concurrent insertion. | `test_command_response_replay.py; test_phase1_postgres_contracts.py` |

## Producer and consumer integration defect

Public analysis mutation commands store the operation, such as `validation.launch`, as their resource type. Analysis submission, observation, cleanup and legacy migration accepted only the older physical `model_run` label. Fresh valid API requests could therefore return acceptance while dispatch stayed pending.

Consumers now accept the physical label or the command's own exact operation. They still require the operation to match the run kind, along with all existing resource, actor, project, identity and fence checks. The producer and persisted requests are unchanged. New tests use actual HTTP admission and PostgreSQL outbox records with a prepared plan double, then the actual factory dispatcher/native SDK boundary. They cover all three analysis kinds, repeated API responses before and after original Job cleanup, and qualified legacy first delivery.

## Remaining qualification

The current qualification passes **1,127 distinct tests**: 23 header, 144 contract, 6 producer/consumer, 2 concurrency, 49 legacy, 198 upload, 274 worker and 431 analysis cases. The owned zero-Pod k3d validation probe uses the public `validation.launch` resource label, accepts the original observation, holds a changed command without another native read and retires the original UID. Its namespace is removed normally; capacity sampling is a test double. Strict schema remains revision 0021 with 44 tables. [Source fingerprints and evidence](evidence/phase-1/idempotency-coverage-2026-10-29.yaml) preserve the measured limitations. Formal gates remain 0 met /3 unmet; Phase 1 remains 2 Verified /34 Open. A full deployed ingress/provider matrix, a historical protocol corpus, previous application binaries, actor/native-context qualification, complete running workloads, production-sized migration and full branch coverage remain unqualified. In particular, the HTTP hash tests stop before runtime effects and do not prove crash recovery of every effect. The buffered upload path still performs its object-store write inside the outer SQL mutation; its kill/recovery boundary needs separate remediation before Phase 1 can be declared ready.
