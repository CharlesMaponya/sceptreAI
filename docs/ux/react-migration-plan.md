# Sceptre React UI migration plan

## Status

Completed. The React application is the sole supported UI and the transitional
Streamlit application has been removed from the repository.

## Product verdict

The former Streamlit application proved the initial platform workflow but was
not suitable as the long-term customer experience.

The main usability constraints are:

- all project activity is compressed into four large nested tabs;
- long-running profiling and training work triggers whole-page reruns;
- setup, review, and destructive operational actions have similar visual weight;
- the interface exposes backend objects before explaining the user's next decision;
- narrow screens, keyboard navigation, shareable URLs, and durable client state are limited;
- loading, empty, partial-success, and recovery states are inconsistent.

The React application changes the information architecture while preserving the
FastAPI contract. The primary journey is **Data → Train → Results & validation
→ Deploy & monitor**, with project/team management kept separate.

## Brand direction

The design tokens are derived from `sceptre-logo.png`:

- Sceptre cobalt `#3159E8` for primary actions and active navigation;
- deep blue `#17213D` for text and governance-oriented surfaces;
- warm ivory `#FCFAF5` for welcoming authentication surfaces;
- restrained success, warning, and danger colors for operational state;
- Manrope display type with DM Sans body type, with system fallbacks.

The logo is used as the visual anchor, not as a background decoration. The UI
keeps a high information density without borrowing Streamlit's notebook feel.

## Phased implementation

### Phase 0 — audit and contract map

- Inventory every Streamlit screen and FastAPI route.
- Identify the core jobs-to-be-done and failure/recovery states.
- Treat project UUIDs as the navigation and cache isolation boundary.
- Preserve refresh-token rotation, resource preflight, and RBAC behavior.

Acceptance: every current capability has a destination in the React information
architecture, and API gaps are documented rather than hidden in UI code.

### Phase 1 — foundation and design system

- Vite, strict TypeScript, React Router, and TanStack Query.
- Cobalt/ivory design tokens, reusable controls, status badges, notices, modals,
  empty states, metrics, and responsive shell.
- Login/registration and durable session handling with one-request token refresh.
- Project dashboard, create/join project, switching, and protected routes.

Acceptance: keyboard-usable authentication and project navigation work at mobile,
tablet, and desktop widths; expired sessions return to sign-in cleanly.

### Phase 2 — data and profiling

- Dataset upload with file type feedback and upload progress affordance.
- Immutable version picker and metadata summary.
- Non-blocking profile status polling, inferred task summary, quality flags, and
  feature table.
- Preparation and relationship details can be progressively disclosed without
  blocking the main readiness decision.

Acceptance: a user can upload a supported file, leave the screen while profiling,
return to current status, and understand whether the data is ready to train.

### Phase 3 — training and evidence

- Guided training configuration: data, problem framing, candidates, and budget.
- Explicit resource estimate gate with cluster blockers and warnings.
- Progressive run list and leaderboard with cancel/restart recovery.
- Validation and SHAP analysis history attached to the source training run.

Acceptance: payloads match API schemas, no run can launch without a current
estimate, and active status updates do not reset the page.

### Phase 4 — registry, deployment, and collaboration

- Registry cards with lifecycle stage, champion evidence, and explicit fallback.
- Deployment launch/status/stop controls and endpoint links.
- Drift entry points and cleanup controls should require contextual confirmation.
- Project members and secure, expiring invitation tokens.

Acceptance: stage and deployment actions show status immediately, production
deployment is unavailable for non-production entries, and operational failures
remain recoverable.

### Phase 5 — hardening and cutover

- Unit tests for session refresh, payload builders, and state transitions.
- Component/integration tests with Mock Service Worker.
- Playwright happy paths against a disposable API/database.
- WCAG 2.2 AA pass, reduced-motion support, responsive review, bundle budget, and
  security review of token storage and content security policy.
- Run Streamlit and React in parallel for one release, collect task completion
  evidence, then make React the default and retain Streamlit only as an internal
  tool until feature parity is signed off.

Acceptance: production build and automated tests pass; representative users can
complete upload-to-deploy tasks without developer assistance; rollback remains
available during the parallel release.

## Use-case validation

The redesign optimizes for four primary roles:

1. An analyst brings data, understands quality, and frames the target.
2. A data scientist compares candidates and investigates diagnostics.
3. A reviewer validates, explains, and approves a model with evidence.
4. An operator promotes, deploys, monitors, and safely stops a model.

The application should be evaluated by task completion rate, time to first
successful run, training-launch error rate, percentage of promoted models with
validation evidence, and recovery success after a failed job—not by page views.
