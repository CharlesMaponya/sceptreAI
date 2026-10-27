# Inference pod identity

New deployments use `deployment.reconcile.v4` and the `inference-pod-tls-v1`
worker. Admission requires an existing serving authority and an authenticated
HTTPS control endpoint. A missing authority returns a controlled admission
error before native writes. Original v3 journals remain readable for retirement.

This conversion is in progress. The gateway conversion and owned native
qualification must pass before the complete inference path is qualified.

## Provision the authority

Create one authority for the API replicas. Keep its private key outside the
repository and retain it in your secret backup system. Helm references an
existing Secret; it does not generate or rotate this key.

```bash
umask 077
inference_authority_dir="$(mktemp -d /tmp/sceptre-inference-authority.XXXXXXXX)"
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 \
  -out "$inference_authority_dir/ca.key"
openssl req -new -x509 -sha256 -days 365 \
  -key "$inference_authority_dir/ca.key" \
  -out "$inference_authority_dir/ca.crt" \
  -subj '/CN=Sceptre inference authority' \
  -addext 'basicConstraints=critical,CA:TRUE,pathlen:0' \
  -addext 'keyUsage=critical,digitalSignature,keyCertSign,cRLSign'
```

Choose the intended cluster and namespace before provisioning the Secret:

```bash
kubectl --context YOUR_CONTEXT --namespace YOUR_NAMESPACE create secret generic \
  sceptre-inference-authority \
  --from-file=ca.crt="$inference_authority_dir/ca.crt" \
  --from-file=ca.key="$inference_authority_dir/ca.key"
```

Configure the chart:

```yaml
inference:
  identity:
    caSecret: sceptre-inference-authority
attemptIdentity:
  controlBaseUrl: https://app.example.com/api/v1/internal/attempts
```

The adjacent `/api/v1/internal/deployments` routes must reach the API through
that HTTPS endpoint. Its certificate must be trusted by the worker's system
trust store. The serving authority above authenticates the separate pod TLS
connection; it does not replace the control endpoint's HTTPS certificate.

The API mounts `ca.crt` and `ca.key` read-only with group access for UID 10001.
Migration containers and other Deployments receive neither this mount nor its
file-path settings. Existing broad controller Secret permissions remain a
separate Phase 1 trust and least-privilege obligation.

## Worker and network contracts

| Connection | Contract |
| --- | --- |
| API to an original inference Pod | TCP 8443, mutual TLS, exact original pod identity |
| Kubelet to inference health | TCP 8080, only `/health/live` and `/health/ready` |
| Worker to the configured control endpoint | HTTPS, current projected pod token, no redirects or environment proxy |
| Worker model download | Original bound pod identity and verified immutable model bytes |

Apply these flows to the cluster's actual network policy and Gateway topology.
Rendering a chart or testing local listeners does not qualify a native network
policy. Production already forbids direct inference Ingress; viewers use the
authenticated platform Gateway.

TLS deployments do not advertise raw Service DNS URLs. Their certificates
identify individual original Pods, and the platform gateway supplies the
matching TLS identity after admission.

The worker generates its own P-256 key in a memory-backed volume. Container
restarts retain that key; another key cannot renew the same pod binding.
Certificates live for at most ten minutes, and failed renewal withdraws
prediction readiness. No platform JWT secret, deployment HMAC, database or
object-store credential is injected into the TLS worker.

Rotate the authority through a new Secret and redeploy the affected inference
workloads. Existing workload declarations pin the original authority digest;
they cannot silently rebind to a replacement authority. Preserve original
deployment journals for retirement rather than rewriting their identities.
