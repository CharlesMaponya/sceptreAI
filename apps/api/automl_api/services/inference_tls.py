"""Pod-scoped serving certificates and mutually authenticated TLS contexts."""

from __future__ import annotations

import hashlib
import json
import os
import ssl
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

PROTOCOL = "inference-pod-tls-v1"
DATA_PORT = 8443
HEALTH_PORT = 8080
MAX_CERTIFICATE_SECONDS = 600
MAX_PEM_BYTES = 16 * 1024
TOKEN_PATH = "/var/run/sceptre-inference-token/token"
IDENTITY_DIRECTORY = "/var/run/sceptre-inference"


def audience(run_id, ca_digest):
    return f"sceptre-inference:{run_id}:{ca_digest}"


def credential_volumes(run_id, ca_digest):
    return [
        {
            "name": "inference-token",
            "projected": {
                "sources": [
                    {
                        "serviceAccountToken": {
                            "audience": audience(run_id, ca_digest),
                            "expirationSeconds": 600,
                            "path": "token",
                        }
                    }
                ]
            },
        },
        {"name": "inference-identity", "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"}},
    ]


def credential_mounts():
    return [
        {"name": "inference-token", "mountPath": TOKEN_PATH.rsplit("/", 1)[0], "readOnly": True},
        {"name": "inference-identity", "mountPath": IDENTITY_DIRECTORY},
    ]


def serving_declaration(
    manifests, *, project_id, run_id, model_digest, model_uri, ca_digest=None, base_url=None
):
    pod = manifests["deployment"]["spec"]["template"]["spec"]
    containers = pod["containers"]
    if len(containers) != 1:
        raise ValueError("Inference requires one original serving container")
    container = containers[0]
    env = container.get("env") or []
    values = {item["name"]: item.get("value") for item in env}
    allowed = {
        "MODEL_URI",
        "MODEL_SHA256",
        "MODEL_DOWNLOAD_URL",
        "MODEL_NAME",
        "PROJECT_NAME",
        "DEPLOYMENT_ENVIRONMENT",
        "INFERENCE_IDENTITY_PROTOCOL",
        "INFERENCE_CA_SHA256",
        "INFERENCE_DEPLOYMENT_ID",
        "INFERENCE_PROJECT_ID",
        "INFERENCE_IDENTITY_URL",
    }
    digest = values.get("INFERENCE_CA_SHA256")
    endpoint = values.get("INFERENCE_IDENTITY_URL", "")
    control = urlsplit(endpoint)
    if (
        len(values) != len(env)
        or set(values) - allowed
        or any(set(item) != {"name", "value"} or not isinstance(item["value"], str) for item in env)
        or values.get("INFERENCE_IDENTITY_PROTOCOL") != PROTOCOL
        or values.get("INFERENCE_DEPLOYMENT_ID") != str(run_id)
        or values.get("INFERENCE_PROJECT_ID") != str(project_id)
        or values.get("MODEL_SHA256") != model_digest
        or values.get("MODEL_URI") != model_uri
        or any(
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
            for value in (digest, model_digest)
        )
        or ca_digest is not None
        and digest != ca_digest
        or control.scheme != "https"
        or not control.hostname
        or control.username is not None
        or control.password is not None
        or control.query
        or control.fragment
        or not control.path.endswith(f"/internal/deployments/{run_id}/identity")
        or base_url is not None
        and endpoint != base_url + f"/{run_id}/identity"
        or values.get("MODEL_DOWNLOAD_URL") != endpoint.removesuffix("/identity") + "/model"
        or pod.get("automountServiceAccountToken") is not False
        or not pod.get("serviceAccountName")
        or pod.get("volumes") != credential_volumes(run_id, digest)
        or pod.get("securityContext")
        != {"fsGroup": 10001, "seccompProfile": {"type": "RuntimeDefault"}}
        or set(pod)
        - {
            "containers",
            "serviceAccountName",
            "automountServiceAccountToken",
            "securityContext",
            "volumes",
            "imagePullSecrets",
            "nodeSelector",
        }
        or any(
            pod.get(field)
            for field in (
                "initContainers",
                "ephemeralContainers",
                "hostNetwork",
                "hostPID",
                "hostIPC",
            )
        )
        or container.get("command") != ["python", "-m", "automl_api.inference.runtime"]
        or set(container)
        - {
            "name",
            "image",
            "imagePullPolicy",
            "ports",
            "env",
            "resources",
            "command",
            "volumeMounts",
            "startupProbe",
            "readinessProbe",
            "livenessProbe",
            "securityContext",
        }
        or container.get("args")
        or container.get("envFrom")
        or container.get("lifecycle")
        or container.get("securityContext")
        != {
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
            "runAsNonRoot": True,
            "runAsUser": 10001,
        }
        or container.get("volumeMounts") != credential_mounts()
        or container.get("ports")
        != [
            {"name": "https", "containerPort": DATA_PORT},
            {"name": "health", "containerPort": HEALTH_PORT},
        ]
        or any(
            (container.get(probe) or {}).get("httpGet") != {"path": path, "port": "health"}
            for probe, path in (
                ("startupProbe", "/health/ready"),
                ("readinessProbe", "/health/ready"),
                ("livenessProbe", "/health/live"),
            )
        )
        or manifests["service"]["spec"]["ports"]
        != [{"name": "https", "port": DATA_PORT, "targetPort": "https"}]
    ):
        raise ValueError("Inference credential declaration differs from its original scope")
    return values


@dataclass(frozen=True)
class PeerIdentity:
    project_id: str
    run_id: str
    namespace_uid: str
    workload_uid: str
    pod_uid: str

    def __post_init__(self):
        if any(
            type(value) is not str or not 0 < len(value) <= 128 for value in asdict(self).values()
        ):
            raise ValueError("Inference identity requires bounded original scope identifiers")

    @property
    def hostname(self):
        digest = hashlib.sha256(
            json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return f"{digest[:32]}.{digest[32:]}.inference.sceptre.invalid"


def new_private_key():
    return ec.generate_private_key(ec.SECP256R1())


def private_key_pem(key):
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


def public_key_digest(key):
    return hashlib.sha256(
        key.public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    ).hexdigest()


def _p256(key):
    if not isinstance(key, ec.EllipticCurvePublicKey) or not isinstance(key.curve, ec.SECP256R1):
        raise ValueError("The inference identity protocol requires a P-256 key")


def make_csr(key):
    _p256(key.public_key())
    return (
        x509.CertificateSigningRequestBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "sceptre-inference")]))
        .sign(key, hashes.SHA256())
        .public_bytes(serialization.Encoding.PEM)
    )


def csr_public_key(pem):
    if (
        not isinstance(pem, bytes)
        or not 0 < len(pem) <= MAX_PEM_BYTES
        or pem.count(b"-----BEGIN CERTIFICATE REQUEST-----") != 1
    ):
        raise ValueError("A bounded inference certificate request is required")
    request = x509.load_pem_x509_csr(pem)
    if not request.is_signature_valid:
        raise ValueError("Inference certificate request has no valid possession proof")
    key = request.public_key()
    _p256(key)
    return key


def ca_certificate(pem):
    if (
        not isinstance(pem, bytes)
        or not 0 < len(pem) <= MAX_PEM_BYTES
        or pem.count(b"-----BEGIN CERTIFICATE-----") != 1
    ):
        raise ValueError("One bounded inference CA certificate is required")
    certificate = x509.load_pem_x509_certificate(pem)
    _p256(certificate.public_key())
    certificate.verify_directly_issued_by(certificate)
    constraints = certificate.extensions.get_extension_for_class(x509.BasicConstraints)
    usage = certificate.extensions.get_extension_for_class(x509.KeyUsage)
    now = datetime.now(UTC)
    if (
        not constraints.critical
        or not constraints.value.ca
        or constraints.value.path_length != 0
        or not usage.value.key_cert_sign
        or not certificate.not_valid_before_utc <= now < certificate.not_valid_after_utc
    ):
        raise ValueError("Inference CA policy or validity is invalid")
    return certificate


@dataclass(frozen=True)
class Authority:
    certificate: x509.Certificate
    private_key: ec.EllipticCurvePrivateKey = field(repr=False)

    @classmethod
    def from_pem(cls, certificate, key):
        cert = ca_certificate(certificate)
        if not isinstance(key, bytes) or not 0 < len(key) <= MAX_PEM_BYTES:
            raise ValueError("A bounded inference CA private key is required")
        private = serialization.load_pem_private_key(key, password=None)
        _p256(private.public_key())
        if public_key_digest(private.public_key()) != public_key_digest(cert.public_key()):
            raise ValueError("Inference CA certificate and private key differ")
        return cls(cert, private)

    @property
    def pem(self):
        return self.certificate.public_bytes(serialization.Encoding.PEM)

    @property
    def digest(self):
        return self.certificate.fingerprint(hashes.SHA256()).hex()

    def _issue(self, public_key, hostname, purpose, expires_at):
        now = datetime.now(UTC)
        if expires_at.tzinfo is None or not now < expires_at <= min(
            now + timedelta(seconds=MAX_CERTIFICATE_SECONDS), self.certificate.not_valid_after_utc
        ):
            raise ValueError("Inference certificate lifetime exceeds its authority or grant")
        if not self.certificate.not_valid_before_utc <= now < self.certificate.not_valid_after_utc:
            raise ValueError("Inference CA is not currently valid")
        certificate = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "sceptre-inference")]))
            .issuer_name(self.certificate.subject)
            .public_key(public_key)
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(seconds=30))
            .not_valid_after(expires_at)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(hostname)]), critical=False)
            .add_extension(x509.ExtendedKeyUsage([purpose]), critical=True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False)
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(self.certificate.public_key()),
                critical=False,
            )
            .sign(self.private_key, hashes.SHA256())
        )
        return certificate.public_bytes(serialization.Encoding.PEM)

    def issue_server(self, csr, peer, expires_at):
        return self._issue(
            csr_public_key(csr), peer.hostname, ExtendedKeyUsageOID.SERVER_AUTH, expires_at
        )

    def client_context(self):
        key = new_private_key()
        certificate = self._issue(
            key.public_key(),
            "api.inference.sceptre.invalid",
            ExtendedKeyUsageOID.CLIENT_AUTH,
            min(
                datetime.now(UTC) + timedelta(seconds=MAX_CERTIFICATE_SECONDS - 1),
                self.certificate.not_valid_after_utc,
            ),
        )
        return client_context(self.pem, certificate, private_key_pem(key))


def _load_chain(context, certificate, key):
    with tempfile.TemporaryDirectory(prefix="sceptre-inference-tls-") as directory:
        paths = []
        for name, content in (("certificate.pem", certificate), ("key.pem", key)):
            path = Path(directory) / name
            with os.fdopen(
                os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb"
            ) as output:
                output.write(content)
            paths.append(path)
        context.load_cert_chain(str(paths[0]), str(paths[1]))


def client_context(ca, certificate, key):
    root = ca_certificate(ca)
    context = ssl.create_default_context(
        cadata=root.public_bytes(serialization.Encoding.PEM).decode("ascii")
    )
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.hostname_checks_common_name = False
    _load_chain(context, certificate, key)
    return context


def server_context(ca, certificate, key):
    root = ca_certificate(ca)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(
        cadata=root.public_bytes(serialization.Encoding.PEM).decode("ascii")
    )
    _load_chain(context, certificate, key)
    return context


def validate_server_certificate(ca, certificate, peer, key):
    root = ca_certificate(ca)
    if (
        not isinstance(certificate, bytes)
        or not 0 < len(certificate) <= MAX_PEM_BYTES
        or certificate.count(b"-----BEGIN CERTIFICATE-----") != 1
    ):
        raise ValueError("One bounded inference server certificate is required")
    leaf = x509.load_pem_x509_certificate(certificate)
    leaf.verify_directly_issued_by(root)
    names = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    purposes = leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    constraints = leaf.extensions.get_extension_for_class(x509.BasicConstraints).value
    now = datetime.now(UTC)
    if (
        list(names) != [x509.DNSName(peer.hostname)]
        or list(purposes) != [ExtendedKeyUsageOID.SERVER_AUTH]
        or constraints.ca
        or public_key_digest(leaf.public_key()) != public_key_digest(key.public_key())
        or not leaf.not_valid_before_utc
        <= now
        < leaf.not_valid_after_utc
        <= now + timedelta(seconds=MAX_CERTIFICATE_SECONDS)
    ):
        raise ValueError("Inference certificate differs from its original peer or key")
    return leaf.not_valid_after_utc
