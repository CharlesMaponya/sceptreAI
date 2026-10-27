"""Real TLS peer checks happen before any HTTP credential or data is sent."""

import asyncio
import ssl
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from automl_api.services.inference_tls import (
    Authority,
    PeerIdentity,
    ca_certificate,
    client_context,
    csr_public_key,
    make_csr,
    new_private_key,
    private_key_pem,
    public_key_digest,
    server_context,
    validate_server_certificate,
)
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def authority_material(*, expires=None):
    key = new_private_key()
    now = datetime.now(UTC)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "owned-test-inference-ca")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(expires or now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM), private_key_pem(key)


@pytest.fixture
def authority():
    return Authority.from_pem(*authority_material())


@pytest.fixture
def peer():
    return PeerIdentity(*(str(uuid.uuid4()) for _ in range(5)))


def test_certificate_scope_and_possession_are_exact(authority, peer):
    key = new_private_key()
    csr = make_csr(key)
    cert = authority.issue_server(csr, peer, datetime.now(UTC) + timedelta(seconds=300))
    assert public_key_digest(csr_public_key(csr)) == public_key_digest(key.public_key())
    assert validate_server_certificate(authority.pem, cert, peer, key) > datetime.now(UTC)
    leaf = x509.load_pem_x509_certificate(cert)
    assert leaf.extensions.get_extension_for_class(x509.BasicConstraints).value.ca is False
    assert list(leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value) == [
        ExtendedKeyUsageOID.SERVER_AUTH
    ]
    for field in ("project_id", "run_id", "namespace_uid", "workload_uid", "pod_uid"):
        changed = replace(peer, **{field: str(uuid.uuid4())})
        assert changed.hostname != peer.hostname
        with pytest.raises(ValueError):
            validate_server_certificate(authority.pem, cert, changed, key)
    with pytest.raises(ValueError):
        validate_server_certificate(authority.pem, cert, peer, new_private_key())


@pytest.mark.parametrize("seconds", [-1, 601])
def test_certificate_cannot_outlive_its_grant(authority, peer, seconds):
    with pytest.raises(ValueError):
        authority.issue_server(
            make_csr(new_private_key()), peer, datetime.now(UTC) + timedelta(seconds=seconds)
        )


def test_csr_cannot_select_client_or_ca_powers(authority, peer):
    key = new_private_key()
    csr = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "api.inference.sceptre.invalid")])
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=True)
        .sign(key, hashes.SHA256())
        .public_bytes(serialization.Encoding.PEM)
    )
    cert = authority.issue_server(csr, peer, datetime.now(UTC) + timedelta(seconds=300))
    validate_server_certificate(authority.pem, cert, peer, key)
    leaf = x509.load_pem_x509_certificate(cert)
    assert list(leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value) == [
        ExtendedKeyUsageOID.SERVER_AUTH
    ]
    assert not leaf.extensions.get_extension_for_class(x509.KeyUsage).value.key_cert_sign


def test_authority_and_csr_reject_malformed_or_misbound_material(authority):
    with pytest.raises(ValueError):
        Authority.from_pem(authority.pem, private_key_pem(new_private_key()))
    with pytest.raises(ValueError):
        ca_certificate(authority.pem + authority.pem)
    with pytest.raises(ValueError):
        Authority.from_pem(*authority_material(expires=datetime.now(UTC) - timedelta(seconds=1)))
    for csr in (b"", b"not a csr", b"x" * (16 * 1024 + 1)):
        with pytest.raises(ValueError):
            csr_public_key(csr)
    with pytest.raises(ValueError):
        PeerIdentity("", "run", "namespace", "deployment", "pod")


@pytest.mark.parametrize(
    "fault",
    ["none", "project_id", "run_id", "namespace_uid", "workload_uid", "pod_uid", "other-ca"],
)
def test_real_https_rejects_wrong_peer_before_headers_or_body(authority, peer, fault):
    async def check():
        key = new_private_key()
        serving_peer = replace(peer, **{fault: str(uuid.uuid4())}) if fault in vars(peer) else peer
        issuer = Authority.from_pem(*authority_material()) if fault == "other-ca" else authority
        certificate = issuer.issue_server(
            make_csr(key), serving_peer, datetime.now(UTC) + timedelta(seconds=300)
        )
        ctx = server_context(issuer.pem, certificate, private_key_pem(key))
        received, consumed = [], []

        async def handle(reader, writer):
            try:
                headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 2)
                body = await asyncio.wait_for(reader.readuntil(b"0\r\n\r\n"), 2)
                received.append((headers, body))
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        async def body():
            consumed.append(True)
            yield b'{"private_feature":7}'

        server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=ctx)
        async with server:
            port = server.sockets[0].getsockname()[1]
            async with httpx.AsyncClient(
                verify=authority.client_context(), trust_env=False, timeout=3
            ) as client:
                request = client.build_request(
                    "POST",
                    f"https://127.0.0.1:{port}/predict",
                    headers={"X-Test-Credential": "must-not-reach-a-foreign-peer"},
                    content=body(),
                )
                request.extensions["sni_hostname"] = peer.hostname
                if fault == "none":
                    response = await client.send(request)
                    assert response.status_code == 200 and len(received) == len(consumed) == 1
                    assert (
                        b"X-Test-Credential" in received[0][0]
                        and b"private_feature" in received[0][1]
                    )
                else:
                    with pytest.raises(httpx.ConnectError):
                        await client.send(request)
                    assert not received and not consumed

    asyncio.run(check())


def test_server_certificate_cannot_authenticate_as_api_client(authority, peer):
    async def check():
        key = new_private_key()
        certificate = authority.issue_server(
            make_csr(key), peer, datetime.now(UTC) + timedelta(seconds=300)
        )
        received = []

        async def handle(reader, writer):
            received.append(await reader.read(1024))
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(
            handle,
            "127.0.0.1",
            0,
            ssl=server_context(authority.pem, certificate, private_key_pem(key)),
        )
        async with server:
            # TLS1.2 completes mutual authentication before allowing HTTP writes.
            ctx = client_context(authority.pem, certificate, private_key_pem(key))
            ctx.maximum_version = ssl.TLSVersion.TLSv1_2
            async with httpx.AsyncClient(verify=ctx, trust_env=False, timeout=3) as client:
                request = client.build_request(
                    "GET", f"https://127.0.0.1:{server.sockets[0].getsockname()[1]}/"
                )
                request.extensions["sni_hostname"] = peer.hostname
                with pytest.raises(httpx.ConnectError):
                    await client.send(request)
        assert not received

    asyncio.run(check())
