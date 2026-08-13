from __future__ import annotations

import json
import socket
import ssl
import tempfile
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

SERVER_NAME = "ray-head.phase0a.internal"


def private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def issue_ca(common_name: str) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = private_key()
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name(common_name))
        .issuer_name(name(common_name))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=2))
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
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def issue_leaf(
    ca_key: rsa.RSAPrivateKey,
    ca: x509.Certificate,
    *,
    common_name: str,
    server: bool,
) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = private_key()
    now = datetime.now(UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(name(common_name))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=True,
        )
    )
    if server:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(common_name), x509.IPAddress(ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
    return key, builder.sign(ca_key, hashes.SHA256())


def write_pair(path: Path, stem: str, key: rsa.RSAPrivateKey, cert: x509.Certificate) -> None:
    (path / f"{stem}.key").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    (path / f"{stem}.crt").write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def tls_round_trip(path: Path, ca_stem: str, server_stem: str, client_stem: str) -> str:
    server_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    server_context.minimum_version = ssl.TLSVersion.TLSv1_2
    server_context.verify_mode = ssl.CERT_REQUIRED
    server_context.load_cert_chain(path / f"{server_stem}.crt", path / f"{server_stem}.key")
    server_context.load_verify_locations(path / f"{ca_stem}.crt")
    client_context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH, cafile=path / f"{ca_stem}.crt"
    )
    client_context.minimum_version = ssl.TLSVersion.TLSv1_2
    client_context.load_cert_chain(path / f"{client_stem}.crt", path / f"{client_stem}.key")
    left, right = socket.socketpair()
    server = server_context.wrap_socket(left, server_side=True, do_handshake_on_connect=False)
    client = client_context.wrap_socket(
        right, server_hostname=SERVER_NAME, do_handshake_on_connect=False
    )
    import threading

    failure: list[BaseException] = []

    def handshake_server() -> None:
        try:
            server.do_handshake()
            server.recv(4)
            server.sendall(b"pong")
        except BaseException as exc:  # pragma: no cover - surfaced in caller
            failure.append(exc)

    thread = threading.Thread(target=handshake_server)
    thread.start()
    client.do_handshake()
    client.sendall(b"ping")
    response = client.recv(4).decode()
    thread.join(timeout=5)
    client.close()
    server.close()
    if failure:
        raise failure[0]
    return response


def expect_failure(path: Path, ca_stem: str, server_stem: str, client_stem: str) -> bool:
    try:
        tls_round_trip(path, ca_stem, server_stem, client_stem)
    except (ssl.SSLError, ConnectionError, OSError):
        return True
    return False


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="sceptre-tls-") as directory:
        path = Path(directory)
        old_key, old_ca = issue_ca("phase0a-old-ca")
        new_key, new_ca = issue_ca("phase0a-new-ca")
        rogue_key, rogue_ca = issue_ca("phase0a-rogue-ca")
        for stem, key, cert in (
            ("old-ca", old_key, old_ca),
            ("new-ca", new_key, new_ca),
            ("rogue-ca", rogue_key, rogue_ca),
        ):
            write_pair(path, stem, key, cert)
        for stem, key, cert in (
            ("old-server", *issue_leaf(old_key, old_ca, common_name=SERVER_NAME, server=True)),
            ("old-client", *issue_leaf(old_key, old_ca, common_name="worker", server=False)),
            ("new-server", *issue_leaf(new_key, new_ca, common_name=SERVER_NAME, server=True)),
            ("new-client", *issue_leaf(new_key, new_ca, common_name="worker", server=False)),
            ("rogue-client", *issue_leaf(rogue_key, rogue_ca, common_name="rogue", server=False)),
        ):
            write_pair(path, stem, key, cert)
        wrong_key, wrong_server = issue_leaf(
            new_key, new_ca, common_name="wrong.internal", server=True
        )
        write_pair(path, "wrong-server", wrong_key, wrong_server)

        results = {
            "old_root_before_rotation": tls_round_trip(
                path, "old-ca", "old-server", "old-client"
            )
            == "pong",
            "new_root_after_rotation": tls_round_trip(
                path, "new-ca", "new-server", "new-client"
            )
            == "pong",
            "old_root_rejected_after_rotation": expect_failure(
                path, "new-ca", "old-server", "new-client"
            ),
            "rogue_client_rejected": expect_failure(
                path, "new-ca", "new-server", "rogue-client"
            ),
            "wrong_server_name_rejected": expect_failure(
                path, "new-ca", "wrong-server", "new-client"
            ),
        }
        if not all(results.values()):
            raise RuntimeError(f"TLS qualification failed: {results}")
        print(json.dumps({"results": results, "status": "passed"}, sort_keys=True))


if __name__ == "__main__":
    main()
