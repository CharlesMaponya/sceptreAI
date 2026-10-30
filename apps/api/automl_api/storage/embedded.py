from __future__ import annotations

import errno
import io
import os
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path

from automl_api.storage.contracts import (
    BeginUpload,
    ByteRange,
    CompletedObject,
    HealthcheckResult,
    ObjectMetadata,
    ProviderUpload,
    RayDataSourceDescriptor,
    TransferCursor,
    TransferReceipt,
    UploadCapabilities,
    UploadCapabilityUnavailable,
    UploadContractError,
    UploadInstruction,
    UploadProgress,
    embedded_object_version,
)


class EmbeddedObjectStoreDriver:
    driver_name = "embedded"
    _identity_attribute = "user.sceptre_incarnation"

    def __init__(self, *, root: Path, bucket: str) -> None:
        if not bucket or bucket in {".", ".."} or Path(bucket).name != bucket:
            raise ValueError("Embedded bucket must be one directory name.")
        self.root = root
        self.bucket = bucket

    @contextmanager
    def _locked(self):
        # Conditional unlink and every managed replacement share one process-safe lock.
        self.root.mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            import msvcrt

            with (self.root / ".sceptre-store.lock").open("a+b") as lock:
                if lock.tell() == 0:
                    lock.write(b"0")
                    lock.flush()
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    yield
                finally:
                    lock.seek(0)
                    msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                yield
            finally:
                os.close(descriptor)

    def _stamp(self, descriptor, *, required):
        version = "embedded-v1:" + uuid.uuid4().hex
        try:
            os.setxattr(descriptor, self._identity_attribute, version.encode("ascii"))
        except (AttributeError, OSError) as exc:
            if isinstance(exc, OSError) and exc.errno not in {errno.ENOTSUP, errno.EOPNOTSUPP}:
                raise
            if required:
                raise UploadCapabilityUnavailable(
                    "Qualified local creation requires durable file incarnation attributes."
                ) from exc
            return None
        return version

    def _incarnation(self, descriptor):
        try:
            version = os.getxattr(descriptor, self._identity_attribute).decode("ascii")
            return embedded_object_version(version)
        except (AttributeError, OSError, UnicodeError) as exc:
            raise UploadContractError("Local file lost its original incarnation evidence.") from exc

    def _original(self, uri, version):
        source = self._path_from_uri(uri).open("rb")
        try:
            if version is not None:
                embedded_object_version(version)
                if self._incarnation(source.fileno()) != version:
                    raise FileNotFoundError("Original embedded incarnation is absent.")
            return source
        except BaseException:
            source.close()
            raise

    def _sync_directory(self, path):
        if os.name != "nt":
            root = self.root.absolute()
            path = path.absolute()
            for directory in (path, *path.parents):
                if directory.exists():
                    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                if directory == root.parent:
                    break

    def capabilities(self) -> UploadCapabilities:
        return UploadCapabilities(
            protocol="single_put",
            parallel_chunks_per_object=1,
            can_list_committed_units=False,
            supports_unit_checksum=False,
            supports_final_checksum=False,
            supports_provider_lifecycle=False,
        )

    def begin_upload(self, request: BeginUpload) -> ProviderUpload:
        return ProviderUpload(
            provider_id=uuid.uuid4().hex,
            object_key=self._key(request.object_key),
            protocol="single_put",
            byte_size=request.byte_size,
            expires_at=request.expires_at,
        )

    def create_transfer_instruction(
        self, upload: ProviderUpload, cursor: TransferCursor
    ) -> UploadInstruction:
        raise UploadCapabilityUnavailable(
            "Embedded storage cannot issue a browser-transfer credential."
        )

    def query_progress(self, upload: ProviderUpload) -> UploadProgress:
        uri = self._uri(upload.object_key)
        confirmed = self.size(uri) if self.exists(uri) else 0
        return UploadProgress(
            confirmed_bytes=confirmed,
            total_bytes=upload.byte_size,
            receipts=(),
            complete=confirmed == upload.byte_size,
        )

    def complete_upload(
        self, upload: ProviderUpload, receipts: list[TransferReceipt]
    ) -> CompletedObject:
        if receipts:
            raise ValueError("Embedded single-put completion does not accept unit receipts.")
        metadata = self.stat(self._uri(upload.object_key))
        if metadata.byte_size != upload.byte_size:
            raise ValueError("Embedded object size does not match the upload manifest.")
        return CompletedObject(metadata.uri, metadata.byte_size, metadata.checksum)

    def abort_upload(self, upload: ProviderUpload) -> None:
        self.delete(self._uri(upload.object_key))

    def uri_for_key(self, object_key: str) -> str:
        return self._uri(object_key)

    def stat(self, uri: str, *, version: str | None = None) -> ObjectMetadata:
        path = self._path_from_uri(uri)
        with self._original(uri, version) as source:
            return ObjectMetadata(
                uri=self._canonical_uri(path),
                byte_size=os.fstat(source.fileno()).st_size,
                storage_path=path,
                provider_headers={"version_id": version} if version else {},
            )

    def open_stream(
        self, uri: str, byte_range: ByteRange | None = None, *, version: str | None = None
    ):
        source = self._original(uri, version)
        try:
            if byte_range is None:
                return source
            source.seek(byte_range.start)
            value = source.read(byte_range.end_inclusive - byte_range.start + 1)
        except BaseException:
            source.close()
            raise
        source.close()
        return io.BytesIO(value)

    def put_stream(self, uri: str, source) -> ObjectMetadata:
        with self._locked():
            path = self._path_from_uri(uri)
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(
                    dir=path.parent, prefix=".upload-", delete=False
                ) as destination:
                    temporary = Path(destination.name)
                    while chunk := source.read(1024 * 1024):
                        destination.write(chunk)
                    destination.flush()
                    self._stamp(destination.fileno(), required=False)
                    os.fsync(destination.fileno())
                os.replace(temporary, path)
                self._sync_directory(path.parent)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            return self.stat(uri)

    def put_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        return self.put_stream(self._uri(key), io.BytesIO(value))

    def create_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        with self._locked():
            path = self._path(self._key(key))
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(
                    dir=path.parent, prefix=".upload-", delete=False
                ) as destination:
                    temporary = Path(destination.name)
                    destination.write(value)
                    destination.flush()
                    version = self._stamp(destination.fileno(), required=True)
                    os.fsync(destination.fileno())
                os.link(temporary, path)
                self._sync_directory(path.parent)
                return ObjectMetadata(
                    uri=self._canonical_uri(path),
                    byte_size=len(value),
                    storage_path=path,
                    provider_headers={"version_id": version},
                )
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)

    def read_bytes(self, uri: str) -> bytes:
        return self._path_from_uri(uri).read_bytes()

    def read_head(self, uri: str, byte_count: int = 4096) -> bytes:
        with self._path_from_uri(uri).open("rb") as source:
            return source.read(byte_count)

    def exists(self, uri: str) -> bool:
        try:
            return self._path_from_uri(uri).is_file()
        except ValueError:
            return False

    def size(self, uri: str) -> int:
        return self._path_from_uri(uri).stat().st_size

    def dataframe_source(self, uri: str) -> RayDataSourceDescriptor:
        return RayDataSourceDescriptor(
            path=str(self._path_from_uri(uri).resolve()),
            filesystem_options={},
            provider=self.driver_name,
        )

    def healthcheck(self) -> HealthcheckResult:
        (self.root / self.bucket).mkdir(parents=True, exist_ok=True)
        return HealthcheckResult(healthy=True, driver=self.driver_name)

    def prefix_absent(self, prefix: str) -> bool:
        path = self._path(self._key(prefix)).resolve()
        root = (self.root / self.bucket).resolve()
        if path == root or not path.is_relative_to(root):
            raise ValueError("Cleanup inspection must stay within a resource prefix.")
        return not path.exists()

    def delete_prefix(self, prefix: str) -> None:
        with self._locked():
            import shutil

            path = self._path(self._key(prefix)).resolve()
            root = (self.root / self.bucket).resolve()
            if path == root or not path.is_relative_to(root):
                raise ValueError("Cleanup must stay within a resource prefix.")
            if path.is_dir():
                shutil.rmtree(path)
                self._sync_directory(path.parent)

    def delete(self, uri: str, *, version: str | None = None) -> None:
        if version is not None:
            embedded_object_version(version)
        with self._locked():
            path = self._path_from_uri(uri)
            if version is not None:
                try:
                    with path.open("rb") as source:
                        if self._incarnation(source.fileno()) != version:
                            self._sync_directory(path.parent)
                            return
                except FileNotFoundError:
                    self._sync_directory(path.parent)
                    return
            path.unlink(missing_ok=True)
            if path.parent.exists():
                self._sync_directory(path.parent)

    def _key(self, value: str) -> str:
        key = value.strip("/")
        if not key or ".." in key.split("/") or "\x00" in key or Path(key).drive:
            raise ValueError("Object keys must remain inside the configured bucket.")
        return key

    def _path(self, key: str) -> Path:
        path = self.root / self.bucket / self._key(key)
        if path.resolve() != self.root.resolve() / self.bucket / self._key(key):
            raise ValueError("Object keys must remain inside the configured bucket.")
        return path

    def _uri(self, key: str) -> str:
        return f"embedded://{self.bucket}/{self._key(key)}"

    def _path_from_uri(self, uri: str) -> Path:
        canonical = f"embedded://{self.bucket}/"
        legacy = f"minio://{self.bucket}/"
        if uri.startswith(canonical):
            key = uri.removeprefix(canonical)
        elif uri.startswith(legacy):
            key = uri.removeprefix(legacy)
        else:
            raise ValueError("Object URI does not belong to the configured embedded store.")
        return self._path(key)

    def _canonical_uri(self, path: Path) -> str:
        key = path.relative_to(self.root / self.bucket).as_posix()
        return self._uri(key)
