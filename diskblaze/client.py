from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import posixpath
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from tenacity import (
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

logger = logging.getLogger("diskblaze")


def _graphql_op_name(query: str) -> str:
    """Best-effort extraction of the operation/mutation name for log lines."""
    for token in query.replace("(", " ").replace("{", " ").split():
        if token in {
            "query",
            "mutation",
            "subscription",
        }:
            continue
        return token
    return "graphql"


DEFAULT_ENDPOINT = "https://diskblaze.com/graphql"
MiB = 1024 * 1024


class DiskBlazeError(RuntimeError):
    pass


_TRANSIENT_ERROR_FRAGMENTS = (
    "500",
    "501",
    "502",
    "503",
    "504",
    "internal server error",
    "bad gateway",
    "service unavailable",
    "gateway timeout",
    "timeout",
    "timed out",
    "rate limit",
    "429",
    "too many requests",
    "connection reset",
    "connection aborted",
)


def _is_retryable(exc: BaseException) -> bool:
    """Retry transient errors: network/transport failures and server-side
    errors that the GraphQL endpoint sometimes wraps in a 200 response with an
    ``errors`` payload (e.g. a 500 returned as a DiskBlazeError). Never retry
    genuine business-logic rejections like "not found" or auth failures.
    """
    if isinstance(exc, requests.RequestException):
        return True
    if isinstance(exc, DiskBlazeError):
        lowered = str(exc).lower()
        return any(fragment in lowered for fragment in _TRANSIENT_ERROR_FRAGMENTS)
    return False


@dataclass(frozen=True)
class FileNode:
    id: str
    name: str
    path: str
    parent_path: str
    is_dir: bool
    size_bytes: int
    size: str
    updated_at: str
    readonly: bool = False
    content_sha256: str | None = None


@dataclass(frozen=True)
class CurrentUser:
    id: str
    username: str
    quota: str
    used: str
    remaining: str
    quota_bytes: int
    used_bytes: int
    remaining_bytes: int
    api_access_enabled: bool
    direct_ul_enabled: bool


@dataclass(frozen=True)
class UploadPart:
    number: int
    start: int
    end: int
    url: str


@dataclass(frozen=True)
class UploadPlan:
    token: str
    path: str
    size_bytes: int
    part_size: int
    upload_id: str | None
    put_url: str | None
    parts: list[UploadPart]


@dataclass(frozen=True)
class TransferProgress:
    path: str
    transferred_bytes: int
    total_bytes: int
    phase: str
    speed_bps: float


ProgressCallback = Callable[[TransferProgress], None]


CREATE_UPLOAD_PLAN = """
mutation CreateUploadPlan($path: String!, $sizeBytes: ID!, $contentSha256: String, $partSize: Int) {
  createUploadPlan(path: $path, sizeBytes: $sizeBytes, contentSha256: $contentSha256, partSize: $partSize) {
    token
    path
    sizeBytes
    partSize
    uploadId
    putUrl
    parts { number start end url }
  }
}
"""

COMPLETE_UPLOAD = """
mutation CompleteUpload($token: String!, $completedParts: [CompletedUploadPartInput!], $contentSha256: String) {
  completeUpload(token: $token, completedParts: $completedParts, contentSha256: $contentSha256) {
    id
    name
    path
    parentPath
    isDir
    sizeBytes
    size
    updatedAt
    readonly
    contentSha256
  }
}
"""

DOWNLOAD_URL = """
query DownloadUrl($path: String!, $expiresSeconds: Int) {
  downloadUrl(path: $path, expiresSeconds: $expiresSeconds) { url expiresSeconds }
}
"""

ZIP_URL = """
query ZipUrl($path: String!, $expiresSeconds: Int) {
  zipUrl(path: $path, expiresSeconds: $expiresSeconds) { url expiresSeconds }
}
"""

CREATE_FOLDER = """
mutation CreateFolder($path: String!) {
  createFolder(path: $path) { id name path parentPath isDir sizeBytes size updatedAt readonly contentSha256 }
}
"""

FILES = """
query Files($path: String!) {
  files(path: $path) {
    path
    parent
    items { id name path parentPath isDir sizeBytes size updatedAt readonly contentSha256 }
  }
}
"""

SEARCH_FILES = """
query SearchFiles(
  $query: String!
  $pathPrefix: String
  $kind: String
  $minSizeBytes: ID
  $maxSizeBytes: ID
  $updatedAfter: String
  $updatedBefore: String
  $limit: Int
  $offset: Int
) {
  searchFiles(
    query: $query
    pathPrefix: $pathPrefix
    kind: $kind
    minSizeBytes: $minSizeBytes
    maxSizeBytes: $maxSizeBytes
    updatedAfter: $updatedAfter
    updatedBefore: $updatedBefore
    limit: $limit
    offset: $offset
  ) {
    query
    pathPrefix
    limit
    offset
    hasMore
    items { id name path parentPath isDir sizeBytes size updatedAt readonly contentSha256 }
  }
}
"""

MOVE_PATH = """
mutation MovePath($src: String!, $dst: String!) {
  movePath(src: $src, dst: $dst) {
    id name path parentPath isDir sizeBytes size updatedAt readonly contentSha256
  }
}
"""

DELETE_PATH = """
mutation DeletePath($path: String!) {
  deletePath(path: $path) { ok message }
}
"""

ME = """
query Me {
  me {
    id
    username
    quota
    used
    remaining
    quotaBytes
    usedBytes
    remainingBytes
    apiAccessEnabled
    directUlEnabled
  }
}
"""


def normalize_remote_path(path: str) -> str:
    value = "/" + str(path or "/").strip().lstrip("/")
    normalized = posixpath.normpath(value)
    # Reject path traversal that escapes the root: a ".." that collapses past
    # the leading "/" leaves a leading ".." in the result.
    if normalized == ".." or normalized.startswith("../"):
        raise DiskBlazeError(f"invalid remote path (escapes root): {path!r}")
    return "/" if normalized == "." else normalized


def join_remote(parent: str, name: str) -> str:
    base = normalize_remote_path(parent)
    clean_name = str(name).replace("\\", "/").strip("/")
    return normalize_remote_path(posixpath.join(base, clean_name))


def preferred_part_size(size: int) -> int | None:
    if size >= 8 * 1024 * MiB:
        return 256 * MiB
    if size >= 1024 * MiB:
        return 128 * MiB
    if size >= 64 * MiB:
        return 64 * MiB
    return None


def _node_from_payload(data: dict) -> FileNode:
    return FileNode(
        id=str(data["id"]),
        name=str(data["name"]),
        path=str(data["path"]),
        parent_path=str(data.get("parentPath") or data.get("parent_path") or ""),
        is_dir=bool(data.get("isDir")),
        size_bytes=int(data.get("sizeBytes") or 0),
        size=str(data.get("size") or ""),
        updated_at=str(data.get("updatedAt") or ""),
        readonly=bool(data.get("readonly")),
        content_sha256=data.get("contentSha256"),
    )


def _user_from_payload(data: dict) -> CurrentUser:
    return CurrentUser(
        id=str(data["id"]),
        username=str(data["username"]),
        quota=str(data.get("quota") or ""),
        used=str(data.get("used") or ""),
        remaining=str(data.get("remaining") or ""),
        quota_bytes=int(data.get("quotaBytes") or 0),
        used_bytes=int(data.get("usedBytes") or 0),
        remaining_bytes=int(data.get("remainingBytes") or 0),
        api_access_enabled=bool(data.get("apiAccessEnabled")),
        direct_ul_enabled=bool(data.get("directUlEnabled")),
    )


class _ProgressReader:
    def __init__(
        self,
        handle,
        *,
        length: int,
        offset: int,
        callback: Callable[[int], None] | None,
        chunk_size: int = 1024 * 1024,
    ):
        self.handle = handle
        self.remaining = int(length)
        self.callback = callback
        self.chunk_size = int(chunk_size)
        self.lock = threading.Lock()
        handle.seek(int(offset))

    def __len__(self) -> int:
        return self.remaining

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        if self.remaining <= 0:
            raise StopIteration
        chunk = self.handle.read(min(self.chunk_size, self.remaining))
        if not chunk:
            raise StopIteration
        self.remaining -= len(chunk)
        if self.callback:
            self.callback(len(chunk))
        return chunk


class DiskBlazeClient:
    """Small high-throughput Python client for DiskBlaze GraphQL + gateway URLs."""

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        token: str | None = None,
        timeout: float = 120.0,
        pool_size: int = 64,
        graphql_concurrency: int = 8,
        transfer_timeout: float = 0.0,
    ):
        self.endpoint = (endpoint or os.environ.get("DISKBLAZE_GQL_URL") or DEFAULT_ENDPOINT).rstrip("/")
        self.token = token or os.environ.get("DISKBLAZE_TOKEN") or os.environ.get("DISKBLAZE_API_KEY")
        if not self.token:
            raise DiskBlazeError("DISKBLAZE_TOKEN or DISKBLAZE_API_KEY is required")
        logger.debug(
            "DiskBlazeClient init endpoint=%s timeout=%.1f pool=%d graphql_concurrency=%d transfer_timeout=%.1f",
            self.endpoint,
            float(timeout),
            int(pool_size),
            max(1, int(graphql_concurrency)),
            float(transfer_timeout),
        )
        self.timeout = float(timeout)
        self.pool_size = int(pool_size)
        # Wall-clock cap on a single file transfer (upload or download). The
        # requests timeout is per-idle-read, so a connection that dribbles a few
        # bytes every minute never trips it and a file can hang "completing"
        # forever on a stalled link. transfer_timeout=0 disables the cap.
        self.transfer_timeout = float(transfer_timeout)
        self._headers = {"Authorization": f"Bearer {self.token}"}
        self.session = self._new_session()
        # The GraphQL endpoint is the bottleneck for control-plane calls
        # (createUploadPlan, createFolder): it is far slower than the S3 data
        # plane and returns 500s when hammered by many concurrent requests.
        # Bound the number of in-flight GraphQL calls so a wide upload tree
        # doesn't overload it. Actual byte transfers are unaffected.
        self.graphql_semaphore = threading.Semaphore(max(1, int(graphql_concurrency)))
        # Cache of folder existence (path -> exists?) so ensure_folder doesn't
        # re-issue a createFolder (or an existence probe) for a path it has
        # already resolved. The GraphQL control plane is the slow bottleneck,
        # so avoiding redundant calls matters for wide trees.
        self._folder_cache: dict[str, bool] = {}
        self._listed_parents: set[str] = set()

    def _data_timeout(self):
        """Timeout tuple for data-plane requests (connect, read).

        When ``transfer_timeout`` is set it is applied as the *read* timeout so a
        single stalled socket read (no bytes for ``transfer_timeout`` seconds)
        trips even mid-chunk -- the per-idle ``self.timeout`` alone would not,
        because a connection that dribbles a few bytes keeps the idle timer
        satisfied. A value of 0 disables the wall-clock cap (falls back to the
        default idle timeout).
        """
        if self.transfer_timeout > 0:
            return (self.timeout, self.transfer_timeout)
        return self.timeout

    def _new_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(self._headers)
        adapter = HTTPAdapter(
            pool_connections=self.pool_size,
            pool_maxsize=self.pool_size,
            max_retries=0,
            pool_block=True,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _session(self) -> requests.Session:
        # A single shared session lets urllib3 reuse pooled, keep-alive
        # connections across all worker threads instead of each thread paying
        # for its own TLS handshake. requests.Session is thread-safe for this.
        return self.session

    def graphql(self, query: str, variables: dict | None = None) -> dict:
        op = _graphql_op_name(query)
        logger.debug("graphql %s (semaphore wait)", op)
        with self.graphql_semaphore:
            return self._graphql(query, variables)

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential_jitter(initial=0.5, max=8),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _graphql(self, query: str, variables: dict | None = None) -> dict:
        op = _graphql_op_name(query)
        logger.debug("graphql %s -> POST %s", op, self.endpoint)
        response = self._session().post(
            self.endpoint,
            json={"query": query, "variables": variables or {}},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            message = (
                payload["errors"][0].get("message") if isinstance(payload["errors"], list) else str(payload["errors"])
            )
            raise DiskBlazeError(message or "GraphQL request failed")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise DiskBlazeError("GraphQL response did not include data")
        return data

    def list_files(self, path: str = "/") -> list[FileNode]:
        logger.debug("list_files %s", normalize_remote_path(path))
        data = self.graphql(FILES, {"path": normalize_remote_path(path)})
        return [_node_from_payload(item) for item in data["files"]["items"]]

    def me(self) -> CurrentUser:
        data = self.graphql(ME)
        return _user_from_payload(data["me"])

    def search_files(
        self,
        query: str,
        *,
        path_prefix: str | None = None,
        kind: str | None = None,
        min_size_bytes: int | None = None,
        max_size_bytes: int | None = None,
        updated_after: str | None = None,
        updated_before: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> tuple[list[FileNode], bool]:
        data = self.graphql(
            SEARCH_FILES,
            {
                "query": str(query),
                "pathPrefix": normalize_remote_path(path_prefix) if path_prefix else None,
                "kind": kind,
                "minSizeBytes": str(int(min_size_bytes)) if min_size_bytes is not None else None,
                "maxSizeBytes": str(int(max_size_bytes)) if max_size_bytes is not None else None,
                "updatedAfter": updated_after,
                "updatedBefore": updated_before,
                "limit": int(limit),
                "offset": int(offset),
            },
        )
        result = data["searchFiles"]
        return [_node_from_payload(item) for item in result["items"]], bool(result.get("hasMore"))

    def create_folder(self, path: str) -> FileNode:
        data = self.graphql(CREATE_FOLDER, {"path": normalize_remote_path(path)})
        return _node_from_payload(data["createFolder"])

    def folder_exists(self, path: str) -> bool:
        """Return True if ``path`` is an existing folder, using a cached probe.

        Probes each parent directory listing at most once and records the
        existence of every folder it contains, so checking many siblings under
        the same parent (the common case for an upload tree) costs a single
        GraphQL call instead of one per file.
        """
        if not hasattr(self, "_folder_cache"):
            self._folder_cache = {}
        if not hasattr(self, "_listed_parents"):
            self._listed_parents = set()
        normalized = normalize_remote_path(path)
        if normalized in {"/", "/private", "/public", "/inbox", "/shared"}:
            return True
        if normalized in self._folder_cache:
            return self._folder_cache[normalized]
        parent = normalize_remote_path(posixpath.dirname(normalized))
        # The first time we look inside a parent, list it once and cache the
        # existence of every directory it holds. Subsequent siblings (and even
        # the same folder re-checked) then hit the cache with no extra call.
        if parent not in self._listed_parents:
            self._listed_parents.add(parent)
            try:
                for node in self.list_files(parent):
                    if node.is_dir:
                        self._folder_cache[normalize_remote_path(node.path)] = True
            except DiskBlazeError:
                # Parent may be inaccessible; treat its children as not-found
                # rather than failing the whole upload. create_folder surfaces
                # a real error if the folder truly cannot be made.
                pass
        return bool(self._folder_cache.get(normalized, False))

    def ensure_folder(self, path: str, *, no_create: bool = False) -> None:
        if not hasattr(self, "_folder_cache"):
            self._folder_cache = {}
        normalized = normalize_remote_path(path)
        if normalized in {"/", "/private", "/public", "/inbox", "/shared"}:
            logger.debug("ensure_folder %s -> root, nothing to create", normalized)
            return
        roots = {"private", "public", "inbox", "shared"}
        current = ""
        for part in normalized.strip("/").split("/"):
            current = f"{current}/{part}"
            # The top-level namespace (e.g. "/public") already exists; don't try
            # to (re)create it, but still create every deeper segment.
            if current.count("/") == 1 and part in roots:
                continue
            # Skip the createFolder round-trip when we already know the folder
            # exists (or when the caller asserts it does via no_create).
            if self._folder_cache.get(current, None):
                continue
            if no_create:
                if not self.folder_exists(current):
                    logger.debug("ensure_folder %s assumed present (no_create), not verified", current)
                continue
            if self.folder_exists(current):
                continue
            try:
                logger.debug("ensure_folder creating %s", current)
                self.create_folder(current)
                self._folder_cache[current] = True
            except DiskBlazeError as exc:
                lowered = str(exc).lower()
                # Only swallow a genuine "already exists" race (e.g. a concurrent
                # upload created it first); a transient error would already have
                # been retried by graphql(), so any other message is real.
                if "already exists" in lowered or "already exist" in lowered or "conflict" in lowered:
                    logger.debug("ensure_folder %s already exists, skipping", current)
                    self._folder_cache[current] = True
                    continue
                raise

    def move(self, src: str, dst: str) -> FileNode:
        data = self.graphql(
            MOVE_PATH,
            {"src": normalize_remote_path(src), "dst": normalize_remote_path(dst)},
        )
        return _node_from_payload(data["movePath"])

    def delete(self, path: str) -> str:
        data = self.graphql(DELETE_PATH, {"path": normalize_remote_path(path)})
        payload = data["deletePath"]
        if not payload.get("ok"):
            raise DiskBlazeError(str(payload.get("message") or "delete failed"))
        return str(payload.get("message") or "deleted")

    def create_upload_plan(
        self,
        path: str,
        *,
        size_bytes: int,
        content_sha256: str | None = None,
        part_size: int | None = None,
    ) -> UploadPlan:
        logger.debug("create_upload_plan %s (%d bytes)", normalize_remote_path(path), int(size_bytes))
        data = self.graphql(
            CREATE_UPLOAD_PLAN,
            {
                "path": normalize_remote_path(path),
                "sizeBytes": str(int(size_bytes)),
                "contentSha256": content_sha256,
                "partSize": part_size or preferred_part_size(int(size_bytes)),
            },
        )
        raw = data["createUploadPlan"]
        return UploadPlan(
            token=str(raw["token"]),
            path=str(raw["path"]),
            size_bytes=int(raw["sizeBytes"]),
            part_size=int(raw["partSize"] or 0),
            upload_id=raw.get("uploadId"),
            put_url=raw.get("putUrl"),
            parts=[
                UploadPart(
                    number=int(part["number"]),
                    start=int(part["start"]),
                    end=int(part["end"]),
                    url=str(part["url"]),
                )
                for part in (raw.get("parts") or [])
            ],
        )

    def complete_upload(
        self,
        token: str,
        *,
        completed_parts: list[dict] | None = None,
        content_sha256: str | None = None,
    ) -> FileNode:
        variables = {
            "token": token,
            "completedParts": completed_parts,
            "contentSha256": content_sha256,
        }
        data = self.graphql(COMPLETE_UPLOAD, variables)
        return _node_from_payload(data["completeUpload"])

    def upload_file(
        self,
        local_path: str | Path,
        remote_path: str,
        *,
        workers: int = 8,
        part_size: int | None = None,
        checksum: bool = False,
        ensure_parent: bool = True,
        no_create_folders: bool = False,
        executor: ThreadPoolExecutor | None = None,
        progress: ProgressCallback | None = None,
    ) -> FileNode:
        path = Path(local_path)
        size = path.stat().st_size
        remote = normalize_remote_path(remote_path)
        logger.debug("upload_file %s -> %s (%d bytes, workers=%d, checksum=%s)", path, remote, size, workers, checksum)
        parent = posixpath.dirname(remote)
        if ensure_parent and parent and parent != "/":
            self.ensure_folder(parent, no_create=no_create_folders)
        # Compute the checksum only after the parent folder exists, so we don't
        # waste a full-file read on an auth/quota failure.
        sha256 = self.sha256(path, progress_path=remote, total=size, progress=progress) if checksum else None
        plan = self.create_upload_plan(remote, size_bytes=size, content_sha256=sha256, part_size=part_size)
        if plan.part_size <= 0 and plan.parts:
            raise DiskBlazeError("upload plan returned invalid part size")
        started = time.monotonic()
        transferred = 0
        lock = threading.Lock()

        def report_absolute(absolute: int, phase: str = "uploading") -> None:
            nonlocal transferred
            if not progress:
                return
            with lock:
                transferred = max(0, min(int(absolute), size))
                elapsed = max(time.monotonic() - started, 0.001)
                progress(TransferProgress(remote, transferred, size, phase, transferred / elapsed))

        def bump(delta: int, phase: str = "uploading") -> None:
            report_absolute(transferred + int(delta), phase)

        if plan.put_url:
            report_absolute(0)
            try:
                with path.open("rb") as handle:
                    reader = _ProgressReader(handle, length=size, offset=0, callback=lambda n: bump(n))
                    etag = self._put_stream(plan.put_url, reader, length=size)
            except Exception as exc:
                raise DiskBlazeError(f"upload failed for {remote}: {exc}") from exc
            if etag:
                self._verify_etag(etag, path, size, sha256)
            progress and progress(TransferProgress(remote, size, size, "completing", 0))
            return self.complete_upload(plan.token, content_sha256=sha256 or None)

        if not plan.parts:
            raise DiskBlazeError("upload plan did not include a PUT URL or multipart parts")
        completed: list[dict] = []
        part_progress: dict[int, int] = {}

        def bump_part(part_number: int, loaded: int) -> None:
            if not progress:
                return
            with lock:
                part_progress[int(part_number)] = max(0, int(loaded))
                total = min(size, sum(part_progress.values()))
                elapsed = max(time.monotonic() - started, 0.001)
                progress(TransferProgress(remote, total, size, "uploading", total / elapsed))

        # Cap part-level concurrency. Each in-flight part is a separate gateway
        # PUT; beyond ~8 the marginal bandwidth gain is nil (the link is the
        # bottleneck) yet it raises the chance the server aborts the multipart
        # upload under heavy concurrency.
        max_workers = max(1, min(int(workers), len(plan.parts), 8))
        own_executor = executor is None
        pool = executor or ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="diskblaze-upload")

        def run_parts() -> list[dict]:
            results: list[dict] = []
            futures = {pool.submit(self._upload_part, path, part, bump_part): part for part in plan.parts}
            for future in as_completed(futures):
                part = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    raise DiskBlazeError(f"part {part.number} failed: {exc}") from exc
            return results

        try:
            completed = run_parts()
            completed.sort(key=lambda item: int(item["number"]))
            sent = sum(int(part.end) - int(part.start) for part in plan.parts)
            if sent != size:
                raise DiskBlazeError(f"uploaded {sent} bytes but file is {size} bytes; aborting incomplete upload")
            progress and progress(TransferProgress(remote, size, size, "completing", 0))
            # The gateway occasionally aborts a multipart upload when many overlap
            # (HTTP 404 NoSuchUpload). Re-upload the parts and retry completion once
            # before giving up, since the local bytes are still valid.
            #
            # NOTE: there is no server-side abortUpload mutation, so an orphaned
            # multipart upload cannot be explicitly cancelled from the client. The
            # only recovery is to re-upload the parts and complete; the gateway
            # garbage-collects abandoned multipart uploads server-side. We therefore
            # never call an abort and instead rely on the completion retry below.
            attempts = 2
            while attempts:
                attempts -= 1
                try:
                    return self.complete_upload(plan.token, completed_parts=completed, content_sha256=sha256 or None)
                except DiskBlazeError as exc:
                    if attempts == 0 or "nosuchupload" not in str(exc).lower():
                        raise
                    completed = run_parts()
                    completed.sort(key=lambda item: int(item["number"]))
            raise DiskBlazeError("multipart upload completion failed unexpectedly")
        finally:
            if own_executor:
                pool.shutdown(wait=True)

    def _verify_etag(self, etag: str, path: Path, size: int, sha256: str | None) -> None:
        """Best-effort integrity check on a single-PUT upload.

        S3 returns a quoted ETag. For non-multipart uploads it is the MD5 of the
        object body; if it parses as 32 hex chars we compare against the local
        file's MD5 so a truncated/corrupted transfer is caught. The content SHA-256
        is the authoritative server-side check performed by complete_upload.
        """
        candidate = etag.strip().strip('"')
        if len(candidate) != 32:
            return
        try:
            import binascii

            binascii.unhexlify(candidate)
        except (ValueError, binascii.Error):
            return
        local_md5 = hashlib.md5()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8 * MiB), b""):
                local_md5.update(chunk)
        if local_md5.hexdigest() != candidate:
            raise DiskBlazeError("ETag mismatch after upload: data integrity check failed")

    def upload_tree(
        self,
        local_path: str | Path,
        remote_dir: str,
        *,
        workers: int = 8,
        file_workers: int = 2,
        checksum: bool = False,
        skip_existing: bool = False,
        no_create_folders: bool = False,
        progress: ProgressCallback | None = None,
    ) -> list[FileNode]:
        root = Path(local_path)
        if root.is_file():
            return [
                self.upload_file(
                    root,
                    join_remote(remote_dir, root.name),
                    workers=workers,
                    checksum=checksum,
                    no_create_folders=no_create_folders,
                    progress=progress,
                )
            ]
        # Enumerate with followlinks=False so a symlink into a large tree is
        # not duplicated and a symlink loop cannot recurse forever. Symlink
        # files are still uploaded (they resolve to real bytes); symlink dirs
        # are treated as files of their own and skipped as directories.
        files = [
            Path(dirpath) / name
            for dirpath, _dirnames, filenames in os.walk(root, followlinks=False)
            for name in filenames
        ]
        if not files:
            return []

        # Optional resume: skip files whose remote parent already lists a file
        # with the same name and byte size. Build one cached listing per unique
        # parent remote dir (a handful of GraphQL calls, far cheaper than
        # re-uploading gigabytes on an interrupted run).
        remote_index: dict[str, dict[str, int]] = {}

        def _remote_sizes(parent_remote: str) -> dict[str, int]:
            if parent_remote not in remote_index:
                sizes: dict[str, int] = {}
                try:
                    for node in self.list_files(parent_remote):
                        if not node.is_dir:
                            sizes[node.name] = node.size_bytes
                except DiskBlazeError:
                    # Parent may not exist yet; treat as "nothing to skip".
                    pass
                remote_index[parent_remote] = sizes
            return remote_index[parent_remote]

        results: list[FileNode] = []
        # A single shared executor bounds total concurrency to file_workers * workers
        # instead of file_workers threads each spinning up their own 'workers' pool
        # (which would multiply to file_workers * workers part threads). Per-file
        # part uploads draw from this same pool via the 'executor' argument.
        #
        # Folders are NOT pre-created up front: the GraphQL createFolder mutation is
        # slow (~seconds per call, server-serialized), so a tree of hundreds of
        # folders would stall for minutes before the first byte moved. Instead each
        # upload_file ensures its own parent on the worker thread, so folder
        # creation overlaps with the uploads and the first transfer starts immediately.
        total_workers = max(1, int(file_workers)) * max(1, int(workers))
        with ThreadPoolExecutor(max_workers=total_workers, thread_name_prefix="diskblaze-upload") as executor:
            futures = {}
            for file_path in files:
                rel = file_path.relative_to(root).as_posix()
                remote_path = join_remote(remote_dir, rel)
                if skip_existing and not no_create_folders:
                    # When --no-create-folders is set we assume the target tree
                    # already exists and skip the size probe entirely (no
                    # list_files) for maximum speed; files are just uploaded.
                    parent_remote = posixpath.dirname(remote_path)
                    remote_sizes = _remote_sizes(parent_remote)
                    try:
                        local_size = file_path.stat().st_size
                    except OSError:
                        local_size = -1
                    if remote_sizes.get(file_path.name) == local_size:
                        if progress:
                            progress(TransferProgress(remote_path, local_size, local_size, "skipped", 0))
                        continue
                futures[
                    executor.submit(
                        self.upload_file,
                        file_path,
                        remote_path,
                        workers=workers,
                        checksum=checksum,
                        ensure_parent=not no_create_folders,
                        no_create_folders=no_create_folders,
                        executor=executor,
                        progress=progress,
                    )
                ] = file_path
            failures: list[tuple[str, Exception]] = []
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    # A single file's failure (e.g. a transient 500 on
                    # completeUpload) must not abort the rest of the batch.
                    failures.append((str(file_path), exc))
            if failures:
                summary = "; ".join(f"{path}: {exc}" for path, exc in failures[:5])
                extra = f" (+{len(failures) - 5} more)" if len(failures) > 5 else ""
                raise DiskBlazeError(f"{len(failures)} of {len(futures)} files failed: {summary}{extra}")
        return results

    def download_url(self, path: str, *, expires_seconds: int = 3600) -> str:
        logger.debug("download_url %s (ttl=%ds)", normalize_remote_path(path), expires_seconds)
        data = self.graphql(DOWNLOAD_URL, {"path": normalize_remote_path(path), "expiresSeconds": int(expires_seconds)})
        return str(data["downloadUrl"]["url"])

    def zip_url(self, path: str, *, expires_seconds: int = 3600) -> str:
        data = self.graphql(ZIP_URL, {"path": normalize_remote_path(path), "expiresSeconds": int(expires_seconds)})
        return str(data["zipUrl"]["url"])

    def download(
        self,
        remote_path: str,
        local_path: str | Path,
        *,
        workers: int = 8,
        expires_seconds: int = 3600,
        as_zip: bool | None = None,
        skip_existing: bool = False,
        expected_size: int = 0,
        progress: ProgressCallback | None = None,
    ) -> Path:
        remote = normalize_remote_path(remote_path)
        output = Path(local_path)
        if as_zip is None:
            as_zip = output.suffix.lower() == ".zip"
        url = (
            self.zip_url(remote, expires_seconds=expires_seconds)
            if as_zip
            else self.download_url(remote, expires_seconds=expires_seconds)
        )
        # Treat the destination as a directory when it ends with a separator, is
        # an existing directory, or is a not-yet-created path without an
        # extension (the caller almost certainly meant a folder to drop the file
        # into) -- otherwise write to the exact path given.
        is_dir_target = (
            str(local_path).endswith(os.sep) or output.is_dir() or (not output.exists() and output.suffix == "")
        )
        if is_dir_target:
            name = posixpath.basename(remote.rstrip("/")) or "download"
            if as_zip and not name.endswith(".zip"):
                name += ".zip"
            output = output / name
        output.parent.mkdir(parents=True, exist_ok=True)
        return self._download_url(
            url,
            output,
            display_path=remote,
            workers=workers,
            skip_existing=skip_existing,
            expected_size=expected_size,
            progress=progress,
        )

    def download_tree(
        self,
        remote_dir: str,
        local_dir: str | Path,
        *,
        workers: int = 16,
        file_workers: int = 8,
        expires_seconds: int = 3600,
        skip_existing: bool = False,
        progress: ProgressCallback | None = None,
    ) -> list[Path]:
        """Recursively download a remote folder as normal local files.

        ZIP downloads are simpler, but this path lets fast clients saturate a
        link with multiple files and ranged downloads while preserving the tree.
        """
        root_remote = normalize_remote_path(remote_dir)
        root_local = Path(local_dir)
        root_local.mkdir(parents=True, exist_ok=True)
        files: list[FileNode] = []
        stack = [root_remote]
        while stack:
            folder = stack.pop()
            for node in self.list_files(folder):
                if node.is_dir:
                    stack.append(node.path)
                else:
                    files.append(node)

        results: list[Path] = []
        with ThreadPoolExecutor(
            max_workers=max(1, int(file_workers)), thread_name_prefix="diskblaze-dl-file"
        ) as executor:
            futures = {}
            prefix = root_remote.rstrip("/") + "/"
            for node in files:
                rel = node.path[len(prefix) :] if node.path.startswith(prefix) else node.name
                output = root_local / rel
                futures[
                    executor.submit(
                        lambda p=node.path, o=output, s=node.size_bytes: self.download(
                            p,
                            o,
                            workers=workers,
                            expires_seconds=expires_seconds,
                            as_zip=False,
                            skip_existing=skip_existing,
                            progress=progress,
                            expected_size=s,
                        )
                    )
                ] = node
            failures: list[tuple[str, Exception]] = []
            for future in as_completed(futures):
                node = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    # A single file's failure must not abort the rest of the
                    # batch, mirroring upload_tree's behavior.
                    failures.append((node.path, exc))
            if failures:
                summary = "; ".join(f"{path}: {exc}" for path, exc in failures[:5])
                extra = f" (+{len(failures) - 5} more)" if len(failures) > 5 else ""
                raise DiskBlazeError(f"{len(failures)} of {len(futures)} files failed: {summary}{extra}")
        return results

    def _download_url(
        self,
        url: str,
        output: Path,
        *,
        display_path: str,
        workers: int,
        skip_existing: bool = False,
        expected_size: int = 0,
        progress: ProgressCallback | None,
    ) -> Path:
        logger.debug("download %s -> %s (workers=%d, skip_existing=%s)", display_path, output, workers, skip_existing)
        size = 0
        accepts_ranges = False
        try:
            probe = self._session().head(url, allow_redirects=True, timeout=self.timeout)
            if probe.status_code < 400:
                size = int(probe.headers.get("Content-Length") or 0)
                accepts_ranges = probe.headers.get("Accept-Ranges", "").lower() == "bytes"
            elif probe.status_code in {401, 403}:
                raise DiskBlazeError(f"download not authorized (HTTP {probe.status_code}) for {display_path}")
            elif probe.status_code not in {405, 501}:
                probe.raise_for_status()
        except requests.RequestException:
            size = 0
            accepts_ranges = False
        if not size:
            range_probe = None
            try:
                range_probe = self._session().get(
                    url,
                    headers={"Range": "bytes=0-0"},
                    stream=True,
                    allow_redirects=True,
                    timeout=self.timeout,
                )
                if range_probe.status_code == 206:
                    content_range = range_probe.headers.get("Content-Range", "")
                    if "/" in content_range:
                        size = int(content_range.rsplit("/", 1)[1])
                    accepts_ranges = True
                elif range_probe.status_code in {401, 403}:
                    raise DiskBlazeError(f"download not authorized (HTTP {range_probe.status_code}) for {display_path}")
                elif range_probe.status_code < 400:
                    size = int(range_probe.headers.get("Content-Length") or 0)
                else:
                    range_probe.raise_for_status()
            finally:
                if range_probe is not None:
                    with contextlib.suppress(Exception):
                        range_probe.close()
        if not size:
            size = int(expected_size or 0)
        ranges = accepts_ranges and size > 8 * MiB
        # Resume/short-circuit: a complete local copy (same byte size) needs no
        # transfer when skip_existing is set. We still download to a .part when
        # the local file is partial, so an interrupted run picks up where it
        # left off rather than re-fetching everything.
        if skip_existing and output.exists() and size and output.stat().st_size == size:
            logger.debug("download %s skipped, local copy already complete (%d bytes)", display_path, size)
            progress and progress(TransferProgress(display_path, size, size, "skipped", 0))
            return output
        # Download to a sidecar .part file, then rename into place only on
        # success. A failed or interrupted transfer is left as a .part instead
        # of a silently-corrupt file at the final path.
        part_path = output.with_name(output.name + ".part")
        started = time.monotonic()
        transferred = 0
        lock = threading.Lock()

        def bump(delta: int) -> None:
            nonlocal transferred
            if not progress:
                return
            with lock:
                transferred += delta
                elapsed = max(time.monotonic() - started, 0.001)
                progress(TransferProgress(display_path, transferred, size, "downloading", transferred / elapsed))

        try:
            if ranges and workers > 1:
                logger.debug("download %s using %d parallel ranges", display_path, max(1, int(workers)))
                part_path.write_bytes(b"")
                with part_path.open("r+b") as handle:
                    handle.truncate(size)
                part_size = max(16 * MiB, min(128 * MiB, size // max(1, int(workers))))
                ranges_to_get = [(start, min(size, start + part_size)) for start in range(0, size, part_size)]
                range_progress: dict[int, int] = {}

                def bump_range(start: int, loaded: int) -> None:
                    nonlocal transferred
                    if not progress:
                        return
                    with lock:
                        range_progress[int(start)] = max(0, int(loaded))
                        transferred = min(size, sum(range_progress.values()))
                        elapsed = max(time.monotonic() - started, 0.001)
                        progress(
                            TransferProgress(display_path, transferred, size, "downloading", transferred / elapsed)
                        )

                with ThreadPoolExecutor(
                    max_workers=max(1, int(workers)), thread_name_prefix="diskblaze-download"
                ) as executor:
                    futures = [
                        executor.submit(self._download_range, url, part_path, start, end, bump_range)
                        for start, end in ranges_to_get
                    ]
                    for future in as_completed(futures):
                        future.result()
            else:
                # Resume a previously-interrupted single-stream download when the
                # server honors range requests: request only the remaining bytes
                # and append to the existing .part instead of starting over.
                existing = part_path.stat().st_size if part_path.exists() else 0
                resume = bool(existing) and bool(size) and existing < size and accepts_ranges
                if resume:
                    transferred = existing
                    logger.debug("download %s resuming from %d/%d bytes", display_path, existing, size)
                get_headers: dict[str, str] = {}
                if resume:
                    get_headers["Range"] = f"bytes={existing}-"
                with self._session().get(
                    url, headers=get_headers, stream=True, timeout=self._data_timeout()
                ) as response:
                    response.raise_for_status()
                    # A 206 means we're appending to a partial file; a 200 (or a
                    # server that ignored the Range header) means start fresh.
                    if response.status_code == 206:
                        handle_mode = "ab"
                    else:
                        handle_mode = "wb"
                        existing = 0
                    deadline = 0.0
                    if self.transfer_timeout > 0:
                        deadline = time.monotonic() + self.transfer_timeout
                    with part_path.open(handle_mode) as handle:
                        for chunk in response.iter_content(chunk_size=4 * MiB):
                            if not chunk:
                                continue
                            if deadline and time.monotonic() > deadline:
                                raise requests.Timeout("download stalled: exceeded transfer_timeout")
                            handle.write(chunk)
                            bump(len(chunk))
            os.replace(part_path, output)
        except BaseException:
            # Leave the .part file for inspection/resume; don't clobber a
            # previously-good copy at the destination.
            raise
        progress and progress(TransferProgress(display_path, size or transferred, size or transferred, "done", 0))
        return output

    def _put_stream(self, url: str, body: Iterable[bytes], *, length: int) -> str:
        deadline = 0.0
        if self.transfer_timeout > 0:
            deadline = time.monotonic() + self.transfer_timeout

        def _check_stall() -> None:
            if deadline and time.monotonic() > deadline:
                raise requests.Timeout("upload stalled: exceeded transfer_timeout")

        if length == 0:
            _check_stall()
            response = self._session().put(url, data=b"", headers={"Content-Length": "0"}, timeout=self._data_timeout())
            response.raise_for_status()
            return response.headers.get("ETag", "").replace('"', "")
        # The upload body is an iterator; wrap it so every chunk checks the
        # wall-clock deadline and aborts a stalled connection.
        body_iter = body

        def guarded(body: Iterable[bytes]) -> Iterable[bytes]:
            for chunk in body:
                _check_stall()
                yield chunk

        response = self._session().put(
            url,
            data=guarded(body_iter),
            headers={"Content-Length": str(int(length))},
            timeout=self._data_timeout(),
        )
        response.raise_for_status()
        return response.headers.get("ETag", "").replace('"', "")

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential_jitter(initial=0.5, max=8),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _upload_part(self, path: Path, part: UploadPart, progress: Callable[[int, int], None] | None) -> dict:
        length = part.end - part.start
        if progress:
            progress(part.number, 0)
        with path.open("rb") as handle:
            loaded = 0

            def bump(delta: int) -> None:
                nonlocal loaded
                loaded += int(delta)
                if progress:
                    progress(part.number, loaded)

            reader = _ProgressReader(handle, length=length, offset=part.start, callback=bump)
            etag = self._put_stream(part.url, reader, length=length)
        return {"number": part.number, "etag": etag}

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        wait=wait_exponential_jitter(initial=0.5, max=8),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _download_range(
        self, url: str, output: Path, start: int, end: int, progress: Callable[[int, int], None]
    ) -> None:
        headers = {"Range": f"bytes={start}-{end - 1}"}
        loaded = 0
        progress(start, 0)
        deadline = 0.0
        if self.transfer_timeout > 0:
            deadline = time.monotonic() + self.transfer_timeout
        with self._session().get(url, headers=headers, stream=True, timeout=self._data_timeout()) as response:
            response.raise_for_status()
            if response.status_code != 206:
                raise DiskBlazeError("server did not honor range request")
            with output.open("r+b") as handle:
                handle.seek(start)
                for chunk in response.iter_content(chunk_size=4 * MiB):
                    if not chunk:
                        continue
                    if deadline and time.monotonic() > deadline:
                        raise requests.Timeout("download stalled: exceeded transfer_timeout")
                    handle.write(chunk)
                    loaded += len(chunk)
                    progress(start, loaded)

    @staticmethod
    def sha256(path: Path, *, progress_path: str, total: int, progress: ProgressCallback | None) -> str:
        digest = hashlib.sha256()
        read = 0
        started = time.monotonic()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(8 * MiB)
                if not chunk:
                    break
                digest.update(chunk)
                read += len(chunk)
                if progress:
                    elapsed = max(time.monotonic() - started, 0.001)
                    progress(TransferProgress(progress_path, read, total, "hashing", read / elapsed))
        return digest.hexdigest()


def endpoint_from_base(value: str) -> str:
    raw = value.strip()
    if raw.endswith("/graphql"):
        return raw
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        return raw.rstrip("/") + "/graphql"
    if raw:
        raise DiskBlazeError(f"invalid endpoint (missing scheme/host): {raw!r}")
    return raw
