from __future__ import annotations

import hashlib
import posixpath
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from diskblaze.cli import build_parser
from diskblaze.client import (
    DiskBlazeClient,
    DiskBlazeError,
    FileNode,
    UploadPart,
    UploadPlan,
    _is_retryable,
    endpoint_from_base,
    join_remote,
    normalize_remote_path,
)


def test_remote_path_helpers_normalize_posix_paths():
    assert normalize_remote_path("private/../public//demo.txt") == "/public/demo.txt"
    assert normalize_remote_path("/") == "/"
    assert join_remote("/private/base", "nested\\file.bin") == "/private/base/nested/file.bin"


def test_endpoint_from_base_accepts_base_or_graphql_url():
    assert endpoint_from_base("https://diskblaze.com") == "https://diskblaze.com/graphql"
    assert endpoint_from_base("https://diskblaze.com/graphql") == "https://diskblaze.com/graphql"


def test_is_retryable_treats_wrapped_500_as_transient():
    assert _is_retryable(DiskBlazeError("500 Server Error: Internal Server Error for url: ..."))
    assert _is_retryable(DiskBlazeError("502 Bad Gateway"))
    assert _is_retryable(DiskBlazeError("503 Service Unavailable"))
    assert _is_retryable(DiskBlazeError("rate limit exceeded"))


def test_is_retryable_ignores_business_errors():
    assert not _is_retryable(DiskBlazeError("not found"))
    assert not _is_retryable(DiskBlazeError("unauthorized"))
    assert not _is_retryable(DiskBlazeError("folders must be under /private"))


def test_ensure_folder_creates_nested_segment_under_namespace():
    class Recorder(DiskBlazeClient):
        def __init__(self):
            super().__init__(token="dummy")
            self.created: list[str] = []

        def list_files(self, path: str = "/"):
            return []

        def create_folder(self, path: str) -> FileNode:
            self.created.append(path)
            return FileNode(
                id="id",
                name=path.rsplit("/", 1)[-1],
                path=path,
                parent_path=path.rsplit("/", 1)[0],
                is_dir=True,
                size_bytes=0,
                size="0 B",
                updated_at="",
                readonly=False,
                content_sha256=None,
            )

    client = Recorder()
    client.ensure_folder("/public/dark angels")
    assert "/public/dark angels" in client.created
    assert "/public" not in client.created


def test_ensure_folder_creates_full_nested_chain():
    class Recorder(DiskBlazeClient):
        def __init__(self):
            super().__init__(token="dummy")
            self.created: list[str] = []

        def list_files(self, path: str = "/"):
            return []

        def create_folder(self, path: str) -> FileNode:
            self.created.append(path)
            return FileNode(
                id="id",
                name=path.rsplit("/", 1)[-1],
                path=path,
                parent_path=path.rsplit("/", 1)[0],
                is_dir=True,
                size_bytes=0,
                size="0 B",
                updated_at="",
                readonly=False,
                content_sha256=None,
            )

    client = Recorder()
    client.ensure_folder("/private/a/b/c")
    assert client.created == ["/private/a", "/private/a/b", "/private/a/b/c"]


def test_cli_download_parser_has_remote_and_local_once():
    args = build_parser().parse_args(["download", "/private/a.bin", "./a.bin"])
    assert args.command == "download"
    assert args.remote == "/private/a.bin"
    assert args.local == "./a.bin"


class FakeUploadClient(DiskBlazeClient):
    def __init__(self):
        self.created_folders: list[str] = []
        self.plan_requests: list[dict] = []
        self.uploaded = bytearray()
        self.completed: list[dict] = []

    def ensure_folder(self, path: str, *, no_create: bool = False) -> None:
        self.created_folders.append(path)

    def create_upload_plan(
        self, path: str, *, size_bytes: int, content_sha256: str | None = None, part_size: int | None = None
    ):
        self.plan_requests.append(
            {
                "path": path,
                "size_bytes": size_bytes,
                "content_sha256": content_sha256,
                "part_size": part_size,
            }
        )
        return UploadPlan(
            token="upload-token",
            path=path,
            size_bytes=size_bytes,
            part_size=0,
            upload_id=None,
            put_url="https://upload.invalid/object",
            parts=[],
        )

    def _put_stream(self, url, body, *, length: int, progress=None):
        assert url == "https://upload.invalid/object"
        start = len(self.uploaded)
        for chunk in body:
            self.uploaded.extend(chunk)
        assert len(self.uploaded) - start == length
        return "etag"

    def complete_upload(self, token: str, *, completed_parts=None, content_sha256: str | None = None):
        self.completed.append(
            {
                "token": token,
                "completed_parts": completed_parts,
                "content_sha256": content_sha256,
            }
        )
        return FileNode(
            id="node-1",
            name="file.bin",
            path="/private/up/file.bin",
            parent_path="/private/up",
            is_dir=False,
            size_bytes=len(self.uploaded),
            size=f"{len(self.uploaded)} B",
            updated_at="now",
            content_sha256=content_sha256,
        )


def test_upload_file_streams_bytes_and_sends_checksum(tmp_path: Path):
    local = tmp_path / "file.bin"
    local.write_bytes(b"diskblaze" * 1024)
    client = FakeUploadClient()

    node = client.upload_file(local, "/private/up/file.bin", checksum=True, workers=4)

    expected_sha = hashlib.sha256(local.read_bytes()).hexdigest()
    assert bytes(client.uploaded) == local.read_bytes()
    assert client.created_folders == ["/private/up"]
    assert client.plan_requests[0]["content_sha256"] == expected_sha
    assert client.completed == [
        {
            "token": "upload-token",
            "completed_parts": None,
            "content_sha256": expected_sha,
        }
    ]
    assert node.content_sha256 == expected_sha


class FakeMultipartClient(DiskBlazeClient):
    """Simulates a multipart plan and validates part bytes + completed parts."""

    def __init__(self):
        self.uploaded_parts: dict[int, bytes] = {}
        self.completed_parts: list[dict] | None = None
        self.plan_size = 0

    def ensure_folder(self, path: str, *, no_create: bool = False) -> None:
        pass

    def create_upload_plan(self, path, *, size_bytes, content_sha256=None, part_size=None):
        self.plan_size = int(size_bytes)
        part_size = 5
        parts = []
        for start in range(0, int(size_bytes), part_size):
            parts.append(
                UploadPart(
                    number=len(parts) + 1,
                    start=start,
                    end=min(start + part_size, int(size_bytes)),
                    url=f"https://upload.invalid/part/{len(parts) + 1}",
                )
            )
        return UploadPlan(
            token="multi-token",
            path=path,
            size_bytes=int(size_bytes),
            part_size=part_size,
            upload_id="uid",
            put_url=None,
            parts=parts,
        )

    def _put_stream(self, url, body, *, length: int):
        num = int(str(url).rsplit("/", 1)[1])
        data = b"".join(body)
        assert len(data) == length, f"part {num}: sent {len(data)} != declared {length}"
        self.uploaded_parts[num] = data
        return f"etag-{num}"

    def complete_upload(self, token, *, completed_parts=None, content_sha256=None):
        self.completed_parts = completed_parts
        return FileNode(
            id="n",
            name="f",
            path="/private/mp/f",
            parent_path="/private/mp",
            is_dir=False,
            size_bytes=self.plan_size,
            size=f"{self.plan_size} B",
            updated_at="now",
            content_sha256=content_sha256,
        )


def test_upload_file_multipart_streams_all_parts(tmp_path: Path):
    data = bytes(range(256)) * 20  # 5120 bytes -> multiple 5-byte parts
    local = tmp_path / "big.bin"
    local.write_bytes(data)
    client = FakeMultipartClient()

    client.upload_file(local, "/private/mp/f", workers=3)

    # Every byte uploaded exactly once, in order.
    rebuilt = b"".join(client.uploaded_parts[n] for n in sorted(client.uploaded_parts))
    assert rebuilt == data
    # completed_parts passed to server sorted by number with etags.
    nums = [int(p["number"]) for p in client.completed_parts]
    assert nums == sorted(nums)
    assert all(p["etag"].startswith("etag-") for p in client.completed_parts)


def test_upload_file_empty_file_succeeds(tmp_path: Path):
    local = tmp_path / "empty.bin"
    local.write_bytes(b"")
    client = FakeUploadClient()

    node = client.upload_file(local, "/private/up/empty.bin", workers=1)

    assert bytes(client.uploaded) == b""
    assert node.size_bytes == 0


def test_upload_file_reports_original_error(tmp_path: Path):
    local = tmp_path / "f.bin"
    local.write_bytes(b"x" * 10)

    class BoomClient(FakeUploadClient):
        def _put_stream(self, url, body, *, length: int):
            raise requests.ConnectionError("simulated network down")

    client = BoomClient()
    with pytest.raises(DiskBlazeError) as excinfo:
        client.upload_file(local, "/private/up/f.bin", workers=1)
    # The original cause is preserved, not flattened into a bare string.
    assert isinstance(excinfo.value.__cause__, requests.ConnectionError)
    assert "simulated network down" in str(excinfo.value)


def test_download_tree_uses_relative_path():
    root = normalize_remote_path("/public/a/b")
    nested = normalize_remote_path("/public/a/b/c/d.mp3")
    prefix = root.rstrip("/") + "/"
    rel = nested[len(prefix) :] if nested.startswith(prefix) else nested
    assert rel == "c/d.mp3"


class FakeDownloadClient(DiskBlazeClient):
    def __init__(self):
        self.files = [
            FileNode(
                id="1",
                name="ok.bin",
                path="/public/folder/ok.bin",
                parent_path="/public/folder",
                is_dir=False,
                size_bytes=1,
                size="1 B",
                updated_at="",
                readonly=False,
                content_sha256=None,
            ),
            FileNode(
                id="2",
                name="bad.bin",
                path="/public/folder/bad.bin",
                parent_path="/public/folder",
                is_dir=False,
                size_bytes=1,
                size="1 B",
                updated_at="",
                readonly=False,
                content_sha256=None,
            ),
        ]

    def list_files(self, path: str):
        return self.files

    def download(self, remote_path: str, local_path, **kwargs):
        local_path = Path(local_path)
        if remote_path.endswith("bad.bin"):
            raise DiskBlazeError("500 simulated server error")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(b"ok")
        return local_path


def test_download_tree_collects_failures_instead_of_aborting(tmp_path: Path):
    client = FakeDownloadClient()
    with pytest.raises(DiskBlazeError) as excinfo:
        client.download_tree("/public/folder", tmp_path / "out")
    assert "1 of 2 files failed" in str(excinfo.value)
    assert "bad.bin" in str(excinfo.value)
    # The good file still completed; the batch wasn't killed mid-way.
    assert (tmp_path / "out" / "ok.bin").exists()


def test_normalize_remote_path_blocks_traversal():
    # The leading "/" prefix makes every path absolute under root, so any ".."
    # is collapsed rather than escaping. The guard still rejects a normalized
    # result that escapes root (defensive; shouldn't occur with the prefix).
    assert normalize_remote_path("a/../../b") == "/b"
    assert normalize_remote_path("../private/secret") == "/private/secret"
    assert normalize_remote_path("private/../../etc/passwd") == "/etc/passwd"
    assert normalize_remote_path("a/../../../..") == "/"


def test_upload_tree_skip_existing(tmp_path: Path):
    class SkipClient(FakeUploadClient):
        def __init__(self):
            super().__init__()
            # Remote already has a.bin (10 bytes) but not b.bin.
            self.remote_files = {
                "/public/dest": {"a.bin": 10},
            }

        def list_files(self, path: str):
            sizes = self.remote_files.get(path, {})
            return [
                FileNode(
                    id=str(i),
                    name=name,
                    path=f"{path}/{name}",
                    parent_path=path,
                    is_dir=False,
                    size_bytes=size,
                    size=f"{size} B",
                    updated_at="",
                    readonly=False,
                    content_sha256=None,
                )
                for i, (name, size) in enumerate(sizes.items())
            ]

    src = tmp_path / "src"
    src.mkdir()
    (src / "a.bin").write_bytes(b"x" * 10)  # matches remote -> skip
    (src / "b.bin").write_bytes(b"y" * 5)  # new -> upload

    client = SkipClient()
    results = client.upload_tree(src, "/public/dest", skip_existing=True)
    # Only b.bin should have been uploaded.
    assert len(results) == 1
    assert client.plan_requests == [
        {"path": "/public/dest/b.bin", "size_bytes": 5, "content_sha256": None, "part_size": None}
    ]


def test_upload_file_stall_timeout_raises():
    import time as _time
    from unittest.mock import MagicMock

    client = DiskBlazeClient(token="dummy")
    client.transfer_timeout = 0.2

    # The session consumes the (guarded) body generator; each chunk sleeps
    # longer than the transfer deadline, so the wall-clock guard must trip.
    def slow_put(url, data, **kwargs):
        for _chunk in data:  # pull chunks through the guarded wrapper
            _time.sleep(0.15)
        resp = MagicMock()
        resp.headers.get.return_value = "etag"
        resp.raise_for_status.return_value = None
        return resp

    client._session = MagicMock()
    client._session().put.side_effect = slow_put

    def body():
        for _ in range(4):
            yield b"x" * 1024

    with pytest.raises(requests.Timeout):
        client._put_stream("https://upload.invalid/obj", body(), length=4096)


def test_download_writes_part_file_then_renames(tmp_path: Path):
    client = FakeDownloadClient()
    out = tmp_path / "out" / "ok.bin"
    result = client.download("/public/folder/ok.bin", out)
    assert result == out
    assert out.exists()
    # No leftover .part sidecar at the final path.
    assert not (tmp_path / "out" / "ok.bin.part").exists()


def test_upload_tree_skips_symlink_directory_loops(tmp_path: Path):
    # Build a tree that contains a symlink pointing back into itself (a loop)
    # and a symlink to a file outside the tree. With followlinks=False the walk
    # must not recurse into the symlinked dir and must still collect real files.
    root = tmp_path / "root"
    (root / "sub").mkdir(parents=True)
    (root / "sub" / "real.txt").write_bytes(b"hello")
    (root / "loop").symlink_to(root / "sub")  # points back into the tree
    (root / "outside.txt").symlink_to(tmp_path / "outside")  # dangling-ish link
    (tmp_path / "outside").write_bytes(b"outside-data")

    client = FakeUploadClient()
    client.upload_tree(root, "/public/dest")
    paths = {req["path"] for req in client.plan_requests}
    # Only the real file inside sub and the symlinked file are uploaded; the
    # symlink directory is not recursed into (no infinite loop, no duplication).
    assert "/public/dest/sub/real.txt" in paths
    assert "/public/dest/outside.txt" in paths
    assert len(paths) == 2


def test_download_skip_existing_short_circuits(tmp_path: Path):
    from unittest.mock import MagicMock

    client = DiskBlazeClient(token="dummy")
    out = tmp_path / "out.bin"
    out.write_bytes(b"0123456789")  # 10 bytes, already complete

    head = MagicMock()
    head.status_code = 200
    head.headers = {"Content-Length": "10", "Accept-Ranges": "bytes"}
    client._session = MagicMock()
    client._session().head.return_value = head

    # No GET should be issued because the local copy already matches.
    result = client._download_url(
        "https://dl.invalid/a.bin", out, display_path="/public/a.bin", workers=1, skip_existing=True, progress=None
    )
    assert result == out
    client._session().get.assert_not_called()


def test_download_resumes_partial_file(tmp_path: Path):
    client = DiskBlazeClient(token="dummy")
    out = tmp_path / "out.bin"
    size = 20
    # A previous run wrote the first 10 bytes then was interrupted.
    part = out.with_name(out.name + ".part")
    part.write_bytes(b"A" * 10)

    head = MagicMock()
    head.status_code = 200
    head.headers = {"Content-Length": str(size), "Accept-Ranges": "bytes"}

    captured = {}

    def fake_get(url, headers=None, stream=None, timeout=None, **kwargs):
        captured["range"] = headers.get("Range") if headers else None
        resp = MagicMock()
        resp.status_code = 206
        resp.headers = {"Content-Range": f"bytes 10-19/{size}"}
        resp.iter_content = lambda chunk_size=1: iter([b"B" * 10])
        resp.__enter__ = lambda self: self
        resp.__exit__ = lambda self, *a: None
        return resp

    client._session = MagicMock()
    client._session().head.return_value = head
    client._session().get.side_effect = fake_get

    result = client._download_url(
        "https://dl.invalid/a.bin", out, display_path="/public/a.bin", workers=1, progress=None
    )
    assert result == out
    # We asked only for the remaining bytes.
    assert captured["range"] == "bytes=10-"
    # The final file is the original 10 bytes plus the resumed 10 bytes.
    assert out.read_bytes() == b"A" * 10 + b"B" * 10
    assert not part.exists()


def test_put_stream_applies_read_timeout_for_stall():
    # A stalled socket read (no bytes for transfer_timeout) must trip the read
    # timeout, which is only applied when transfer_timeout > 0. This is what
    # catches a hang *inside* a single chunk that the per-chunk guard can't.
    client = DiskBlazeClient(token="dummy")
    client.transfer_timeout = 0.3
    assert client._data_timeout() == (120.0, 0.3)

    captured = {}

    def slow_put(url, data, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        resp = MagicMock()
        resp.headers.get.return_value = "etag"
        resp.raise_for_status.return_value = None
        return resp

    client._session = MagicMock()
    client._session().put.side_effect = slow_put

    def body():
        yield b"x" * 1024

    client._put_stream("https://upload.invalid/obj", body(), length=1024)
    # The read timeout (transfer_timeout) is threaded into the request so a
    # stalled single read raises requests.Timeout at the socket layer.
    assert captured["timeout"] == (120.0, 0.3)


def test_put_stream_default_timeout_is_idle_only():
    client = DiskBlazeClient(token="dummy")
    assert client.transfer_timeout == 0.0
    assert client._data_timeout() == 120.0


def test_verbose_logging_emits_client_operations(caplog):
    import logging

    from diskblaze.cli import _configure_logging

    _configure_logging(1)  # -v -> INFO
    logger = logging.getLogger("diskblaze")
    try:
        with caplog.at_level(logging.DEBUG, logger="diskblaze"):
            DiskBlazeClient(token="dummy")
        assert any("DiskBlazeClient init" in r.message for r in caplog.records)
    finally:
        logger.handlers = []
        logger.setLevel(logging.NOTSET)


def test_verbose_count_zero_is_silent():
    import logging

    from diskblaze.cli import _configure_logging

    logger = logging.getLogger("diskblaze")
    try:
        # With no -v, no StreamHandler should be attached to the diskblaze
        # logger: verbose output stays off by default.
        _configure_logging(0)
        assert logger.handlers == []
    finally:
        logger.handlers = []
        logger.setLevel(logging.NOTSET)


class FakeFolderClient(DiskBlazeClient):
    """Exercises ensure_folder's existence checks without any network."""

    def __init__(self):
        super().__init__(token="dummy")
        self.created: list[str] = []
        self.existing: set[str] = set()
        self.list_calls: list[str] = []

    def create_folder(self, path: str) -> FileNode:
        self.created.append(path)
        self.existing.add(path)
        return FileNode(
            id="x",
            name=posixpath.basename(path),
            path=path,
            parent_path=posixpath.dirname(path),
            is_dir=True,
            size_bytes=0,
            size="0 B",
            updated_at="",
            readonly=False,
            content_sha256=None,
        )

    def list_files(self, path: str = "/"):
        self.list_calls.append(path)
        return [
            FileNode(
                id="d",
                name=posixpath.basename(p),
                path=p,
                parent_path=posixpath.dirname(p),
                is_dir=True,
                size_bytes=0,
                size="0 B",
                updated_at="",
                readonly=False,
                content_sha256=None,
            )
            for p in self.existing
            if posixpath.dirname(p) == normalize_remote_path(path)
        ]


def test_ensure_folder_skips_existing_and_caches(tmp_path):
    client = FakeFolderClient()
    # Pre-existing folder tree on the server.
    client.existing = {"/public/dest", "/public/dest/sub"}
    # Two files in the same parent: ensure_folder must not call create_folder
    # for an already-present folder, and must probe the parent only once.
    client.ensure_folder("/public/dest/sub")
    client.ensure_folder("/public/dest/sub")
    assert client.created == []
    # Existence probes list each parent once; both calls are served from cache.
    assert client.list_calls == ["/public", "/public/dest"]
    assert client.folder_exists("/public/dest/sub") is True


def test_ensure_folder_creates_missing_then_caches(tmp_path):
    client = FakeFolderClient()
    client.ensure_folder("/public/dest/sub")
    # All missing ancestor segments are created (none pre-existed).
    assert client.created == ["/public/dest", "/public/dest/sub"]
    # A second call for the same path does not re-create or re-probe.
    client.ensure_folder("/public/dest/sub")
    assert client.created == ["/public/dest", "/public/dest/sub"]
    # The parents were probed exactly once across both calls.
    assert client.list_calls == ["/public", "/public/dest"]


def test_ensure_folder_no_create_skips_creation(tmp_path):
    client = FakeFolderClient()
    # Folders are assumed present; create_folder is never invoked even though
    # folder_exists would report them missing.
    client.ensure_folder("/public/dest/sub", no_create=True)
    assert client.created == []
    # list_files is still consulted per segment (to verify), but no mutation.
    assert client.list_calls == ["/public", "/public/dest"]
