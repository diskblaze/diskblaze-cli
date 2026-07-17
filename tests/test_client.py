from __future__ import annotations

import hashlib
from pathlib import Path

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
            self.created: list[str] = []

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
            self.created: list[str] = []

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

    def ensure_folder(self, path: str) -> None:
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
        for chunk in body:
            self.uploaded.extend(chunk)
        assert len(self.uploaded) == length
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

    def ensure_folder(self, path: str) -> None:
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
