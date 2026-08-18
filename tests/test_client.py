from __future__ import annotations

import hashlib
from pathlib import Path
import threading

import pytest
import requests

from diskblaze.client import (
    DiskBlazeClient,
    DiskBlazeError,
    FileNode,
    UploadPlan,
    _ProgressReader,
    endpoint_from_base,
    join_remote,
    normalize_remote_path,
    preferred_part_size,
)
from diskblaze.cli import build_parser


def test_remote_path_helpers_normalize_posix_paths():
    assert normalize_remote_path("private/../public//demo.txt") == "/public/demo.txt"
    assert normalize_remote_path("/") == "/"
    assert join_remote("/private/base", "nested\\file.bin") == "/private/base/nested/file.bin"


def test_endpoint_from_base_accepts_base_or_graphql_url():
    assert endpoint_from_base("https://diskblaze.com") == "https://diskblaze.com/graphql"
    assert endpoint_from_base("https://diskblaze.com/graphql") == "https://diskblaze.com/graphql"
    assert endpoint_from_base("https://gql.hostingsolutions.top/graphql") == "https://diskblaze.com/graphql"


def test_preferred_part_size_preserves_parallelism_for_medium_files():
    mib = 1024 * 1024
    assert preferred_part_size(7 * mib) is None
    assert preferred_part_size(8 * mib) == 8 * mib
    assert preferred_part_size(64 * mib) == 8 * mib
    assert preferred_part_size(128 * mib) == 16 * mib
    assert preferred_part_size(512 * mib) == 32 * mib
    assert preferred_part_size(2 * 1024 * mib) == 64 * mib


def test_search_defaults_to_root_path_prefix():
    class SearchClient(DiskBlazeClient):
        def __init__(self):
            pass

        def graphql(self, _query, variables=None):
            assert variables["pathPrefix"] == "/"
            return {"searchFiles": {"items": [], "hasMore": False}}

    assert SearchClient().search_files("smoke") == ([], False)


def test_list_files_paginates_without_losing_large_folders():
    class PagedClient(DiskBlazeClient):
        def __init__(self):
            self.offsets: list[int] = []

        def graphql(self, _query, variables=None):
            self.offsets.append(variables["offset"])
            offset = variables["offset"]
            return {
                "files": {
                    "items": [
                        {
                            "id": str(offset),
                            "name": f"{offset}.bin",
                            "path": f"/public/{offset}.bin",
                            "isDir": False,
                            "sizeBytes": 1,
                        }
                    ],
                    "hasMore": offset == 0,
                }
            }

    client = PagedClient()
    assert [node.name for node in client.iter_files("/public", page_size=1)] == ["0.bin", "1.bin"]
    assert client.offsets == [0, 1]


class FakeResponse:
    def __init__(self, status_code: int, payload: dict, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = 0

    def post(self, *_args, **_kwargs):
        self.calls += 1
        return next(self.responses)


def test_graphql_does_not_retry_application_errors():
    client = DiskBlazeClient(token="test")
    session = FakeSession([FakeResponse(200, {"errors": [{"message": "path not found"}]})])
    client._local.session = session

    with pytest.raises(DiskBlazeError, match="path not found"):
        client.graphql("query Test { test }")
    assert session.calls == 1


def test_graphql_does_not_retry_non_transient_http_errors():
    client = DiskBlazeClient(token="test")
    session = FakeSession([FakeResponse(403, {})])
    client._local.session = session

    with pytest.raises(DiskBlazeError, match="403"):
        client.graphql("query Test { test }")
    assert session.calls == 1


def test_graphql_retries_temporary_status_with_a_fresh_attempt(monkeypatch):
    client = DiskBlazeClient(token="test")
    session = FakeSession(
        [
            FakeResponse(500, {}),
            FakeResponse(200, {"data": {"ok": True}}),
        ]
    )
    client._local.session = session
    monkeypatch.setattr(client, "_discard_session", lambda: None)
    monkeypatch.setattr("diskblaze.client.time.sleep", lambda _seconds: None)

    assert client.graphql("query Test { test }") == {"ok": True}
    assert session.calls == 2


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

    def create_upload_plan(self, path: str, *, size_bytes: int, content_sha256: str | None = None, part_size: int | None = None):
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


def test_upload_file_finalizes_zero_byte_files_without_gateway_put(tmp_path: Path):
    local = tmp_path / "empty.bin"
    local.write_bytes(b"")
    client = FakeUploadClient()

    node = client.upload_file(local, "/private/up/empty.bin", checksum=False)

    assert client.uploaded == b""
    assert len(client.plan_requests) == 1
    assert len(client.completed) == 1
    assert node.size_bytes == 0


def test_progress_reader_prepares_fixed_length_request(tmp_path: Path):
    local = tmp_path / "part.bin"
    local.write_bytes(b"x" * 1024)
    with local.open("rb") as handle:
        reader = _ProgressReader(handle, length=512, offset=0, callback=None)
        prepared = requests.Request("PUT", "https://example.invalid", data=reader).prepare()

    assert prepared.headers["Content-Length"] == "512"
    assert "Transfer-Encoding" not in prepared.headers


def test_upload_tree_cancels_queued_files_after_failure(tmp_path: Path):
    for index in range(20):
        (tmp_path / f"{index:02d}.bin").write_bytes(b"x")

    attempted: list[str] = []
    first_attempt = threading.Event()

    class FailingClient(DiskBlazeClient):
        def __init__(self):
            pass

        def ensure_folder(self, _path):
            return None

        def upload_file(self, path, *_args, **_kwargs):
            attempted.append(Path(path).name)
            if not first_attempt.is_set():
                first_attempt.set()
                raise RuntimeError("destination removed")
            return None

    try:
        FailingClient().upload_tree(tmp_path, "/public/test", file_workers=1)
    except Exception as exc:
        assert "destination removed" in str(exc)
    else:
        raise AssertionError("upload_tree should fail")

    assert len(attempted) == 1


def test_upload_tree_keeps_only_a_bounded_queue(tmp_path: Path):
    for index in range(25):
        (tmp_path / f"{index:02d}.bin").write_bytes(b"x")

    started: list[str] = []

    class BoundedClient(DiskBlazeClient):
        def __init__(self):
            pass

        def ensure_folder(self, _path):
            return None

        def upload_file(self, path, *_args, **_kwargs):
            started.append(Path(path).name)
            return FileNode("id", Path(path).name, "/public/x", "/public", False, 1, "1 B", "now")

    result = BoundedClient().upload_tree(
        tmp_path,
        "/public/test",
        file_workers=1,
        max_inflight=2,
        collect_results=False,
    )
    assert result == 25
    assert len(started) == 25
