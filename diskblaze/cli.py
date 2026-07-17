from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
import threading
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table

from . import config
from .client import (
    DiskBlazeClient,
    DiskBlazeError,
    TransferProgress,
    endpoint_from_base,
    join_remote,
)

console = Console()


class ProgressMux:
    def __init__(self, progress: Progress):
        self.progress = progress
        self.lock = threading.Lock()
        self.tasks: dict[str, TaskID] = {}

    def __call__(self, event: TransferProgress) -> None:
        key = event.path
        with self.lock:
            task_id = self.tasks.get(key)
            if task_id is None:
                task_id = self.progress.add_task(
                    f"{event.phase} {short_path(key)}",
                    total=max(1, event.total_bytes),
                    start=True,
                )
                self.tasks[key] = task_id
            self.progress.update(
                task_id,
                description=f"{event.phase} {short_path(key)}",
                completed=min(event.transferred_bytes, event.total_bytes),
                total=max(1, event.total_bytes or event.transferred_bytes or 1),
            )


def short_path(value: str, width: int = 72) -> str:
    if len(value) <= width:
        return value
    keep = max(12, (width - 3) // 2)
    return f"{value[:keep]}...{value[-keep:]}"


def resolve_endpoint(args: argparse.Namespace) -> str:
    """Endpoint order: --endpoint, env, saved login endpoint, default host."""
    raw = (
        getattr(args, "endpoint", None)
        or os.environ.get("DISKBLAZE_URL")
        or os.environ.get("DISKBLAZE_GQL_URL")
        or config.stored_endpoint()
        or "https://diskblaze.com"
    )
    return endpoint_from_base(raw)


def resolve_token(args: argparse.Namespace) -> str | None:
    """Token order: --token, env vars, saved login credentials."""
    return (
        getattr(args, "token", None)
        or os.environ.get("DISKBLAZE_TOKEN")
        or os.environ.get("DISKBLAZE_API_KEY")
        or config.stored_token()
    )


def build_client(args: argparse.Namespace) -> DiskBlazeClient:
    token = resolve_token(args)
    if not token:
        raise DiskBlazeError("not authenticated: run `diskblaze login`, pass --token, or set DISKBLAZE_TOKEN")
    endpoint = resolve_endpoint(args)
    workers = max(1, int(getattr(args, "workers", 1) or 1))
    file_workers = max(1, int(getattr(args, "file_workers", 1) or 1))
    # Bound total concurrency so --workers 64 --file-workers 8 can't open 520
    # connections. Total live HTTP connections are capped by file_workers*workers.
    total = min(workers * file_workers, 32)
    # Control-plane (GraphQL) calls are far slower than S3 transfers and the
    # endpoint 500s under heavy concurrency, so cap them independently of the
    # data-plane workers. More file_workers means more simultaneous plan/folder
    # calls, so scale the GraphQL cap with it (bounded).
    graphql_concurrency = max(4, min(file_workers, 16))
    return DiskBlazeClient(
        endpoint=endpoint,
        token=token,
        timeout=args.timeout,
        pool_size=max(total + 8, 32),
        graphql_concurrency=graphql_concurrency,
    )


def transfer_progress() -> Progress:
    return Progress(
        TextColumn("[progress.description]{task.description}", table_column=None),
        BarColumn(),
        DownloadColumn(binary_units=True),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    )


def command_login(args: argparse.Namespace) -> int:
    token = getattr(args, "token", None) or os.environ.get("DISKBLAZE_TOKEN") or os.environ.get("DISKBLAZE_API_KEY")
    if not token:
        if not sys.stdin.isatty():
            raise DiskBlazeError("no token provided; pass --token or set DISKBLAZE_TOKEN")
        token = getpass.getpass("DiskBlaze API token: ").strip()
    if not token:
        raise DiskBlazeError("no token provided")

    endpoint = resolve_endpoint(args)
    # Validate the token before saving so a typo fails loudly here.
    client = DiskBlazeClient(endpoint=endpoint, token=token, timeout=args.timeout)
    user = client.me()

    # Only persist a non-default endpoint so upstream URL changes still apply.
    save_endpoint = getattr(args, "endpoint", None) or config.stored_endpoint()
    path = config.save_credentials(token, endpoint if save_endpoint else None)
    console.print(f"[green]logged in[/green] as {user.username} ({user.used} used of {user.quota})")
    console.print(f"[dim]credentials saved to {path}[/dim]")
    return 0


def command_logout(args: argparse.Namespace) -> int:
    if config.clear_credentials():
        console.print("[green]logged out[/green]")
    else:
        console.print("[yellow]not logged in[/yellow]")
    return 0


def command_whoami(args: argparse.Namespace) -> int:
    client = build_client(args)
    user = client.me()
    if args.json:
        console.print(json.dumps(user.__dict__, indent=2))
        return 0
    table = Table(show_header=False, box=None)
    table.add_column("Key", style="dim")
    table.add_column("Value")
    table.add_row("Username", user.username)
    table.add_row("Used", user.used)
    table.add_row("Remaining", user.remaining)
    table.add_row("Quota", user.quota)
    table.add_row("API access", "enabled" if user.api_access_enabled else "disabled")
    table.add_row("Direct UL", "enabled" if user.direct_ul_enabled else "disabled")
    console.print(table)
    return 0


def command_ls(args: argparse.Namespace) -> int:
    client = build_client(args)
    rows = client.list_files(args.path)
    if args.json:
        console.print(json.dumps([node.__dict__ for node in rows], indent=2))
        return 0
    table = Table(title=args.path, show_header=True, header_style="bold")
    table.add_column("Name", overflow="fold")
    table.add_column("Type", style="dim")
    table.add_column("Size", justify="right")
    table.add_column("Modified", overflow="fold")
    for node in rows:
        table.add_row(node.name, "dir" if node.is_dir else "file", "" if node.is_dir else node.size, node.updated_at)
    console.print(table)
    return 0


def command_search(args: argparse.Namespace) -> int:
    client = build_client(args)
    rows, has_more = client.search_files(
        args.query,
        path_prefix=args.path,
        kind=args.kind,
        min_size_bytes=args.min_size_bytes,
        max_size_bytes=args.max_size_bytes,
        updated_after=args.updated_after,
        updated_before=args.updated_before,
        limit=args.limit,
        offset=args.offset,
    )
    if args.json:
        console.print(json.dumps({"hasMore": has_more, "items": [node.__dict__ for node in rows]}, indent=2))
        return 0
    table = Table(title=f"Search: {args.query}", show_header=True, header_style="bold")
    table.add_column("Path", overflow="fold")
    table.add_column("Type", style="dim")
    table.add_column("Size", justify="right")
    table.add_column("Modified", overflow="fold")
    for node in rows:
        table.add_row(node.path, "dir" if node.is_dir else "file", "" if node.is_dir else node.size, node.updated_at)
    console.print(table)
    if has_more:
        console.print("[yellow]More results available. Increase --limit or use --offset.[/yellow]")
    return 0


def command_mkdir(args: argparse.Namespace) -> int:
    client = build_client(args)
    client.ensure_folder(args.path)
    console.print(f"[green]created[/green] {args.path}")
    return 0


def command_mv(args: argparse.Namespace) -> int:
    client = build_client(args)
    node = client.move(args.src, args.dst)
    console.print(f"[green]moved[/green] {args.src} -> {node.path}")
    return 0


def command_rm(args: argparse.Namespace) -> int:
    if not args.yes:
        prompt = f"Move {args.path!r} to Trash? Type DELETE to continue: "
        if console.input(prompt) != "DELETE":
            console.print("[yellow]cancelled[/yellow]")
            return 130
    client = build_client(args)
    message = client.delete(args.path)
    console.print(f"[green]deleted[/green] {args.path}: {message}")
    return 0


def command_url(args: argparse.Namespace) -> int:
    client = build_client(args)
    url = (
        client.zip_url(args.path, expires_seconds=args.expires)
        if args.zip
        else client.download_url(args.path, expires_seconds=args.expires)
    )
    console.print(url)
    return 0


def command_upload(args: argparse.Namespace) -> int:
    client = build_client(args)
    local = Path(args.local).expanduser()
    if not local.exists():
        raise DiskBlazeError(f"local path does not exist: {local}")
    remote = args.remote or join_remote("/private", local.name)
    progress = transfer_progress()
    with progress:
        mux = ProgressMux(progress)
        if local.is_dir():
            result = client.upload_tree(
                local,
                remote,
                workers=args.workers,
                file_workers=args.file_workers,
                checksum=not args.no_sha256,
                progress=mux,
            )
            console.print(f"[green]uploaded[/green] {len(result)} files to {remote}")
        else:
            node = client.upload_file(
                local,
                remote,
                workers=args.workers,
                part_size=args.part_size,
                checksum=not args.no_sha256,
                progress=mux,
            )
            console.print(f"[green]uploaded[/green] {node.path} ({node.size})")
    return 0


def command_download(args: argparse.Namespace) -> int:
    client = build_client(args)
    output = Path(args.local).expanduser()
    progress = transfer_progress()
    with progress:
        mux = ProgressMux(progress)
        if args.recursive:
            paths = client.download_tree(
                args.remote,
                output,
                workers=args.workers,
                file_workers=args.file_workers,
                expires_seconds=args.expires,
                progress=mux,
            )
            console.print(f"[green]downloaded[/green] {len(paths)} files from {args.remote} -> {output}")
        else:
            path = client.download(
                args.remote,
                output,
                workers=args.workers,
                expires_seconds=args.expires,
                as_zip=args.zip,
                progress=mux,
            )
            console.print(f"[green]downloaded[/green] {args.remote} -> {path}")
    return 0


def add_common(parser: argparse.ArgumentParser, *, suppress_defaults: bool = False) -> None:
    default = argparse.SUPPRESS if suppress_defaults else None
    timeout_default = argparse.SUPPRESS if suppress_defaults else 120.0
    workers_default = argparse.SUPPRESS if suppress_defaults else 64
    file_workers_default = argparse.SUPPRESS if suppress_defaults else 8
    parser.add_argument(
        "--endpoint",
        default=default,
        help="GraphQL endpoint or DiskBlaze base URL. Default: https://diskblaze.com/graphql",
    )
    parser.add_argument(
        "--token", default=default, help="API key. Default: saved login, DISKBLAZE_TOKEN, or DISKBLAZE_API_KEY"
    )
    parser.add_argument("--timeout", type=float, default=timeout_default)
    parser.add_argument(
        "--workers", type=int, default=workers_default, help="Multipart upload/download workers per file."
    )
    parser.add_argument(
        "--file-workers", type=int, default=file_workers_default, help="Concurrent files for folder uploads/downloads."
    )


def add_command_common(parser: argparse.ArgumentParser) -> None:
    add_common(parser, suppress_defaults=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="diskblaze",
        description="Fast DiskBlaze GraphQL/gateway CLI for uploads and downloads.",
    )
    add_common(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    login_cmd = sub.add_parser("login", help="Save a DiskBlaze API token for later commands.")
    add_command_common(login_cmd)
    login_cmd.set_defaults(func=command_login)

    logout_cmd = sub.add_parser("logout", help="Remove saved DiskBlaze credentials.")
    add_command_common(logout_cmd)
    logout_cmd.set_defaults(func=command_logout)

    whoami_cmd = sub.add_parser("whoami", help="Show the authenticated DiskBlaze account.")
    add_command_common(whoami_cmd)
    whoami_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    whoami_cmd.set_defaults(func=command_whoami)

    ls_cmd = sub.add_parser("ls", help="List a remote folder.")
    add_command_common(ls_cmd)
    ls_cmd.add_argument("path", nargs="?", default="/")
    ls_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    ls_cmd.set_defaults(func=command_ls)

    search_cmd = sub.add_parser("search", help="Search remote files recursively.")
    add_command_common(search_cmd)
    search_cmd.add_argument("query", nargs="?", default="")
    search_cmd.add_argument("--path", default=None, help="Optional remote folder prefix.")
    search_cmd.add_argument(
        "--kind",
        choices=["file", "folder", "image", "video", "audio", "document", "archive", "code"],
        help="Optional result type filter.",
    )
    search_cmd.add_argument("--min-size-bytes", type=int, default=None)
    search_cmd.add_argument("--max-size-bytes", type=int, default=None)
    search_cmd.add_argument("--updated-after", default=None, help="ISO timestamp lower bound.")
    search_cmd.add_argument("--updated-before", default=None, help="ISO timestamp upper bound.")
    search_cmd.add_argument("--limit", type=int, default=200)
    search_cmd.add_argument("--offset", type=int, default=0)
    search_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    search_cmd.set_defaults(func=command_search)

    mkdir_cmd = sub.add_parser("mkdir", help="Create a remote folder path.")
    add_command_common(mkdir_cmd)
    mkdir_cmd.add_argument("path")
    mkdir_cmd.set_defaults(func=command_mkdir)

    mv_cmd = sub.add_parser("mv", help="Move or rename a remote file/folder.")
    add_command_common(mv_cmd)
    mv_cmd.add_argument("src")
    mv_cmd.add_argument("dst")
    mv_cmd.set_defaults(func=command_mv)

    rm_cmd = sub.add_parser("rm", help="Move a remote file/folder to Trash.")
    add_command_common(rm_cmd)
    rm_cmd.add_argument("path")
    rm_cmd.add_argument("-y", "--yes", action="store_true", help="Skip the DELETE confirmation prompt.")
    rm_cmd.set_defaults(func=command_rm)

    upload_cmd = sub.add_parser("upload", aliases=["ul"], help="Upload a file or folder.")
    add_command_common(upload_cmd)
    upload_cmd.add_argument("local")
    upload_cmd.add_argument("remote", nargs="?")
    upload_cmd.add_argument("--part-size", type=int, default=None, help="Override multipart part size in bytes.")
    upload_cmd.add_argument(
        "--no-sha256",
        action="store_true",
        help="Skip the local SHA-256 pre-read. Faster to start, but the server may need a backend readback.",
    )
    upload_cmd.set_defaults(func=command_upload)

    download_cmd = sub.add_parser(
        "download", aliases=["dl"], help="Download a file, a folder as ZIP, or a folder recursively."
    )
    add_command_common(download_cmd)
    download_cmd.add_argument("remote")
    download_cmd.add_argument("local")
    download_cmd.add_argument("--zip", action="store_true", help="Request a ZIP download for a folder.")
    download_cmd.add_argument(
        "--recursive", "-r", action="store_true", help="Download a remote folder as normal files in parallel."
    )
    download_cmd.add_argument("--expires", type=int, default=3600, help="Signed URL TTL in seconds.")
    download_cmd.set_defaults(func=command_download)

    url_cmd = sub.add_parser("url", help="Print a signed gateway download URL.")
    add_command_common(url_cmd)
    url_cmd.add_argument("path")
    url_cmd.add_argument("--zip", action="store_true", help="Create a ZIP URL for a folder.")
    url_cmd.add_argument("--expires", type=int, default=3600, help="Signed URL TTL in seconds.")
    url_cmd.set_defaults(func=command_url)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        console.print("\n[yellow]cancelled[/yellow]")
        return 130
    except Exception as exc:
        console.print(f"[red]error:[/red] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
