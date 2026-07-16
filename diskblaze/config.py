"""Persistent credential storage for the DiskBlaze CLI.

Credentials live in a small JSON file so that ``diskblaze login`` can save a
token once and every later command can pick it up without an environment
variable. The file is written with ``0600`` permissions.

Resolution order used by the CLI (see ``cli.resolve_token``):

1. an explicit ``--token`` flag
2. the ``DISKBLAZE_TOKEN`` / ``DISKBLAZE_API_KEY`` environment variables
3. the token saved by ``diskblaze login`` in this file
"""

from __future__ import annotations

import json
import os
from pathlib import Path


def config_dir() -> Path:
    """Return the directory that holds the credentials file.

    Honors ``DISKBLAZE_CONFIG_DIR`` first, then ``XDG_CONFIG_HOME``, then falls
    back to ``~/.config/diskblaze``.
    """
    override = os.environ.get("DISKBLAZE_CONFIG_DIR")
    if override:
        return Path(override).expanduser()
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".config"
    return base / "diskblaze"


def config_path() -> Path:
    return config_dir() / "credentials.json"


def load_config() -> dict:
    path = config_path()
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (FileNotFoundError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save_credentials(token: str, endpoint: str | None = None) -> Path:
    """Persist ``token`` (and optionally ``endpoint``) to the config file."""
    data = load_config()
    data["token"] = token
    if endpoint:
        data["endpoint"] = endpoint
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temp file then replace so a crash never truncates the config.
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)
    return path


def clear_credentials() -> bool:
    """Delete the stored credentials file. Returns True if one existed."""
    path = config_path()
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def stored_token() -> str | None:
    value = load_config().get("token")
    return str(value) if value else None


def stored_endpoint() -> str | None:
    value = load_config().get("endpoint")
    return str(value) if value else None
