from __future__ import annotations

import getpass
import logging
import netrc
import os
import re
import sys
from pathlib import Path

import requests

EARTHDATA_MACHINE = "urs.earthdata.nasa.gov"
_PROFILE_URL = "https://urs.earthdata.nasa.gov/profile"
_MACHINE_BLOCK_RE = re.compile(r"(?ms)^machine\s+urs\.earthdata\.nasa\.gov\b.*?(?=^machine\s+\S+|\Z)")


class AuthError(RuntimeError):
    pass


def load_credentials_from_netrc(netrc_path: Path | None = None) -> tuple[str, str] | None:
    path = netrc_path or (Path.home() / ".netrc")
    if not path.exists():
        return None

    try:
        parsed = netrc.netrc(str(path))
    except netrc.NetrcParseError:
        return None
    auth = parsed.authenticators(EARTHDATA_MACHINE)
    if not auth:
        return None

    username, _, password = auth
    if not username or not password:
        return None
    return username, password


def validate_credentials(
    username: str,
    password: str,
    timeout_sec: int = 15,
    logger: logging.Logger | None = None,
) -> bool:
    try:
        with requests.Session() as session:
            # Keep auth flow stable under mixed local proxy settings (e.g., Clash env vars).
            session.trust_env = False
            response = session.get(
                _PROFILE_URL,
                auth=(username, password),
                timeout=timeout_sec,
                allow_redirects=True,
            )
    except requests.RequestException as exc:
        if logger:
            logger.debug("Credential validation network error: %s", exc)
        return False

    if logger and response.status_code != 200:
        logger.debug("Credential validation HTTP %d from %s", response.status_code, response.url)
    return response.status_code == 200 and "urs.earthdata.nasa.gov" in response.url


def write_netrc_entry(username: str, password: str, netrc_path: Path | None = None) -> Path:
    path = netrc_path or (Path.home() / ".netrc")
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if existing:
        try:
            netrc.netrc(str(path))
        except netrc.NetrcParseError:
            # Recover from malformed historical files by rebuilding the entry block.
            existing = ""

    block = f"machine {EARTHDATA_MACHINE}\n  login {username}\n  password {password}\n"

    if _MACHINE_BLOCK_RE.search(existing):
        updated = _MACHINE_BLOCK_RE.sub(block, existing)
    else:
        updated = existing.rstrip() + ("\n\n" if existing.strip() else "") + block

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(updated, encoding="utf-8")
    if sys.platform != "win32":
        temp_path.chmod(0o600)
    os.replace(temp_path, path)
    if sys.platform != "win32":
        path.chmod(0o600)
    return path


def get_or_create_credentials(
    logger: logging.Logger,
    interactive: bool = True,
    netrc_path: Path | None = None,
) -> tuple[str, str]:
    existing = load_credentials_from_netrc(netrc_path=netrc_path)
    if existing:
        username, password = existing
        if validate_credentials(username, password, logger=logger):
            logger.info("Using valid credentials from .netrc")
            return username, password
        logger.warning(
            "Credentials in .netrc failed validation (rejected by server or network issue). Run with --verbose for details."
        )

    if not interactive:
        raise AuthError("No valid Earthdata credentials available and interactive mode is disabled.")

    # Infinite retries by design: user can interrupt manually when needed.
    while True:
        username = input("Earthdata username: ").strip()
        password = getpass.getpass("Earthdata password: ").strip()

        if not username or not password:
            print("Username/password cannot be empty, please retry.")
            continue

        if validate_credentials(username, password, logger=logger):
            path = write_netrc_entry(username, password, netrc_path=netrc_path)
            logger.info("Credentials validated and saved to %s", path)
            return username, password

        print("Credential validation failed. Please retry.")
