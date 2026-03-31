from pathlib import Path

from s1downloader.auth import load_credentials_from_netrc, validate_credentials, write_netrc_entry


def test_write_and_load_netrc_roundtrip(tmp_path: Path):
    netrc_path = tmp_path / ".netrc"
    write_netrc_entry("demo_user", "demo_pass", netrc_path=netrc_path)

    creds = load_credentials_from_netrc(netrc_path=netrc_path)
    assert creds == ("demo_user", "demo_pass")


def test_load_credentials_ignores_malformed_netrc(tmp_path: Path):
    netrc_path = tmp_path / ".netrc"
    # Simulate previously broken output with literal "\n" sequences.
    netrc_path.write_text(
        r"machine urs.earthdata.nasa.gov\n  login bad\n  password bad\n",
        encoding="utf-8",
    )

    creds = load_credentials_from_netrc(netrc_path=netrc_path)
    assert creds is None


def test_write_netrc_entry_recovers_from_malformed_file(tmp_path: Path):
    netrc_path = tmp_path / ".netrc"
    netrc_path.write_text(
        r"machine urs.earthdata.nasa.gov\n  login bad\n  password bad\n",
        encoding="utf-8",
    )

    write_netrc_entry("fixed_user", "fixed_pass", netrc_path=netrc_path)
    creds = load_credentials_from_netrc(netrc_path=netrc_path)
    assert creds == ("fixed_user", "fixed_pass")


def test_validate_credentials_uses_direct_session(monkeypatch):
    captured = {"trust_env": None}

    class _FakeResponse:
        status_code = 200
        url = "https://urs.earthdata.nasa.gov/profile"

    class _FakeSession:
        def __init__(self):
            self.trust_env = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, *args, **kwargs):
            captured["trust_env"] = self.trust_env
            return _FakeResponse()

    monkeypatch.setattr("s1downloader.auth.requests.Session", _FakeSession)
    ok = validate_credentials("u", "p")
    assert ok is True
    assert captured["trust_env"] is False
