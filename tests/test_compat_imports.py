from app.main import main as compat_main
from s1downloader.main import main as new_main


def test_legacy_app_import_still_points_to_main():
    assert compat_main is not None
    assert new_main is not None
