import pytest
from automl_api.core.run_names import versioned_run_name


@pytest.mark.parametrize(
    ("name", "legacy", "expected"),
    [
        ("Test Project-v1", False, "Test Project-v2"),
        ("Test Project-v1 restart", True, "Test Project-v3"),
        ("Test Project-v1 restart restart", True, "Test Project-v4"),
        ("Test Project", False, "Test Project-v2"),
        ("Test Project V9", False, "Test Project-v10"),
        ("Business restart", False, "Business restart-v2"),
    ],
)
def test_restart_names_increment_versions(name, legacy, expected):
    assert versioned_run_name(name, legacy_restart=legacy, increment=True) == expected


def test_legacy_display_and_database_length():
    assert versioned_run_name("Test-v1 restart restart", legacy_restart=True) == "Test-v3"
    assert len(versioned_run_name("x" * 255, increment=True)) == 255
