import pytest

from lakehouse.config import ConfigurationError, load_settings


def test_load_settings_uses_documented_defaults() -> None:
    settings = load_settings({})

    assert settings.source_months == ("2024-01", "2024-02")
    assert settings.log_level == "INFO"


def test_load_settings_parses_months_and_normalizes_log_level() -> None:
    settings = load_settings(
        {"LAKEHOUSE_SOURCE_MONTHS": "2023-12, 2024-01", "LAKEHOUSE_LOG_LEVEL": "debug"}
    )

    assert settings.source_months == ("2023-12", "2024-01")
    assert settings.log_level == "DEBUG"


@pytest.mark.parametrize(
    "value",
    ["", "2024-00", "2024-13", "24-01", "2024-1", "2024-01,invalid", "2024-01,"],
)
def test_load_settings_rejects_invalid_months(value: str) -> None:
    with pytest.raises(ConfigurationError, match="LAKEHOUSE_SOURCE_MONTHS"):
        load_settings({"LAKEHOUSE_SOURCE_MONTHS": value})


def test_load_settings_rejects_duplicate_months() -> None:
    with pytest.raises(ConfigurationError, match="duplicate"):
        load_settings({"LAKEHOUSE_SOURCE_MONTHS": "2024-01,2024-01"})


def test_load_settings_rejects_unknown_log_level() -> None:
    with pytest.raises(ConfigurationError, match="LAKEHOUSE_LOG_LEVEL"):
        load_settings({"LAKEHOUSE_LOG_LEVEL": "TRACE"})
