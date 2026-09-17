"""Tests for Settings and RedshiftSettings."""

from __future__ import annotations

import base64

import pytest
from pydantic import ValidationError

from jira_ingest.config import Settings


def make_settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "url": "https://jira.example.com",
        "api_token": "secret",
        "mode": "cloud",
        "email": "user@example.com",
    }
    return Settings.model_validate({**defaults, **overrides})


class TestModeValidation:
    def test_mode_is_required(self) -> None:
        """An unconfigured mode must not silently resolve to one deployment
        type or the other -- e.g. a DC user who forgets JIRA_MODE but still
        has JIRA_EMAIL set (left over from a Cloud .env) would otherwise
        pass validation and send Basic auth to a server expecting Bearer,
        producing a confusing 401 that never points at MODE as the cause."""
        with pytest.raises(ValidationError, match="mode"):
            Settings.model_validate(
                {
                    "url": "https://jira.example.com",
                    "api_token": "secret",
                }
            )

    def test_cloud_requires_email(self) -> None:
        with pytest.raises(ValidationError, match="JIRA_EMAIL"):
            Settings.model_validate(
                {
                    "url": "https://jira.example.com",
                    "api_token": "secret",
                    "mode": "cloud",
                }
            )

    def test_dc_without_email_is_valid(self) -> None:
        s = Settings.model_validate(
            {
                "url": "https://jira.example.com",
                "api_token": "secret",
                "mode": "dc",
            }
        )
        assert s.mode == "dc"
        assert s.email is None

    def test_url_trailing_slash_stripped(self) -> None:
        s = make_settings(url="https://jira.example.com/")
        assert s.url == "https://jira.example.com"


class TestEffectiveBaseUrl:
    """cloud_id (JIRA_CLOUD_ID) routes Cloud requests through Atlassian's API
    gateway instead of the direct tenant domain -- required for
    fine-grained/scoped API tokens, which 401 on the direct domain
    regardless of permissions. See docs/authentication.md."""

    def test_defaults_to_direct_domain(self) -> None:
        s = make_settings()
        assert s.effective_base_url() == "https://jira.example.com"

    def test_cloud_id_switches_to_gateway(self) -> None:
        s = make_settings(cloud_id="d14306f1-5802-4283-834c-8a799a89321a")
        assert (
            s.effective_base_url()
            == "https://api.atlassian.com/ex/jira/d14306f1-5802-4283-834c-8a799a89321a"
        )

    def test_cloud_id_has_no_effect_in_dc_mode(self) -> None:
        s = Settings.model_validate(
            {
                "url": "https://jira.internal.com",
                "api_token": "dctoken",
                "mode": "dc",
                "cloud_id": "d14306f1-5802-4283-834c-8a799a89321a",
            }
        )
        assert s.effective_base_url() == "https://jira.internal.com"


class TestPartFileMaxRecords:
    """utils.batched(buffer, n) silently returns zero chunks for a negative
    n (an empty range()), which BatchWriter._flush() would then treat as
    "nothing to write" while still clearing the buffer -- a negative
    JIRA_PART_FILE_MAX_RECORDS would discard every record with no file
    written and no error at all. n=0 does raise, but only as an unhelpful
    ValueError deep inside range(), not a clear startup-time error. Both
    must be rejected at config-validation time instead."""

    def test_defaults_to_ten_thousand(self) -> None:
        s = make_settings()
        assert s.part_file_max_records == 10_000

    def test_negative_value_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="part_file_max_records"):
            make_settings(part_file_max_records=-1)

    def test_zero_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="part_file_max_records"):
            make_settings(part_file_max_records=0)

    def test_positive_value_is_accepted(self) -> None:
        s = make_settings(part_file_max_records=500)
        assert s.part_file_max_records == 500


class TestProjectKeys:
    def test_comma_separated_string(self) -> None:
        s = make_settings(project_keys="PROJ,INFRA, PLATFORM")
        assert s.project_keys == ["PROJ", "INFRA", "PLATFORM"]

    def test_empty_string_gives_empty_list(self) -> None:
        s = make_settings(project_keys="")
        assert s.project_keys == []


class TestDataTypes:
    def test_default_is_all_types(self) -> None:
        s = make_settings()
        assert set(s.data_types) == {"projects", "releases", "boards", "issues", "transitions"}

    def test_comma_separated_string(self) -> None:
        s = make_settings(data_types="projects, issues")
        assert s.data_types == ["projects", "issues"]

    def test_empty_string_gives_empty_list(self) -> None:
        s = make_settings(data_types="")
        assert s.data_types == []

    def test_unknown_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Unknown JIRA_DATA_TYPES"):
            make_settings(data_types="projects,not_a_type")


class TestCustomFields:
    def test_parses_json_string(self) -> None:
        s = make_settings(custom_fields='{"type_of_work": "customfield_10100"}')
        assert s.custom_fields == {"type_of_work": "customfield_10100"}

    def test_empty_default(self) -> None:
        s = make_settings()
        assert s.custom_fields == {}


class TestSinkOptions:
    def test_parses_json_string(self) -> None:
        s = make_settings(sink_options='{"account_name": "mystorageaccount"}')
        assert s.sink_options == {"account_name": "mystorageaccount"}

    def test_empty_default(self) -> None:
        s = make_settings()
        assert s.sink_options == {}


class TestPemResolution:
    def test_no_pem_returns_none(self) -> None:
        s = make_settings()
        assert s.resolve_pem_path() is None

    def test_valid_pem_writes_temp_file(self, tmp_path: object) -> None:
        fake_pem = b"-----BEGIN CERTIFICATE-----\nMIIBIjANBg==\n-----END CERTIFICATE-----\n"
        encoded = base64.b64encode(fake_pem).decode()
        s = Settings.model_validate(
            {
                "url": "https://jira.example.com",
                "api_token": "secret",
                "mode": "dc",
                "cert_pem": encoded,
            }
        )
        path = s.resolve_pem_path()
        assert path is not None
        import os

        assert os.path.exists(path)
        with open(path, "rb") as f:
            assert f.read() == fake_pem

    def test_invalid_base64_raises(self) -> None:
        s = Settings.model_validate(
            {
                "url": "https://jira.example.com",
                "api_token": "secret",
                "mode": "dc",
                "cert_pem": "not-valid-base64!!!",
            }
        )
        with pytest.raises(ValueError, match="JIRA_CERT_PEM"):
            s.resolve_pem_path()
