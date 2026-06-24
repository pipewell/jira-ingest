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
    def test_cloud_requires_email(self) -> None:
        with pytest.raises(ValidationError, match="JIRA_EMAIL"):
            Settings.model_validate({
                "url": "https://jira.example.com",
                "api_token": "secret",
                "mode": "cloud",
            })

    def test_dc_without_email_is_valid(self) -> None:
        s = Settings.model_validate({
            "url": "https://jira.example.com",
            "api_token": "secret",
            "mode": "dc",
        })
        assert s.mode == "dc"
        assert s.email is None

    def test_url_trailing_slash_stripped(self) -> None:
        s = make_settings(url="https://jira.example.com/")
        assert s.url == "https://jira.example.com"


class TestProjectKeys:
    def test_comma_separated_string(self) -> None:
        s = make_settings(project_keys="PROJ,INFRA, PLATFORM")
        assert s.project_keys == ["PROJ", "INFRA", "PLATFORM"]

    def test_empty_string_gives_empty_list(self) -> None:
        s = make_settings(project_keys="")
        assert s.project_keys == []


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
        s = Settings.model_validate({
            "url": "https://jira.example.com",
            "api_token": "secret",
            "mode": "dc",
            "cert_pem": encoded,
        })
        path = s.resolve_pem_path()
        assert path is not None
        import os
        assert os.path.exists(path)
        with open(path, "rb") as f:
            assert f.read() == fake_pem

    def test_invalid_base64_raises(self) -> None:
        s = Settings.model_validate({
            "url": "https://jira.example.com",
            "api_token": "secret",
            "mode": "dc",
            "cert_pem": "not-valid-base64!!!",
        })
        with pytest.raises(ValueError, match="JIRA_CERT_PEM"):
            s.resolve_pem_path()
