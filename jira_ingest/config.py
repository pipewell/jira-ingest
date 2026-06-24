from __future__ import annotations

import atexit
import base64
import json
import os
import tempfile
from typing import Any, Literal

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="JIRA_",
        extra="ignore",
    )

    # ── Mode ──────────────────────────────────────────────────────────────────
    mode: Literal["dc", "cloud"] = "cloud"

    # ── Connection ─────────────────────────────────────────────────────────────
    url: str
    api_token: str
    email: str | None = None  # Cloud only
    cert_pem: str | None = None  # DC only: base64-encoded PEM

    # ── Scope ──────────────────────────────────────────────────────────────────
    project_keys: list[str] = []
    data_types: list[str] = ["projects", "releases", "boards", "issues", "transitions"]

    # Custom fields: logical_name -> Jira field ID
    # e.g. {"type_of_work": "customfield_10100", "team": "customfield_10001"}
    custom_fields: dict[str, str] = {}

    # ── Output ─────────────────────────────────────────────────────────────────
    output_format: Literal["csv", "parquet", "jsonl"] = "parquet"

    # fsspec URI: "./output", "s3://bucket/prefix", "az://container/prefix"
    sink_uri: str = "./output"

    # Storage options forwarded to fsspec (auth for S3, Azure, GCS, etc.)
    # Stored as a JSON string in env; parsed to dict at validation time.
    sink_options: dict[str, Any] = {}

    # ── Redshift ───────────────────────────────────────────────────────────────
    # These live outside the JIRA_ prefix so they share a common convention
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Tuning ─────────────────────────────────────────────────────────────────
    max_concurrent_requests: int = 10
    request_timeout_seconds: int = 120
    cache_ttl_seconds: int = 300
    max_retry_attempts: int = 20

    # ── Runtime ────────────────────────────────────────────────────────────────
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    # ── Validators ─────────────────────────────────────────────────────────────
    @field_validator("url")
    @classmethod
    def strip_trailing_slash(cls, v: str) -> str:
        return v.rstrip("/")

    @field_validator("project_keys", mode="before")
    @classmethod
    def parse_project_keys(cls, v: Any) -> list[str]:
        if isinstance(v, str):
            return [k.strip() for k in v.split(",") if k.strip()]
        return list(v) if v else []

    @field_validator("custom_fields", "sink_options", mode="before")
    @classmethod
    def parse_json_dict(cls, v: Any) -> dict[str, Any]:
        if isinstance(v, str):
            parsed: dict[str, Any] = json.loads(v)
            return parsed
        return dict(v) if v else {}

    @model_validator(mode="after")
    def validate_mode_credentials(self) -> Settings:
        if self.mode == "cloud" and not self.email:
            raise ValueError("JIRA_EMAIL is required when JIRA_MODE=cloud")
        return self

    # ── PEM resolution ─────────────────────────────────────────────────────────
    def resolve_pem_path(self) -> str | None:
        """Decode JIRA_CERT_PEM, write to a temp file, register cleanup, return path."""
        if not self.cert_pem:
            return None
        try:
            pem_content = base64.b64decode(self.cert_pem).decode("utf-8")
        except Exception as exc:
            raise ValueError(f"Invalid JIRA_CERT_PEM: {exc}") from exc

        fd, path = tempfile.mkstemp(suffix=".pem")
        try:
            os.write(fd, pem_content.encode())
        finally:
            os.close(fd)

        atexit.register(os.unlink, path)
        return path


class RedshiftSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="REDSHIFT_",
        extra="ignore",
    )

    host: str | None = None
    port: int = 5439
    database: str | None = None
    user: str | None = None
    password: str | None = None
    schema_name: str = "bronze"
    iam_role: str | None = None  # ARN for COPY command

    def is_configured(self) -> bool:
        return all([self.host, self.database, self.user, self.password])
