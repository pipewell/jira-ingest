from __future__ import annotations

import atexit
import base64
import json
import os
import tempfile
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Data types the processor can emit. Keep in sync with the dtype strings
# yielded by jira_ingest.processor.stream_all.
VALID_DATA_TYPES = frozenset({"projects", "releases", "boards", "issues", "transitions"})


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="JIRA_",
        extra="ignore",
    )

    # ── Mode ──────────────────────────────────────────────────────────────────
    # No default: an unconfigured mode must not silently resolve to one deployment
    # type or the other. A DC user who forgets JIRA_MODE but happens to still have
    # JIRA_EMAIL set (e.g. left over from a Cloud .env) would otherwise pass
    # validation and send Basic auth to a server expecting Bearer, producing a
    # confusing 401 that gives no hint MODE was the actual problem.
    mode: Literal["dc", "cloud"]

    # ── Connection ─────────────────────────────────────────────────────────────
    url: str
    api_token: str
    email: str | None = None  # Cloud only
    cert_pem: str | None = None  # DC only: base64-encoded PEM

    # Cloud only. Fine-grained/scoped Atlassian API tokens are rejected with a
    # 401 against the direct tenant domain and must be routed through
    # Atlassian's API gateway by cloud ID instead. Leave unset for classic,
    # unrestricted tokens (the default). See docs/authentication.md.
    cloud_id: str | None = None

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

    # Row-count threshold per data type before flushing a new part file.
    # `stream_all` yields some data types (projects, boards) one record at
    # a time, so flushing on every yield would produce mostly one-row part
    # files; buffering up to this many records first keeps parts a
    # reasonable size. See jira_ingest.output.writer.BatchWriter.
    #
    # Must be positive: BatchWriter._flush() chunks via utils.batched(buffer,
    # n), and batched() silently returns zero chunks for a negative n (an
    # empty range()), which would clear the buffer and produce no output
    # file at all -- not even an error. n=0 does raise, but only as an
    # unhelpful ValueError deep in range(), not a clear startup-time error.
    part_file_max_records: int = Field(default=10_000, gt=0)

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

    @field_validator("data_types", mode="before")
    @classmethod
    def parse_data_types(cls, v: Any) -> list[str]:
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        return list(v) if v else []

    @field_validator("data_types")
    @classmethod
    def validate_data_types(cls, v: list[str]) -> list[str]:
        unknown = sorted(set(v) - VALID_DATA_TYPES)
        if unknown:
            raise ValueError(
                f"Unknown JIRA_DATA_TYPES entries: {unknown}. "
                f"Valid values: {sorted(VALID_DATA_TYPES)}"
            )
        return v

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

    # ── Effective base URL ────────────────────────────────────────────────────
    def effective_base_url(self) -> str:
        """The base URL requests actually go to.

        Same as ``url`` unless ``cloud_id`` is set (Cloud only), in which
        case requests route through Atlassian's API gateway instead of the
        direct tenant domain -- required for fine-grained/scoped API tokens.
        """
        if self.mode == "cloud" and self.cloud_id:
            return f"https://api.atlassian.com/ex/jira/{self.cloud_id}"
        return self.url

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
