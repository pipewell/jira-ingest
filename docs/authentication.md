# Authentication

jira-ingest supports two Jira deployment types. Set `JIRA_MODE` to select one.

---

## Jira Cloud

Cloud uses HTTP Basic Auth: your account email plus an API token.

**Required variables:**

| Variable | Description |
|---|---|
| `JIRA_MODE` | `cloud` (default) |
| `JIRA_URL` | Your Cloud base URL, e.g. `https://myorg.atlassian.net` |
| `JIRA_API_TOKEN` | API token from [id.atlassian.com/manage-profile/security/api-tokens](https://id.atlassian.com/manage-profile/security/api-tokens) |
| `JIRA_EMAIL` | The email address tied to that token |

**.env example:**

```dotenv
JIRA_MODE=cloud
JIRA_URL=https://myorg.atlassian.net
JIRA_API_TOKEN=ATATT3xFfGF0...
JIRA_EMAIL=you@example.com
```

Verify connectivity:

```bash
jira-ingest validate
```

---

## Jira Data Center (Server)

Data Center uses a Personal Access Token (PAT) sent as a Bearer header. mTLS is optional and only needed when your DC instance sits behind a mutual-TLS gateway.

**Required variables:**

| Variable | Description |
|---|---|
| `JIRA_MODE` | `dc` |
| `JIRA_URL` | Your DC base URL, e.g. `https://jira.internal.example.com` |
| `JIRA_API_TOKEN` | PAT generated in Jira under Profile > Personal Access Tokens |

**.env example (no mTLS):**

```dotenv
JIRA_MODE=dc
JIRA_URL=https://jira.internal.example.com
JIRA_API_TOKEN=NjI2...
```

---

### mTLS (optional, DC only)

If your DC instance requires a client certificate, supply it via `JIRA_CERT_PEM`. The value must be a **base64-encoded PEM** containing the certificate and private key concatenated:

```bash
# Combine cert + key, then encode
cat client.crt client.key | base64 | tr -d '\n'
```

Paste the output into `.env`:

```dotenv
JIRA_MODE=dc
JIRA_URL=https://jira.internal.example.com
JIRA_API_TOKEN=NjI2...
JIRA_CERT_PEM=LS0tLS1CRUdJTi...
```

The pipeline decodes the value at startup, writes it to a temporary file, and registers an `atexit` handler to delete it -- the PEM never persists on disk after the process exits.

---

## Scoping to specific projects

By default jira-ingest fetches all projects your token has access to. Restrict to a subset with a comma-separated list:

```dotenv
JIRA_PROJECT_KEYS=PROJ,OPS,PLATFORM
```

Or pass it at runtime:

```bash
jira-ingest run --start-date 2024-01-01 --end-date 2024-03-31
```

(Date filters apply to issues only; projects, boards, and releases are always fetched in full.)

---

## Tuning

| Variable | Default | Description |
|---|---|---|
| `JIRA_MAX_CONCURRENT_REQUESTS` | `10` | Parallel in-flight requests |
| `JIRA_REQUEST_TIMEOUT_SECONDS` | `120` | Per-request timeout |
| `JIRA_CACHE_TTL_SECONDS` | `300` | In-memory response cache lifetime |
| `JIRA_MAX_RETRY_ATTEMPTS` | `20` | Exponential-backoff retry limit |
| `JIRA_LOG_LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR` |
