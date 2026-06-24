# Custom fields

Jira custom fields have internal IDs like `customfield_10100` that differ between instances. jira-ingest lets you map these to human-readable names so your output has consistent, predictable column names regardless of which Jira instance you are connected to.

## How it works

Set `JIRA_CUSTOM_FIELDS` to a JSON object mapping logical names to Jira field IDs:

```dotenv
JIRA_CUSTOM_FIELDS={"story_points": "customfield_10016", "team": "customfield_10001", "sprint": "customfield_10020"}
```

For each issue, jira-ingest reads the raw value at `fields.customfield_XXXXX` and writes it into the `custom_fields` column under your chosen key:

```json
{
  "story_points": "5",
  "team": "Platform",
  "sprint": "Sprint 12"
}
```

In Parquet and database outputs the `custom_fields` column is a JSON/JSONB object. In CSV it is serialised as a JSON string.

---

## Finding field IDs

### Method 1: Jira REST API

```bash
curl -u you@example.com:YOUR_API_TOKEN \
  https://myorg.atlassian.net/rest/api/2/field \
  | jq '.[] | select(.custom == true) | {name: .name, id: .id}'
```

Output:

```json
{"name": "Story Points", "id": "customfield_10016"}
{"name": "Team",         "id": "customfield_10001"}
{"name": "Sprint",       "id": "customfield_10020"}
```

### Method 2: issue detail endpoint

Fetch any issue that has the field populated and inspect the `fields` object:

```bash
curl -u you@example.com:YOUR_API_TOKEN \
  https://myorg.atlassian.net/rest/api/2/issue/PROJ-1 \
  | jq '.fields | keys | map(select(startswith("customfield")))'
```

### Method 3: Jira UI (Cloud)

1. Go to **Settings > Issues > Custom fields**.
2. Click the field name.
3. The URL contains the field ID: `.../custom_fields/10016/...`

### Method 4: Jira DC admin panel

Go to **Administration > Issues > Custom Fields**, click the field, and read the ID from the URL parameter `fieldId=customfield_10016`.

---

## Configuration

**In `.env`:**

```dotenv
JIRA_CUSTOM_FIELDS={"story_points": "customfield_10016", "team": "customfield_10001"}
```

**As a shell variable (single line, must be valid JSON):**

```bash
export JIRA_CUSTOM_FIELDS='{"story_points": "customfield_10016"}'
jira-ingest run
```

**In a multi-environment setup**, keep a separate `.env` per environment since field IDs can differ between Jira instances:

```
envs/
  prod.env     # JIRA_CUSTOM_FIELDS={"story_points": "customfield_10016", ...}
  staging.env  # JIRA_CUSTOM_FIELDS={"story_points": "customfield_10028", ...}
```

```bash
jira-ingest run --env-file envs/prod.env
jira-ingest run --env-file envs/staging.env
```

---

## Unmapped fields

Fields not listed in `JIRA_CUSTOM_FIELDS` are ignored. If you want to capture a field temporarily without a logical name, you can use the raw ID as the key:

```dotenv
JIRA_CUSTOM_FIELDS={"customfield_10099": "customfield_10099"}
```

---

## Validating your mapping

`jira-ingest validate` prints the current custom fields config:

```
Custom fields: {'story_points': 'customfield_10016', 'team': 'customfield_10001'}
```

To confirm a field is populated on real issues, run with a single project and inspect the output:

```bash
JIRA_PROJECT_KEYS=PROJ jira-ingest run --output-format jsonl
head -5 output/issues/issues_$(date +%Y%m%d).jsonl | jq '.custom_fields'
```
