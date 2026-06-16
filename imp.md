# Refactor Google Jobs Architecture

## Problem

The current implementation incorrectly treats Google Jobs as the final authoritative source.

Current incorrect flow:

```text
Google Jobs → Final Job Data
```

Correct architecture:

```text
Google Jobs = Discovery Layer
Provider Page = Truth Source
```

Google Jobs data is intentionally incomplete.

It should ONLY be used for:

- job discovery
- extracting provider URLs
- lightweight metadata

The provider page must be used for structured enrichment.

---

# Required Architecture

```text
Google Jobs Search
        ↓
Extract Provider URL
        ↓
Detect Provider
        ↓
Fetch Provider Page
        ↓
Extract Structured Fields
        ↓
Merge Final Result
```

---

# Important Data Ownership

| Field | Google Jobs | Provider Page |
|---|---|---|
| title | partial | authoritative |
| company | partial | authoritative |
| location | partial | authoritative |
| employment_type | missing often | authoritative |
| salary | missing often | authoritative |
| description | truncated | authoritative |
| qualifications | missing | authoritative |

---

# Keep Existing Google Scraping

DO NOT remove Google Jobs scraping.

Keep using it for:

```python
title
company
provider_url
location
posted_date
```

---

# Main Fix Required

Current implementation uses:

```python
requests.get(url)
```

inside:

```python
_enrich_job_fields_from_url()
```

This is the root problem.

Provider sites return:

- blocked HTML
- bot pages
- consent pages
- incomplete lightweight responses

especially:

- Indeed
- LinkedIn
- Japanese job boards

---

# REQUIRED CHANGE

Replace direct requests with Oxylabs Universal Scraper.

## REMOVE

```python
response = requests.get(
    url,
    timeout=20,
    headers={...}
)
```

## REPLACE WITH

```python
payload = {
    "source": "universal",
    "url": url,
    "render": "html",
    "parse": False,
    "user_agent_type": "desktop",
}

response = requests.post(
    self._settings.OXYLABS_URL,
    json=payload,
    auth=(
        self._settings.OXYLABS_USERNAME,
        self._settings.OXYLABS_PASSWORD,
    ),
    timeout=120,
)

response.raise_for_status()

data = response.json()

results = data.get("results") or []

if not results:
    return empty

html = results[0].get("content") or ""

soup = BeautifulSoup(html, "html.parser")
```

---

# Why This Works

Google Jobs gives provider URLs only.

Oxylabs fetches the REAL rendered provider page.

Then the parser can correctly extract:

- employmentType
- salary
- jobDescription
- qualifications
- jobDetails

instead of blocked HTML.

---

# Provider Detection Layer

Implement provider-specific enrichment:

```python
if "indeed" in url:
    enrich_indeed()

elif "linkedin" in url:
    enrich_linkedin()

elif "greenhouse" in url:
    enrich_greenhouse()

elif "lever" in url:
    enrich_lever()

elif "rikunabi" in url:
    enrich_rikunabi()
```

---

# Important Parser Improvement

Current Google parser is fragile because Google changes CSS classes often.

DO NOT rely only on:

```text
QJPWVe
RP7SMd
```

Add fallbacks:

- aria-label
- data-* attributes
- href extraction
- semantic HTML
- JSON-LD structured data

---

# Final Expected Architecture

```text
Google Jobs Search
        ↓
Extract provider URLs
        ↓
Provider enrichment
        ↓
Normalize fields
        ↓
Final structured job object
```

---

# MOST IMPORTANT

DO NOT rewrite the entire system.

The architecture is already mostly correct.

Only change:

```text
❌ requests.get()
```

to:

```text
✅ Oxylabs universal scraping
```

---

# Expected Fixes

This change should fix:

- missing job type
- missing salary
- empty descriptions
- blocked Indeed pages
- incomplete metadata
- Japanese encoding issues
- partial Google snippets
- broken provider parsing