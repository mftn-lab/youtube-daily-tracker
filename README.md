# youtube-daily-tracker

Automated YouTube data collection project focused on **applied tech content**  
(daily channel time-series + monthly video-level performance snapshots).

---

## 📌 Project overview

This project builds and maintains a **clean, reproducible and growing dataset**
from a curated list of **tech-focused YouTube channels**  
(hardware, software, productivity tools, desk setups, applied AI, peripherals).

The objective is **data quality and longitudinal analysis**, not short-term trends
or viral noise.

This dataset is designed for:
- time-series analysis of channel growth
- comparison of audience dynamics across tech creators
- video-level performance analysis
- data analysis / data engineering portfolio projects
- future client-oriented YouTube analytics use cases

---

## 🧠 Project philosophy

- Focus on **practical tech usage**, not hype or speculative content
- Prefer **robust, reproducible pipelines** over one-off scripts
- Clearly separate **daily time-series tracking** from **monthly deep snapshots**
- Keep data structures **simple, auditable and analysis-ready**
- Treat data files as **products**, not temporary artifacts

---

## ⚙️ High-level architecture

```text
channels_reference.xlsx   → manual edit (Excel)
           ↓ export
channels_reference.csv    → source of truth (tracked locally, not committed)
           ↓
collect_youtube.py        → daily channel-level snapshots
monthly_videos_snapshot.py→ monthly video-level snapshots
```

This architecture deliberately separates:
- **human-friendly input** (Excel)
- **versioned source of truth** (CSV)
- **automated data pipelines** (Python)

Each layer has a **single responsibility**, which keeps the system robust and easy to evolve.

---

## 📘 Channels reference (source of truth)

### channels_reference.xlsx (local, not versioned)

- Used for **manual editing**:
  - adding new channels (only `channel_id` is required)
  - adding personal annotations or notes
- Human-friendly format (Excel)
- This file is **not tracked by Git**

Typical workflow:
- add or remove channel IDs
- add optional notes
- export to CSV

---

### channels_reference.csv (versioned)

- **Single source of truth**
- Tracked in Git
- Read by both daily and monthly pipelines
- Automatically enriched and updated by scripts

Only the `channel_id` column is mandatory.  
All other fields are filled or updated automatically during pipeline runs.

Automatically managed fields include:
- channel_title
- channel_url
- custom_url
- country
- uploads_playlist_id
- channel_published_at
- last_seen_utc

---

## ⚙️ How it works

### 🟢 Daily pipeline (channel-level time series)

- Reads `channels_reference.csv`
- Validates channel IDs (format and duplicates)
- Queries the YouTube Data API (`channels.list`)
- Runs **once per day (UTC)** via GitHub Actions
- Appends **one row per channel per day**
- Safe to re-run (anti-duplicate logic)

Built-in features:
- retry logic and API error handling
- structured daily error logs
- safe upsert of channel metadata

**Outputs**
- `youtube_daily_snapshots.csv`
- `data/daily/errors_daily.csv`

---

### 🔵 Monthly pipeline (video-level snapshot)

- Runs **once per month**
- For each channel:
  - collects the **20 most recent uploaded videos**
  - selects the **20 most viewed videos** from the **last 12 months**
  - deduplicates overlapping videos
- Results in ~30–40 videos per channel
- Uses **atomic writes** to prevent partial files

**Outputs**
- `data/monthly/videos_YYYY-MM.csv`
- `data/monthly/errors_YYYY-MM.csv`

---

## 📂 Data outputs

### Daily datasets

- `youtube_daily_snapshots.csv`
  - date (UTC)
  - channel_id
  - channel_title
  - subscribers
  - total views
  - video count

- `channels_reference.csv`
  - channel metadata (ID, title, URL, country, uploads playlist, last seen, etc.)

---

### Monthly datasets

- `data/monthly/videos_YYYY-MM.csv`
  - snapshot month and timestamp
  - channel ID and title
  - video ID
  - publish date
  - title
  - duration
  - category ID
  - view count
  - like count
  - comment count

- `data/monthly/errors_YYYY-MM.csv`
  - structured error log
  - API errors
  - missing data
  - invalid or unavailable channels

---

## 🤖 Automation

- GitHub Actions handle:
  - daily scheduled runs
  - monthly scheduled runs
  - manual execution (`workflow_dispatch`)
- Commits occur **only if data has changed**
- Python version and dependencies are pinned
- Pipelines are **idempotent and safe to re-run**

---

## ▶️ Run locally

Daily collection:

```bash
pip install -r requirements.txt
python collect_youtube.py
```

Monthly snapshot:

```bash
pip install -r requirements.txt
python monthly_videos_snapshot.py
```

Environment variable required:
YOUTUBE_API_KEY (YouTube Data API v3 key)


---

## 🚧 Notes

- This project is intentionally **data-first**
- No UI layer by design
- CSV outputs are analysis-ready (Pandas, Power BI, SQL, etc.)
- The structure may evolve as analytical needs grow
- API keys and secrets are **never committed** to the repository

---

## ✅ How to add a new channel safely

This project enforces **server-side validation** of all YouTube channel IDs to
guarantee data integrity.

Recommended workflow when adding a new channel:

1. Open the YouTube channel page (or one of its videos)
2. Use the provided bookmarklet to:
   - copy the **Channel ID (UC...)**
   - visually confirm the **official channel name**
3. Paste the `channel_id` into `channels_reference.xlsx`
4. Export to `channels_reference.csv`
5. Run the daily collector:
```bash
   python collect_youtube.py
```

During the run:
- Channel IDs are validated against the **YouTube Data API**
- Invalid formats are rejected
- Non-existing channels are flagged as `missing`
- Valid channels are cached as `ok` to avoid repeated checks

Validation cache:
- `data/daily/channels_validation_cache.csv`
- Used only as a runtime optimization (not committed)

This ensures that **only real, existing YouTube channels** enter the dataset.


---

## 🧩 Data model — Channel classification

This project relies on a **channel-level classification model** used to enrich
and segment YouTube channels for analysis purposes.

The classification is designed to be:
- stable over time
- analytically meaningful
- orthogonal across dimensions
- simple to maintain manually

> **Note**  
> The channel classification table is maintained **locally in Excel** for analysis purposes  
> and is **not included nor versioned in this repository**.  
> This section documents the **data model and classification logic**, not the data itself.

---

## 🎯 Design principles

The data model follows a few strict principles:

- Each axis represents **one independent analytical dimension**
- Axes are **orthogonal** (no overlap in meaning)
- Classification is done at the **channel level**, not per video
- Tags reflect **dominant patterns**, not edge cases
- The model favors **long-term comparability** over exhaustiveness

---

## 🧱 Model overview

Each YouTube channel is described using the following axes:

| Axis | Dimension | Column name | Required |
|----|----------|-------------|----------|
| Axis 1 | Incarnation | `incarnation` | Yes |
| Axis 2 | Main topic | `topic_main` | Yes |
| Axis 2 | Secondary topic | `topic_secondary` | No |
| Axis 3 | Content angle | `content_angle` | Yes |
| Axis 4 | Aesthetic / branding | `aesthetic` | No |
| Axis 5 | Positioning | `positioning` | Yes |

---

## 🧍 Axis 1 — Incarnation

**Purpose**  
Describes the **perceived human presence on screen**.

**Allowed values**
```
incarnated
voice_only
faceless
ai_generated
b_roll
```
---

## 🧠 Axis 2 — Topic

**Purpose**  
Describes **what the channel is mainly about**, from an analytical perspective.

**Rules**
- One main topic is mandatory
- One secondary topic is optional
- Maximum two topics per channel

**Allowed values**
```
hardware
software
ai
consumer_electronics
apple
tech_news
productivity
tech_lifestyle
creative_tools
```
---

## 🎥 Axis 3 — Content angle

**Purpose**  
Describes **how the content is primarily approached**.

**Allowed values**
```
reviews
comparisons
tutorials
explainers
opinions
use_cases
benchmarks
setup_desk
```
---

## 🎨 Axis 4 — Aesthetic / branding (optional)

**Purpose**  
Describes the **dominant visual identity** of the channel.

**Allowed values**
```
minimalist
cinematic
clean
dark
lifestyle
tech_focus
```
---

## 🧭 Axis 5 — Positioning

**Purpose**  
Describes the **structural nature of the channel**.

**Allowed values**
```
solo_creator
team_channel
media_brand
company_brand
educational_channel
```
---

## 🔒 Model stability

This data model is intentionally **limited and stable**.

New axes or tags should only be introduced if they provide **clear analytical value**
and do not compromise long-term comparability.

The goal is to keep the model valid across multiple years of data collection and analysis.