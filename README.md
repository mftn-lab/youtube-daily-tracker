# youtube-daily-tracker

Automated YouTube data collection project focused on **applied tech content**  
(daily channel statistics + monthly video-level snapshots).

---

## 📌 Project overview

This project automatically collects and maintains structured datasets from a curated list of
**tech-focused YouTube channels** (hardware, software, desk setups, productivity tools, applied AI,
networking, peripherals).

The goal is to build a **clean, reliable and growing historical dataset** based on real-world tech
usage — not hype or generic trends.

This dataset is designed for:
- time-series analysis of channel growth
- comparison of audience dynamics across tech creators
- video-level performance analysis (monthly snapshots)
- data analysis projects and portfolio demonstrations
- future client-oriented YouTube analytics use cases

---

## 🧠 Project philosophy

- Focus on **practical tech usage**, not viral or speculative content
- Prefer **robust, reproducible pipelines** over one-off scripts
- Separate **daily time-series tracking** from **monthly deep snapshots**
- Keep data structures simple, auditable and analysis-ready

---

## ⚙️ How it works

### Daily pipeline
- A curated list of **YouTube Channel IDs** is maintained in the project
- A Python script queries the YouTube Data API (`channels.list`)
- A GitHub Actions workflow runs **once per day (UTC)**
- One snapshot per channel per day is appended (anti-duplicate logic)

### Monthly pipeline
- A separate Python script runs **once per month**
- For each channel:
  - recent uploads are collected
  - top-performing videos from the last 12 months are selected
- Video-level statistics are stored as a **monthly snapshot**
- The script is idempotent and can be safely re-run

---

## 📂 Data outputs

### Daily datasets
- `youtube_daily_snapshots.csv`  
  Daily channel-level statistics (time-series):
  - subscribers
  - total views
  - video count

- `channels_reference.csv`  
  Reference table:
  - channel ID
  - channel name
  - channel URL
  - last seen timestamp

### Monthly datasets
- `data/monthly/videos_YYYY-MM.csv`  
  Monthly video-level snapshot including:
  - video metadata (title, publish date, duration)
  - engagement metrics (views, likes, comments)
  - channel association

- `data/monthly/errors_YYYY-MM.csv`  
  Monthly error log (empty when runs are successful)

---

## 🤖 Automation

- GitHub Actions handles:
  - daily scheduled runs
  - monthly scheduled runs
  - manual execution (`workflow_dispatch`)
- Commits are performed **only if data has changed**
- Python version and dependencies are pinned for stability

---

## ▶️ Run locally

### Daily collection
```bash
pip install -r requirements.txt
python collect_youtube.py
```

### Monthly snapshot
```bash
pip install -r requirements.txt
python monthly_videos_snapshot.py
```

YouTube API credentials must be provided via the environment variable:
```bash
YOUTUBE_API_KEY=your_api_key_here
```

---

## 📊 Typical use cases

- Long-term growth analysis of tech YouTube channels
- Monthly comparison of video performance across creators
- Dataset for Python / Pandas / Power BI analysis
- Foundation for future YouTube analytics tooling

---

## 🚧 Notes

- This project is intentionally **data-first** (no UI layer)
- CSV outputs are designed to be easily ingested into analysis tools
- Structure may evolve as new analytical needs emerge
