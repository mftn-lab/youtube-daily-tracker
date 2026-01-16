# youtube-daily-tracker

Daily YouTube channel statistics collector focused on **applied tech content**.

## 📌 Project overview
This project automatically collects daily statistics from selected **tech-focused YouTube channels**
(hardware, software, desk setups, productivity tools, applied AI, networking, peripherals).

The goal is to build a **clean and growing historical dataset** based on real-world tech usage,
which can later be used for:
- trend analysis in applied tech content
- channel growth and audience behavior comparison
- data analysis projects and portfolio use
- future client-oriented YouTube channel analysis

This project is intentionally focused on **practical tech usage**, not hype or generic trends.

## ⚙️ How it works
- A curated list of **tech YouTube channel IDs** is maintained in the project
- A Python script collects public statistics using the YouTube Data API
- A GitHub Actions workflow runs **daily** (scheduled in UTC)
- Daily snapshots are stored as CSV files to build time-series data

## 📂 Outputs
- `youtube_daily_snapshots.csv` → daily statistics per channel (time-series dataset)
- `channels_reference.csv` → reference table (channel name, URL, last seen)
- `run_log.txt` → execution logs for monitoring and debugging

## ▶️ Run locally
```bash
pip install -r requirements.txt
python collect_youtube.py
