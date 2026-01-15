# youtube-daily-tracker

Daily YouTube channel statistics collector.

## 📌 Project overview
This project automatically collects daily statistics from selected YouTube channels
(subscribers, views, videos, etc.) and stores them as CSV snapshots for analysis.

The goal is to build a growing historical dataset that can be used for:
- trend analysis
- channel growth comparison
- data analysis / portfolio projects

## ⚙️ How it works
- A list of YouTube channel IDs is maintained in the project
- A Python script collects statistics using the YouTube Data API
- A GitHub Actions workflow runs **daily** (scheduled in UTC)
- Daily snapshots are saved as CSV files and committed automatically

## 📂 Outputs
- `youtube_daily_snapshots.csv` → daily channel statistics
- `channels_reference.csv` → reference list of tracked channels
- `run_log.txt` → execution logs

## ▶️ Run locally
```bash
pip install -r requirements.txt
python collect_youtube.py
