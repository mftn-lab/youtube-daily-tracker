import os
import csv
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Set, Tuple

import requests
from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta

# ------------------------------------------------------------
# Monthly Videos Snapshot
# - Lit CHANNEL_IDS depuis collect_youtube.py (mon script actuel)
# - Pour chaque chaîne:
#   -  les 20 vidéos les plus récentes (Uploads)
#   - + les 20 vidéos les plus vues parmi celles des 12 derniers mois
#   - déduplique
# - Écrit dans youtube_monthly_videos_snapshots.csv (nouveau fichier)
# ------------------------------------------------------------

# 1) Récupération de la liste de chaînes directement depuis mon script existant
from collect_youtube import CHANNEL_IDS  # <-- ne  pas modifier collect_youtube.py

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
API_KEY = os.getenv("YOUTUBE_API_KEY")

# Paramètres de sélection
RECENT_N = 20
TOP_RECENT_N = 20
TOP_WINDOW_MONTHS = 12
POOL_SIZE = 120  # pool de vidéos récentes pour avoir assez de candidats "top"

OUTPUT_CSV = "youtube_monthly_videos_snapshots.csv"

FIELDS = [
    "snapshot_month",
    "snapshot_utc",
    "channel_id",
    "channel_title",
    "video_id",
    "published_at",
    "title",
    "duration_iso8601",
    "category_id",
    "view_count",
    "like_count",
    "comment_count",
]

def yt_get(endpoint: str, params: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
    if not API_KEY:
        raise RuntimeError("Clé API manquante : exporte YOUTUBE_API_KEY avant de lancer le script.")

    url = f"{YOUTUBE_API_BASE}/{endpoint}"
    params = dict(params)
    params["key"] = API_KEY

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code}: {r.text[:300]}"
            time.sleep(1.5 ** attempt)
        except requests.RequestException as e:
            last_err = str(e)
            time.sleep(1.5 ** attempt)

    raise RuntimeError(f"Erreur API sur {endpoint}: {last_err}")

def chunk(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i+size] for i in range(0, len(items), size)]

def channels_info(channel_ids: List[str]) -> List[Dict[str, Any]]:
    all_items = []
    for batch in chunk(channel_ids, 50):
        data = yt_get("channels", {
            "part": "snippet,contentDetails",
            "id": ",".join(batch),
            "maxResults": 50
        })
        all_items.extend(data.get("items", []))
    return all_items

def get_uploads_playlist_id(channel_item: Dict[str, Any]) -> str:
    return channel_item["contentDetails"]["relatedPlaylists"]["uploads"]

def playlist_items_limit(playlist_id: str, max_items: int) -> List[str]:
    """Retourne une liste de videoIds (max max_items) de la playlist Uploads."""
    video_ids = []
    page_token = None

    while len(video_ids) < max_items:
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": min(50, max_items - len(video_ids)),
        }
        if page_token:
            params["pageToken"] = page_token

        data = yt_get("playlistItems", params)
        for it in data.get("items", []):
            vid = it.get("contentDetails", {}).get("videoId")
            if vid:
                video_ids.append(vid)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return video_ids[:max_items]

def videos_info(video_ids: List[str]) -> List[Dict[str, Any]]:
    all_items = []
    for batch in chunk(video_ids, 50):
        data = yt_get("videos", {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(batch),
            "maxResults": 50
        })
        all_items.extend(data.get("items", []))
    return all_items

def append_rows(rows: List[Dict[str, Any]]):
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            w.writeheader()
        w.writerows(rows)

def main():
    snapshot_utc = datetime.now(timezone.utc).isoformat()
    snapshot_month = datetime.now(timezone.utc).strftime("%Y-%m")
    cutoff = datetime.now(timezone.utc) - relativedelta(months=TOP_WINDOW_MONTHS)

    # Map channel_id -> (title, uploads_playlist_id)
    channels = channels_info(CHANNEL_IDS)
    meta = {}
    for ch in channels:
        cid = ch.get("id")
        meta[cid] = {
            "title": ch.get("snippet", {}).get("title", ""),
            "uploads": get_uploads_playlist_id(ch)
        }

    total_rows = 0

    for cid in CHANNEL_IDS:
        if cid not in meta:
            print(f"[WARN] ChannelId introuvable via API: {cid}")
            continue

        channel_title = meta[cid]["title"]
        uploads_id = meta[cid]["uploads"]

        # 1) Pool de vidéos récentes
        pool_video_ids = playlist_items_limit(uploads_id, POOL_SIZE)
        if not pool_video_ids:
            print(f"[WARN] Aucune vidéo trouvée: {cid}")
            continue

        # 2) Les plus récentes
        recent_ids = pool_video_ids[:RECENT_N]

        # 3) Candidats "top sur 12 mois"
        pool_videos = videos_info(pool_video_ids)

        candidates: List[Tuple[int, str]] = []
        video_by_id = {}
        for v in pool_videos:
            vid = v.get("id")
            if not vid:
                continue
            video_by_id[vid] = v
            published_at = v.get("snippet", {}).get("publishedAt")
            if not published_at:
                continue
            if isoparse(published_at) < cutoff:
                continue
            views = int(v.get("statistics", {}).get("viewCount", 0))
            candidates.append((views, vid))

        candidates.sort(key=lambda x: x[0], reverse=True)
        top_recent_ids = [vid for _, vid in candidates[:TOP_RECENT_N]]

        # 4) Union + dédup
        selected: List[str] = []
        seen: Set[str] = set()
        for vid in recent_ids + top_recent_ids:
            if vid and vid not in seen:
                seen.add(vid)
                selected.append(vid)

        # 5) Recharger les infos seulement pour les selected (plus propre)
        selected_videos = videos_info(selected)

        rows = []
        for v in selected_videos:
            snippet = v.get("snippet", {})
            stats = v.get("statistics", {})
            cd = v.get("contentDetails", {})

            rows.append({
                "snapshot_month": snapshot_month,
                "snapshot_utc": snapshot_utc,
                "channel_id": cid,
                "channel_title": channel_title,
                "video_id": v.get("id", ""),
                "published_at": snippet.get("publishedAt", ""),
                "title": snippet.get("title", ""),
                "duration_iso8601": cd.get("duration", ""),
                "category_id": snippet.get("categoryId", ""),
                "view_count": stats.get("viewCount", ""),
                "like_count": stats.get("likeCount", ""),
                "comment_count": stats.get("commentCount", ""),
            })

        append_rows(rows)
        total_rows += len(rows)
        print(f"[OK] {channel_title} ({cid}) -> {len(rows)} vidéos")

    print(f"\nTerminé. Lignes ajoutées: {total_rows}")
    print(f"Fichier: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
