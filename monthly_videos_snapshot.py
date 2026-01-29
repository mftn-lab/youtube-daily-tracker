import os
import csv
import time
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple
from pathlib import Path

import requests
from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta

# ------------------------------------------------------------
# Monthly Videos Snapshot (hardened)
#
# Objectif :
# - Lire channels_reference.csv (source de vérité)
# - Pour chaque chaîne :
#   - 20 vidéos les plus récentes (Uploads)
#   - + 20 vidéos les plus vues parmi celles des 12 derniers mois
#   - déduplication
# - Écrit 1 fichier par mois : data/monthly/videos_YYYY-MM.csv (overwrite)
# - Journalise les problèmes : data/monthly/errors_YYYY-MM.csv
#
# Renforcements :
# - try/except par chaîne (le run ne casse plus)
# - validation uploads_playlist_id + fallback API
# - logs erreurs plus précis (API/playlist/videos)
# - aucune KeyError (accès dict safe)
# ------------------------------------------------------------

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# Sélection
RECENT_N = 20
TOP_VIEWED_N = 20
TOP_WINDOW_MONTHS = 12
POOL_SIZE = 120

# Fichiers
CHANNELS_REFERENCE_CSV = Path("channels_reference.csv")
OUTPUT_DIR = Path("data") / "monthly"
CSV_ENCODING = "utf-8-sig"

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

ERROR_FIELDS = [
    "snapshot_month",
    "snapshot_utc",
    "channel_id",
    "error_type",
    "message",
]

CHANNEL_ID_RE = re.compile(r"^UC[a-zA-Z0-9_-]{22}$")
UPLOADS_PLAYLIST_RE = re.compile(r"^UU[a-zA-Z0-9_-]{22}$")  # uploads playlist id


class YouTubeAPIError(RuntimeError):
    pass


# ------------------------------------------------------------
# Utils
# ------------------------------------------------------------

def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_api_key() -> str:
    key = os.getenv("YOUTUBE_API_KEY")
    if not key:
        raise YouTubeAPIError("Clé API manquante : définis YOUTUBE_API_KEY.")
    return key


def yt_get(endpoint: str, params: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
    api_key = get_api_key()
    url = f"{YOUTUBE_API_BASE}/{endpoint}"

    params = dict(params)
    params["key"] = api_key

    last_err = None

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=30)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 403:
                raise YouTubeAPIError(f"HTTP 403 (quota/forbidden): {r.text[:200]}")

            if r.status_code == 429 or 500 <= r.status_code <= 599:
                last_err = f"HTTP {r.status_code}"
                time.sleep(1.5 ** attempt)
                continue

            last_err = f"HTTP {r.status_code}"
            break

        except requests.RequestException as e:
            last_err = str(e)
            time.sleep(1.5 ** attempt)

    raise YouTubeAPIError(f"Erreur API sur {endpoint}: {last_err}")


def chunk(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def add_error(errors: List[Dict[str, Any]], snapshot_month: str, snapshot_utc: str,
              channel_id: str, error_type: str, message: str) -> None:
    errors.append({
        "snapshot_month": snapshot_month,
        "snapshot_utc": snapshot_utc,
        "channel_id": channel_id,
        "error_type": error_type,
        "message": message,
    })


# ------------------------------------------------------------
# Chargement du référentiel
# ------------------------------------------------------------

def load_channels_reference(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"{path} introuvable")

    sample = path.read_text(encoding="utf-8", errors="replace")[:4096]
    delimiter = "," if sample.count(",") >= sample.count(";") else ";"

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = []
        for row in reader:
            cleaned = {k.lstrip("\ufeff"): (v or "") for k, v in row.items() if k}
            rows.append(cleaned)
        return rows


def extract_channels(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []

    for r in rows:
        cid = r.get("channel_id", "").strip()
        if not cid or cid in seen:
            continue
        seen.add(cid)
        out.append({
            "channel_id": cid,
            "channel_title": r.get("channel_title", "").strip(),
            "uploads_playlist_id": r.get("uploads_playlist_id", "").strip(),
        })

    return out


def validate_channel_ids(ids: List[str]) -> Tuple[List[str], List[Dict[str, str]]]:
    valid = []
    errors = []
    seen = set()

    for cid in ids:
        cid = (cid or "").strip()

        if cid in seen:
            errors.append({"channel_id": cid, "error_type": "DUPLICATE_ID", "message": "ID dupliqué"})
            continue
        seen.add(cid)

        if not CHANNEL_ID_RE.match(cid):
            errors.append({"channel_id": cid, "error_type": "FORMAT_INVALID", "message": "Format invalide"})
            continue

        valid.append(cid)

    return valid, errors


# ------------------------------------------------------------
# API helpers
# ------------------------------------------------------------

def channels_info(ids: List[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for batch in chunk(ids, 50):
        data = yt_get("channels", {"part": "snippet,contentDetails", "id": ",".join(batch)})
        items.extend(data.get("items", []))
    return items


def safe_get_uploads_playlist_id(ch: Dict[str, Any]) -> str:
    try:
        return (ch.get("contentDetails", {}) or {}).get("relatedPlaylists", {}).get("uploads", "") or ""
    except Exception:
        return ""


def playlist_items_limit(playlist_id: str, max_items: int) -> List[str]:
    vids: List[str] = []
    token = None

    while len(vids) < max_items:
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": min(50, max_items - len(vids)),
        }
        if token:
            params["pageToken"] = token

        data = yt_get("playlistItems", params)

        for it in data.get("items", []):
            vid = (it.get("contentDetails", {}) or {}).get("videoId")
            if vid:
                vids.append(vid)

        token = data.get("nextPageToken")
        if not token:
            break

    return vids[:max_items]


def videos_info(video_ids: List[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for batch in chunk(video_ids, 50):
        data = yt_get("videos", {"part": "snippet,contentDetails,statistics", "id": ",".join(batch)})
        items.extend(data.get("items", []))
    return items


def atomic_write_csv(path: Path, fields: List[str], rows: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")

    with open(tmp, "w", encoding=CSV_ENCODING, newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        if rows:
            w.writerows(rows)

    tmp.replace(path)


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main() -> None:
    snapshot_utc = now_utc_iso()
    snapshot_month = datetime.now(timezone.utc).strftime("%Y-%m")
    cutoff = datetime.now(timezone.utc) - relativedelta(months=TOP_WINDOW_MONTHS)

    output_csv = OUTPUT_DIR / f"videos_{snapshot_month}.csv"
    errors_csv = OUTPUT_DIR / f"errors_{snapshot_month}.csv"

    ref_rows = load_channels_reference(CHANNELS_REFERENCE_CSV)
    channels = extract_channels(ref_rows)

    valid_ids, pre_errors = validate_channel_ids([c["channel_id"] for c in channels])

    errors: List[Dict[str, Any]] = []
    for e in pre_errors:
        add_error(errors, snapshot_month, snapshot_utc, e["channel_id"], e["error_type"], e["message"])

    ref_by_id = {c["channel_id"]: c for c in channels}

    # meta[cid] = {"title": "...", "uploads": "..."}
    meta: Dict[str, Dict[str, str]] = {}

    # 1) Take uploads_playlist_id from reference if valid
    missing_api: List[str] = []
    for cid in valid_ids:
        r = ref_by_id.get(cid, {})
        title = (r.get("channel_title") or "").strip()
        uploads = (r.get("uploads_playlist_id") or "").strip()

        if uploads and UPLOADS_PLAYLIST_RE.match(uploads):
            meta[cid] = {"title": title, "uploads": uploads}
        else:
            # uploads absent ou invalide -> fallback API
            missing_api.append(cid)

    # 2) Fallback: fetch channels info for missing uploads/title
    if missing_api:
        try:
            api_items = channels_info(missing_api)
        except Exception as e:
            # Si l'API plante ici, on log et on tentera quand même la suite (mais beaucoup seront NOT_FOUND)
            for cid in missing_api:
                add_error(errors, snapshot_month, snapshot_utc, cid, "API_ERROR", f"channels.list failed: {e}")
            api_items = []

        returned = set()
        for ch in api_items:
            cid = (ch.get("id") or "").strip()
            if not cid:
                continue
            returned.add(cid)

            title = ((ch.get("snippet", {}) or {}).get("title") or "").strip()
            uploads = safe_get_uploads_playlist_id(ch).strip()

            if not uploads:
                add_error(errors, snapshot_month, snapshot_utc, cid, "UPLOADS_MISSING",
                          "uploads playlist id missing in API response")
                continue
            if not UPLOADS_PLAYLIST_RE.match(uploads):
                add_error(errors, snapshot_month, snapshot_utc, cid, "UPLOADS_INVALID",
                          f"uploads playlist id invalid: {uploads}")
                continue

            meta[cid] = {"title": title, "uploads": uploads}

        # IDs non retournés -> NOT_FOUND
        for cid in missing_api:
            if cid not in returned and cid not in meta:
                add_error(errors, snapshot_month, snapshot_utc, cid, "NOT_FOUND",
                          "Chaîne non retournée par l'API (channels.list)")

    rows: List[Dict[str, Any]] = []

    # 3) Per-channel processing (never crash the run)
    for cid in valid_ids:
        if cid not in meta:
            # déjà loggé (NOT_FOUND / UPLOADS_MISSING etc.)
            continue

        try:
            uploads_id = meta[cid]["uploads"]
            channel_title = meta[cid]["title"]

            # a) Build pool from uploads playlist
            try:
                pool_ids = playlist_items_limit(uploads_id, POOL_SIZE)
            except Exception as e:
                add_error(errors, snapshot_month, snapshot_utc, cid, "PLAYLIST_ERROR", f"playlistItems failed: {e}")
                continue

            if not pool_ids:
                add_error(errors, snapshot_month, snapshot_utc, cid, "EMPTY_UPLOADS",
                          "No videos returned from uploads playlist")
                continue

            # b) Fetch video details for pool
            try:
                pool_videos = videos_info(pool_ids)
            except Exception as e:
                add_error(errors, snapshot_month, snapshot_utc, cid, "VIDEOS_ERROR", f"videos.list failed: {e}")
                continue

            by_id = {v.get("id"): v for v in pool_videos if v.get("id")}

            # c) Recent (playlist is ordered by recency)
            recent = pool_ids[:RECENT_N]

            # d) Top viewed within last 12 months
            candidates: List[Tuple[int, str]] = []
            for vid, v in by_id.items():
                if not vid:
                    continue
                try:
                    published_at = (v.get("snippet", {}) or {}).get("publishedAt", "")
                    if not published_at:
                        continue
                    if isoparse(published_at) < cutoff:
                        continue
                except Exception:
                    continue

                views = safe_int((v.get("statistics", {}) or {}).get("viewCount"), default=0)
                candidates.append((views, vid))

            candidates.sort(reverse=True)
            top = [vid for _, vid in candidates[:TOP_VIEWED_N]]

            # e) Dedup preserve order
            selected: List[str] = []
            seen: set = set()
            for vid in recent + top:
                if vid and vid not in seen:
                    seen.add(vid)
                    selected.append(vid)

            # f) Emit rows
            for vid in selected:
                v = by_id.get(vid)
                if not v:
                    continue

                sn = v.get("snippet", {}) or {}
                cd = v.get("contentDetails", {}) or {}
                st = v.get("statistics", {}) or {}

                rows.append({
                    "snapshot_month": snapshot_month,
                    "snapshot_utc": snapshot_utc,
                    "channel_id": cid,
                    "channel_title": channel_title,
                    "video_id": vid,
                    "published_at": sn.get("publishedAt", "") or "",
                    "title": sn.get("title", "") or "",
                    "duration_iso8601": cd.get("duration", "") or "",
                    "category_id": sn.get("categoryId", "") or "",
                    "view_count": safe_int(st.get("viewCount"), default=0),
                    "like_count": safe_int(st.get("likeCount"), default=0),
                    "comment_count": safe_int(st.get("commentCount"), default=0),
                })

        except Exception as e:
            # Catch-all per channel
            add_error(errors, snapshot_month, snapshot_utc, cid, "UNEXPECTED_ERROR", str(e))
            continue

    atomic_write_csv(output_csv, FIELDS, rows)
    atomic_write_csv(errors_csv, ERROR_FIELDS, errors)

    print(f"[OK] Monthly snapshot écrit : {output_csv}")
    print(f"[OK] Erreurs : {errors_csv}")
    print(f"[OK] Chaînes valides (format) : {len(valid_ids)}")
    print(f"[OK] Vidéos collectées : {len(rows)}")
    print(f"[OK] Erreurs loggées : {len(errors)}")


if __name__ == "__main__":
    main()
