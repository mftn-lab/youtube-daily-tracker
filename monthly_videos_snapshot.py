import os
import csv
import time
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Set, Tuple, Optional
from pathlib import Path

import requests
from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta

# ------------------------------------------------------------
# Monthly Videos Snapshot (robust)
# - Lit CHANNEL_IDS depuis collect_youtube.py
# - Pour chaque chaîne :
#   - 20 vidéos les plus récentes (Uploads)
#   - + 20 vidéos les plus vues parmi celles des 12 derniers mois
#   - déduplication
# - Écrit 1 fichier par mois : data/monthly/videos_YYYY-MM.csv (overwrite)
# - Journalise les problèmes : data/monthly/errors_YYYY-MM.csv
#   -> coquille d'ID / chaîne supprimée / aucune vidéo / erreur API
# ------------------------------------------------------------

from collect_youtube import CHANNEL_IDS  # ne pas modifier collect_youtube.py

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# Paramètres de sélection
RECENT_N = 20
TOP_VIEWED_N = 20          # (renommé: plus clair que TOP_RECENT_N)
TOP_WINDOW_MONTHS = 12
POOL_SIZE = 120

# Dossier de sortie (1 fichier par mois)
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

# Regex stricte (comme ton daily)
CHANNEL_ID_RE = re.compile(r"^UC[a-zA-Z0-9_-]{22}$")


class YouTubeAPIError(RuntimeError):
    pass


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_api_key() -> str:
    key = os.getenv("YOUTUBE_API_KEY")
    if not key:
        raise YouTubeAPIError("Clé API manquante : définis YOUTUBE_API_KEY avant de lancer le script.")
    return key


def yt_get(endpoint: str, params: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
    """
    GET YouTube Data API avec retries raisonnables:
    - Retry: 429, 5xx, timeouts
    - 403 quota: remonte une erreur explicite
    - 4xx (hors 429/403): considéré comme définitif
    """
    api_key = get_api_key()
    url = f"{YOUTUBE_API_BASE}/{endpoint}"

    params = dict(params)
    params["key"] = api_key

    last_err: Optional[str] = None

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=30)

            if r.status_code == 200:
                return r.json()

            # 403: souvent quota / forbidden
            if r.status_code == 403:
                snippet = (r.text or "")[:300]
                raise YouTubeAPIError(f"HTTP 403 (forbidden/quota?) sur {endpoint}: {snippet}")

            # 429: rate limit, retry
            if r.status_code == 429 or 500 <= r.status_code <= 599:
                last_err = f"HTTP {r.status_code}: {(r.text or '')[:300]}"
                # backoff simple
                time.sleep(1.5 ** attempt)
                continue

            # autres 4xx : pas retry
            last_err = f"HTTP {r.status_code}: {(r.text or '')[:300]}"
            break

        except requests.RequestException as e:
            last_err = str(e)
            time.sleep(1.5 ** attempt)

    raise YouTubeAPIError(f"Erreur API sur {endpoint}: {last_err}")


def chunk(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def validate_channel_ids(ids: List[str]) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Sépare les IDs valides des invalides (format + doublons)."""
    valid: List[str] = []
    errors: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    for raw in ids:
        cid = (raw or "").strip()
        if not cid:
            continue

        if cid in seen:
            errors.append({
                "channel_id": cid,
                "error_type": "DUPLICATE_ID",
                "message": "Channel ID dupliqué dans CHANNEL_IDS (ignoré).",
            })
            continue
        seen.add(cid)

        if not CHANNEL_ID_RE.match(cid):
            errors.append({
                "channel_id": cid,
                "error_type": "FORMAT_INVALID",
                "message": "Format invalide (attendu: UC + 22 caractères [A-Za-z0-9_-]).",
            })
            continue

        valid.append(cid)

    return valid, errors


def channels_info(channel_ids: List[str]) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    for batch in chunk(channel_ids, 50):
        data = yt_get(
            "channels",
            {"part": "snippet,contentDetails", "id": ",".join(batch), "maxResults": 50},
        )
        all_items.extend(data.get("items", []))
    return all_items


def get_uploads_playlist_id(channel_item: Dict[str, Any]) -> str:
    return channel_item["contentDetails"]["relatedPlaylists"]["uploads"]


def playlist_items_limit(playlist_id: str, max_items: int) -> List[str]:
    video_ids: List[str] = []
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
    all_items: List[Dict[str, Any]] = []
    for batch in chunk(video_ids, 50):
        data = yt_get(
            "videos",
            {"part": "snippet,contentDetails,statistics", "id": ",".join(batch), "maxResults": 50},
        )
        all_items.extend(data.get("items", []))
    return all_items


def atomic_write_csv(file_path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    """
    Écriture atomique: écrit dans un fichier temporaire puis remplace.
    Évite les CSV partiels si le script crash.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")

    with open(tmp_path, "w", encoding=CSV_ENCODING, newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        if rows:
            w.writerows(rows)

    tmp_path.replace(file_path)


def main() -> None:
    snapshot_utc = now_utc_iso()
    snapshot_month = datetime.now(timezone.utc).strftime("%Y-%m")
    cutoff = datetime.now(timezone.utc) - relativedelta(months=TOP_WINDOW_MONTHS)

    output_csv = OUTPUT_DIR / f"videos_{snapshot_month}.csv"
    errors_csv = OUTPUT_DIR / f"errors_{snapshot_month}.csv"

    # 1) Validation format + doublons
    valid_ids, pre_errors = validate_channel_ids(CHANNEL_IDS)

    errors: List[Dict[str, Any]] = [{
        "snapshot_month": snapshot_month,
        "snapshot_utc": snapshot_utc,
        "channel_id": e["channel_id"],
        "error_type": e["error_type"],
        "message": e["message"],
    } for e in pre_errors]

    # 2) channels.list pour metadata + uploads playlist
    meta: Dict[str, Dict[str, str]] = {}
    try:
        channels = channels_info(valid_ids)
    except Exception as ex:
        errors.append({
            "snapshot_month": snapshot_month,
            "snapshot_utc": snapshot_utc,
            "channel_id": "",
            "error_type": "API_ERROR",
            "message": f"Impossible de récupérer les chaînes (channels.list) : {ex}",
        })
        atomic_write_csv(output_csv, FIELDS, [])          # fichier data vide mais valide
        atomic_write_csv(errors_csv, ERROR_FIELDS, errors)
        print("[ERROR] L'API a échoué sur channels.list. Voir errors CSV.")
        print(f"Fichier erreurs: {errors_csv}")
        return

    for ch in channels:
        cid = ch.get("id")
        if not cid:
            continue
        try:
            meta[cid] = {
                "title": ch.get("snippet", {}).get("title", ""),
                "uploads": get_uploads_playlist_id(ch),
            }
        except Exception as ex:
            errors.append({
                "snapshot_month": snapshot_month,
                "snapshot_utc": snapshot_utc,
                "channel_id": cid,
                "error_type": "MISSING_UPLOADS",
                "message": f"Impossible de lire la playlist Uploads: {ex}",
            })

    all_rows: List[Dict[str, Any]] = []
    ok_channels = 0
    warn_channels = 0

    # 3) Traitement chaîne par chaîne
    for cid in valid_ids:
        if cid not in meta:
            warn_channels += 1
            errors.append({
                "snapshot_month": snapshot_month,
                "snapshot_utc": snapshot_utc,
                "channel_id": cid,
                "error_type": "NOT_FOUND",
                "message": "ChannelId introuvable via API (typo, chaîne supprimée/privée, ou ID invalide).",
            })
            print(f"[WARN] ChannelId introuvable via API: {cid}")
            continue

        channel_title = meta[cid]["title"]
        uploads_id = meta[cid]["uploads"]

        # pool depuis Uploads
        try:
            pool_video_ids = playlist_items_limit(uploads_id, POOL_SIZE)
        except Exception as ex:
            warn_channels += 1
            errors.append({
                "snapshot_month": snapshot_month,
                "snapshot_utc": snapshot_utc,
                "channel_id": cid,
                "error_type": "API_ERROR",
                "message": f"Erreur API playlistItems: {ex}",
            })
            print(f"[WARN] API playlistItems failed: {cid}")
            continue

        if not pool_video_ids:
            warn_channels += 1
            errors.append({
                "snapshot_month": snapshot_month,
                "snapshot_utc": snapshot_utc,
                "channel_id": cid,
                "error_type": "NO_VIDEOS",
                "message": "Aucune vidéo trouvée dans la playlist Uploads.",
            })
            print(f"[WARN] Aucune vidéo trouvée: {cid}")
            continue

        recent_ids = pool_video_ids[:RECENT_N]

        # infos sur le pool
        try:
            pool_videos = videos_info(pool_video_ids)
        except Exception as ex:
            warn_channels += 1
            errors.append({
                "snapshot_month": snapshot_month,
                "snapshot_utc": snapshot_utc,
                "channel_id": cid,
                "error_type": "API_ERROR",
                "message": f"Erreur API videos.list (pool): {ex}",
            })
            print(f"[WARN] API videos.list(pool) failed: {cid}")
            continue

        pool_by_id: Dict[str, Dict[str, Any]] = {v.get("id"): v for v in pool_videos if v.get("id")}

        # top vues sur fenêtre (12 mois)
        candidates: List[Tuple[int, str]] = []
        for vid, v in pool_by_id.items():
            published_at = v.get("snippet", {}).get("publishedAt")
            if not published_at:
                continue
            try:
                if isoparse(published_at) < cutoff:
                    continue
            except Exception:
                continue

            views = safe_int(v.get("statistics", {}).get("viewCount", 0), default=0)
            candidates.append((views, vid))

        candidates.sort(key=lambda x: x[0], reverse=True)
        top_viewed_ids = [vid for _, vid in candidates[:TOP_VIEWED_N]]

        # dédup recent + top viewed
        selected: List[str] = []
        seen_vids: Set[str] = set()
        for vid in recent_ids + top_viewed_ids:
            if vid and vid not in seen_vids:
                seen_vids.add(vid)
                selected.append(vid)

        # on réutilise les data du pool autant que possible
        selected_videos: List[Dict[str, Any]] = []
        missing_selected: List[str] = [vid for vid in selected if vid not in pool_by_id]

        selected_videos.extend([pool_by_id[vid] for vid in selected if vid in pool_by_id])

        if missing_selected:
            try:
                extra = videos_info(missing_selected)
                selected_videos.extend(extra)
            except Exception as ex:
                warn_channels += 1
                errors.append({
                    "snapshot_month": snapshot_month,
                    "snapshot_utc": snapshot_utc,
                    "channel_id": cid,
                    "error_type": "API_ERROR",
                    "message": f"Erreur API videos.list (selected-extra): {ex}",
                })
                print(f"[WARN] API videos.list(selected-extra) failed: {cid}")
                continue

        # lignes
        for v in selected_videos:
            snippet = v.get("snippet", {}) or {}
            stats = v.get("statistics", {}) or {}
            cd = v.get("contentDetails", {}) or {}

            all_rows.append({
                "snapshot_month": snapshot_month,
                "snapshot_utc": snapshot_utc,
                "channel_id": cid,
                "channel_title": channel_title,
                "video_id": v.get("id", "") or "",
                "published_at": snippet.get("publishedAt", "") or "",
                "title": snippet.get("title", "") or "",
                "duration_iso8601": cd.get("duration", "") or "",
                "category_id": snippet.get("categoryId", "") or "",
                "view_count": stats.get("viewCount", "") or "",
                "like_count": stats.get("likeCount", "") or "",
                "comment_count": stats.get("commentCount", "") or "",
            })

        ok_channels += 1
        print(f"[OK] {channel_title} ({cid}) -> {len(selected_videos)} vidéos")

    # 4) Écriture atomique (données + erreurs)
    atomic_write_csv(output_csv, FIELDS, all_rows)
    atomic_write_csv(errors_csv, ERROR_FIELDS, errors)

    # Résumé final
    total_input = len(CHANNEL_IDS)
    total_valid_format = len(valid_ids)
    total_pre_errors = len(pre_errors)
    total_not_found = len([e for e in errors if e.get("error_type") == "NOT_FOUND"])

    print("\n--- RÉSUMÉ ---")
    print(f"Chaînes dans CHANNEL_IDS: {total_input}")
    print(f"Format OK: {total_valid_format}")
    print(f"Erreurs format/doublons: {total_pre_errors}")
    print(f"Chaînes OK traitées: {ok_channels}")
    print(f"Chaînes en warning: {warn_channels}")
    print(f"NOT_FOUND (API): {total_not_found}")
    print(f"Lignes écrites: {len(all_rows)}")
    print(f"Fichier données: {output_csv}")
    print(f"Fichier erreurs: {errors_csv}")


if __name__ == "__main__":
    main()
