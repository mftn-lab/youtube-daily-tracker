import os
import csv
import time
import re
from datetime import datetime, timezone
from typing import List, Tuple, Set
from pathlib import Path

import requests

# ------------------------------------------------------------
# YouTube Daily Tracker
#
# Objectif :
# - Récupérer chaque jour les statistiques publiques de chaînes YouTube (thématique Tech)
# - Stocker un historique "time-series" dans youtube_daily_snapshots.csv
# - Maintenir une table de référence dans channels_reference.csv (titre, URL, dernière vue)
#
# Sécurité :
# - La clé API doit être fournie via la variable d'environnement : YOUTUBE_API_KEY
# - Ne jamais mettre de secret en clair dans le dépôt
#
# Monitoring :
# - Journal d'erreurs daily dans data/daily/errors_daily.csv
#   -> FORMAT_INVALID / NOT_FOUND / API_ERROR
# - Le script continue même si un chunk échoue.
# ------------------------------------------------------------

API_KEY = os.getenv("YOUTUBE_API_KEY")

# ------------------------------------------------------------
# Référentiel des chaînes (CSV = source de vérité)
# ------------------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent
CHANNELS_REFERENCE_PATH = PROJECT_DIR / "channels_reference.csv"

# Fichiers de sortie
DAILY_OUTFILE = "youtube_daily_snapshots.csv"
REF_OUTFILE = "channels_reference.csv"
LOGFILE = "run_log.txt"

# Monitoring erreurs
DATA_DAILY_DIR = Path("data") / "daily"
ERRORS_DAILY_CSV = DATA_DAILY_DIR / "errors_daily.csv"
ERRORS_DAILY_HEADER = ["snapshot_utc", "date_utc", "channel_id", "error_type", "message"]

# Limite YouTube API : max 50 IDs par requête
MAX_IDS_PER_REQUEST = 50

# Petit retry simple
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 2

# Encodage homogène CSV (Excel-friendly)
CSV_ENCODING = "utf-8-sig"


def log(msg: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{stamp}] {msg}"
    print(line)
    try:
        with open(LOGFILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def ensure_daily_dir() -> None:
    DATA_DAILY_DIR.mkdir(parents=True, exist_ok=True)


def init_errors_daily_file() -> None:
    """Crée data/daily/errors_daily.csv avec entête si absent."""
    ensure_daily_dir()
    if not ERRORS_DAILY_CSV.exists():
        with open(ERRORS_DAILY_CSV, "w", newline="", encoding=CSV_ENCODING) as f:
            w = csv.writer(f)
            w.writerow(ERRORS_DAILY_HEADER)


def append_error_daily(snapshot_utc: str, date_utc: str, channel_id: str, error_type: str, message: str) -> None:
    ensure_daily_dir()
    file_exists = ERRORS_DAILY_CSV.exists()
    with open(ERRORS_DAILY_CSV, "a", newline="", encoding=CSV_ENCODING) as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(ERRORS_DAILY_HEADER)
        w.writerow([snapshot_utc, date_utc, channel_id, error_type, message])


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def validate_channel_ids(ids: List[str]) -> Tuple[List[str], List[str]]:
    """
    Valide les Channel IDs YouTube.
    Format attendu : UC + 22 caractères (lettres, chiffres, _ ou -)
    """
    pattern = re.compile(r"^UC[a-zA-Z0-9_-]{22}$")

    valid: List[str] = []
    invalid: List[str] = []

    for cid in ids:
        c = cid.strip()
        if pattern.match(c):
            valid.append(c)
        else:
            invalid.append(c)

    return valid, invalid


def load_channels_reference(path: Path) -> list[dict]:
    """
    Lit channels_reference.csv (source de vérité).
    Supporte , ou ; + BOM éventuel.
    """
    if not path.exists():
        raise FileNotFoundError(f"channels_reference.csv introuvable : {path}")

    sample = path.read_text(encoding="utf-8", errors="replace")[:4096]
    delimiter = "," if sample.count(",") >= sample.count(";") else ";"

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)

        if not reader.fieldnames:
            raise ValueError("channels_reference.csv n'a pas d'entête.")

        # Gestion BOM : "﻿channel_id" au lieu de "channel_id"
        fieldnames = [fn.lstrip("\ufeff") for fn in reader.fieldnames]
        if "channel_id" not in fieldnames:
            raise ValueError("La colonne 'channel_id' est obligatoire dans channels_reference.csv")

        rows = []
        for row in reader:
            cleaned = {}
            for k, v in row.items():
                if k is None:
                    continue
                cleaned[k.lstrip("\ufeff")] = v
            rows.append(cleaned)
        return rows


def extract_channel_ids(rows: list[dict]) -> list[str]:
    ids = []
    seen = set()

    for row in rows:
        cid = (row.get("channel_id") or "").strip()
        if cid and cid not in seen:
            seen.add(cid)
            ids.append(cid)

    return ids


def youtube_channels_api_call(channel_ids: List[str]) -> List[dict]:
    if not API_KEY:
        raise RuntimeError("Clé API manquante. Vérifie la variable d'environnement 'YOUTUBE_API_KEY'.")

    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        # On ajoute contentDetails pour récupérer uploads_playlist_id
        "part": "snippet,statistics,contentDetails",
        "id": ",".join(channel_ids),
        "key": API_KEY,
    }

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            return data.get("items", [])
        except Exception as e:
            last_err = e
            log(f"Appel API en échec (tentative {attempt}/{MAX_RETRIES}) : {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)

    raise RuntimeError(f"Échec API après {MAX_RETRIES} tentatives : {last_err}")


def load_existing_daily_keys(outfile: str) -> set[tuple[str, str]]:
    """Charge les couples (date_utc, channel_id) déjà présents (anti-doublon)."""
    keys: Set[Tuple[str, str]] = set()
    if not os.path.isfile(outfile):
        return keys

    try:
        with open(outfile, "r", newline="", encoding=CSV_ENCODING) as f:
            reader = csv.DictReader(f)
            for row in reader:
                d = (row.get("date_utc") or "").strip()
                c = (row.get("channel_id") or "").strip()
                if d and c:
                    keys.add((d, c))
    except Exception as e:
        log(f"Warning : impossible de lire {outfile} pour anti-doublon : {e}")

    return keys


def append_rows_csv(outfile: str, header: List[str], rows: List[List]) -> None:
    file_exists = os.path.isfile(outfile)
    with open(outfile, "a", newline="", encoding=CSV_ENCODING) as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerows(rows)


def upsert_reference_full_schema(outfile: str, api_updates: dict) -> None:
    """
    Met à jour channels_reference.csv (schéma complet) sans écraser les colonnes manuelles.

    Auto (écrasable/actualisé par le script) :
    - channel_title
    - custom_url
    - channel_url
    - country
    - channel_published_at
    - uploads_playlist_id
    - last_seen_utc

    Manuel (conservé tel quel) :
    - language
    - tags
    - notes
    """

    fieldnames = [
        "channel_id",
        "channel_title",
        "custom_url",
        "channel_url",
        "country",
        "language",
        "tags",
        "notes",
        "channel_published_at",
        "uploads_playlist_id",
        "last_seen_utc",
    ]

    existing = {}

    if os.path.isfile(outfile):
        try:
            with open(outfile, "r", encoding=CSV_ENCODING, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = (row.get("channel_id") or "").strip()
                    if cid:
                        existing[cid] = row
        except Exception as e:
            log(f"Warning : impossible de lire {outfile} (reference) : {e}")

    for cid, api in api_updates.items():
        old = existing.get(cid, {})

        existing[cid] = {
            "channel_id": cid,
            "channel_title": api.get("channel_title", old.get("channel_title", "")),
            "custom_url": api.get("custom_url", old.get("custom_url", "")),
            "channel_url": api.get("channel_url", old.get("channel_url", f"https://www.youtube.com/channel/{cid}")),
            "country": api.get("country", old.get("country", "")),

            # Manuels : on garde toujours l'existant
            "language": old.get("language", ""),
            "tags": old.get("tags", ""),
            "notes": old.get("notes", ""),

            "channel_published_at": api.get("channel_published_at", old.get("channel_published_at", "")),
            "uploads_playlist_id": api.get("uploads_playlist_id", old.get("uploads_playlist_id", "")),
            "last_seen_utc": api.get("last_seen_utc", old.get("last_seen_utc", "")),
        }

    with open(outfile, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for cid in sorted(existing.keys()):
            # garantit que toutes les clés existent
            row = {k: (existing[cid].get(k, "") or "") for k in fieldnames}
            writer.writerow(row)


def main() -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    snapshot_utc = datetime.now(timezone.utc).isoformat()

    log("=== Début du run ===")
    log("API_KEY: OK" if API_KEY else "API_KEY: MISSING")
    log(f"Date du snapshot (UTC) : {today_utc}")

    init_errors_daily_file()

    # Charge les IDs depuis le référentiel CSV (source de vérité)
    ref_rows = load_channels_reference(CHANNELS_REFERENCE_PATH)
    channel_ids = extract_channel_ids(ref_rows)

    valid_ids, invalid_ids = validate_channel_ids(channel_ids)
    valid_ids = list(dict.fromkeys(valid_ids))  # dédup en conservant l'ordre

    if invalid_ids:
        log(f"IDs invalides détectés (ignorés) : {invalid_ids}")
        for bad in invalid_ids:
            append_error_daily(snapshot_utc, today_utc, bad, "FORMAT_INVALID", "Channel ID format invalide (typo probable).")

    if not valid_ids:
        raise RuntimeError("Aucun Channel ID valide. Ajoute au moins une chaîne (UC...) dans channels_reference.csv.")

    existing_keys = load_existing_daily_keys(DAILY_OUTFILE)
    chunks = chunk_list(valid_ids, MAX_IDS_PER_REQUEST)

    daily_rows_to_append: List[List] = []
    ref_updates = {}

    for idx, chunk in enumerate(chunks, start=1):
        log(f"Appel API chunk {idx}/{len(chunks)} — {len(chunk)} chaînes")

        try:
            items = youtube_channels_api_call(chunk)
        except Exception as e:
            log(f"[WARN] Chunk {idx}/{len(chunks)} ignoré suite à erreur API: {e}")
            for cid in chunk:
                append_error_daily(
                    snapshot_utc, today_utc, cid, "API_ERROR",
                    f"Erreur API channels.list (chunk {idx}/{len(chunks)}): {e}"
                )
            continue

        returned_ids_chunk = set()

        for it in items:
            cid = it.get("id", "")
            if not cid:
                continue

            returned_ids_chunk.add(cid)

            snippet = it.get("snippet", {}) or {}
            stats = it.get("statistics", {}) or {}
            content_details = it.get("contentDetails", {}) or {}
            related = (content_details.get("relatedPlaylists", {}) or {})

            title = snippet.get("title", "") or ""
            custom_url = snippet.get("customUrl", "") or ""
            country = snippet.get("country", "") or ""
            channel_published_at = snippet.get("publishedAt", "") or ""
            uploads_playlist_id = related.get("uploads", "") or ""

            url = f"https://www.youtube.com/channel/{cid}"

            subs = safe_int(stats.get("subscriberCount"), default=0)
            views = safe_int(stats.get("viewCount"), default=0)
            videos = safe_int(stats.get("videoCount"), default=0)

            # Données pour mise à jour du référentiel (schéma complet)
            ref_updates[cid] = {
                "channel_id": cid,
                "channel_title": title,
                "custom_url": custom_url,
                "channel_url": url,
                "country": country,
                "channel_published_at": channel_published_at,
                "uploads_playlist_id": uploads_playlist_id,
                "last_seen_utc": now_utc,
            }

            # Daily : anti-doublon (une fois par jour et par chaîne)
            if (today_utc, cid) in existing_keys:
                continue

            daily_rows_to_append.append([today_utc, cid, title, subs, views, videos])

        missing_chunk = sorted(set(chunk) - returned_ids_chunk)
        if missing_chunk:
            log(f"Attention : IDs non retournés (chunk {idx}/{len(chunks)}) : {missing_chunk}")
            for cid in missing_chunk:
                append_error_daily(
                    snapshot_utc, today_utc, cid, "NOT_FOUND",
                    "ID non retourné par l'API (typo, chaîne supprimée/privée, ou indisponible)."
                )

    daily_header = ["date_utc", "channel_id", "channel_title", "subscribers", "views", "videos"]

    if daily_rows_to_append:
        append_rows_csv(DAILY_OUTFILE, daily_header, daily_rows_to_append)
        log(f"CSV daily mis à jour : +{len(daily_rows_to_append)} lignes")
    else:
        log("Aucune nouvelle ligne à ajouter (peut-être déjà collecté aujourd'hui).")

    # Mise à jour du référentiel complet (sans écraser language/tags/notes)
    if ref_updates:
        upsert_reference_full_schema(REF_OUTFILE, ref_updates)
        log(f"Référence mise à jour : {len(ref_updates)} chaînes (schéma complet)")
    else:
        log("Référence non modifiée (aucune chaîne retournée).")

    log("=== Fin du run ===")


if __name__ == "__main__":
    main()
