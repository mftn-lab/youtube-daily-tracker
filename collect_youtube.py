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
#
# + Validation serveur (best practice) :
# - Cache OK/MISSING dans data/daily/channels_validation_cache.csv
#   -> évite de retester tous les jours des IDs supprimés/typos
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

# Cache validation serveur (anti-IDs fantômes)
VALIDATION_CACHE_CSV = DATA_DAILY_DIR / "channels_validation_cache.csv"
VALIDATION_CACHE_HEADER = ["channel_id", "status", "title", "last_checked_utc"]  # status: ok | missing | invalid

# Limite YouTube API : max 50 IDs par requête
MAX_IDS_PER_REQUEST = 50

# Petit retry simple
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 2

# Encodage homogène CSV (Excel-friendly)
# -> utf-8-sig gère le BOM à l'écriture (Excel) ET à la lecture (si on l'utilise)
CSV_ENCODING = "utf-8-sig"


def log(msg: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{stamp}] {msg}"
    print(line)
    try:
        # Cohérence avec le reste (Excel/Windows friendly)
        with open(LOGFILE, "a", encoding=CSV_ENCODING) as f:
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


# -------------------------
# Validation cache (serveur)
# -------------------------

def init_validation_cache_file() -> None:
    """Crée data/daily/channels_validation_cache.csv avec entête si absent."""
    ensure_daily_dir()
    if not VALIDATION_CACHE_CSV.exists():
        with open(VALIDATION_CACHE_CSV, "w", newline="", encoding=CSV_ENCODING) as f:
            w = csv.writer(f)
            w.writerow(VALIDATION_CACHE_HEADER)


def load_validation_cache() -> dict:
    """
    Retourne un dict:
      cache[channel_id] = {"status": "...", "title": "...", "last_checked_utc": "..."}
    """
    init_validation_cache_file()
    cache: dict = {}
    try:
        with open(VALIDATION_CACHE_CSV, "r", newline="", encoding=CSV_ENCODING) as f:
            reader = csv.DictReader(f)
            for row in reader:
                cid = (row.get("channel_id") or "").strip()
                if cid:
                    cache[cid] = {
                        "status": (row.get("status") or "").strip(),
                        "title": (row.get("title") or "").strip(),
                        "last_checked_utc": (row.get("last_checked_utc") or "").strip(),
                    }
    except Exception as e:
        log(f"Warning : impossible de lire le cache validation : {e}")
    return cache


def save_validation_cache(cache: dict) -> None:
    """Réécrit le cache complet (simple et robuste)."""
    init_validation_cache_file()
    try:
        with open(VALIDATION_CACHE_CSV, "w", newline="", encoding=CSV_ENCODING) as f:
            w = csv.DictWriter(f, fieldnames=VALIDATION_CACHE_HEADER)
            w.writeheader()
            for cid in sorted(cache.keys()):
                row = cache.get(cid, {}) or {}
                w.writerow({
                    "channel_id": cid,
                    "status": row.get("status", ""),
                    "title": row.get("title", ""),
                    "last_checked_utc": row.get("last_checked_utc", ""),
                })
    except Exception as e:
        log(f"Warning : impossible d'écrire le cache validation : {e}")


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
        c = (cid or "").strip()
        if pattern.match(c):
            valid.append(c)
        else:
            invalid.append(c)

    return valid, invalid


def _detect_delimiter(sample: str) -> str:
    """Détecte , ou ; de façon simple et robuste."""
    return "," if sample.count(",") >= sample.count(";") else ";"


def load_channels_reference(path: Path) -> list[dict]:
    """
    Lit channels_reference.csv (source de vérité).

    Robuste :
    - Supporte , ou ;
    - Supporte BOM UTF-8 (Excel) via utf-8-sig
    - Normalise les noms de colonnes et les clés (strip + suppression BOM)
    """
    if not path.exists():
        raise FileNotFoundError(f"channels_reference.csv introuvable : {path}")

    # Lire un échantillon en utf-8-sig pour neutraliser le BOM éventuel
    sample = path.read_text(encoding="utf-8-sig", errors="replace")[:4096]
    delimiter = _detect_delimiter(sample)

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)

        if not reader.fieldnames:
            raise ValueError("channels_reference.csv n'a pas d'entête.")

        normalized_fieldnames = [fn.lstrip("\ufeff").strip() for fn in reader.fieldnames if fn]

        if "channel_id" not in normalized_fieldnames:
            raise ValueError(
                "La colonne 'channel_id' est obligatoire dans channels_reference.csv "
                f"(trouvé: {normalized_fieldnames})"
            )

        rows: list[dict] = []
        for row in reader:
            cleaned: dict = {}
            for k, v in (row or {}).items():
                if k is None:
                    continue
                nk = k.lstrip("\ufeff").strip()
                cleaned[nk] = v
            rows.append(cleaned)

        return rows


def extract_channel_ids(rows: list[dict]) -> list[str]:
    ids: List[str] = []
    seen: set = set()

    for row in rows:
        # Support au cas où une ligne aurait encore une clé BOM
        cid = (row.get("channel_id") or row.get("\ufeffchannel_id") or "").strip()
        if cid and cid not in seen:
            seen.add(cid)
            ids.append(cid)

    return ids


def youtube_channels_api_call(channel_ids: List[str]) -> List[dict]:
    if not API_KEY:
        raise RuntimeError("Clé API manquante. Vérifie la variable d'environnement 'YOUTUBE_API_KEY'.")

    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
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


def validate_channel_ids_server(
    snapshot_utc: str,
    today_utc: str,
    now_utc: str,
    candidate_ids: List[str],
) -> List[str]:
    """
    Validation "côté serveur" via l'API channels.list :
    - garde uniquement les IDs réellement retournés par l'API (existants)
    - met en cache ok/missing
    - évite de retaper l'API pour les IDs déjà validés ok
    - si erreur API sur un chunk : chunk ignoré (mode strict) + log API_ERROR
    """
    cache = load_validation_cache()

    # Garder ceux déjà OK en cache
    server_ok_ids: List[str] = []
    for cid in candidate_ids:
        if cache.get(cid, {}).get("status") == "ok":
            server_ok_ids.append(cid)

    # Vérifier uniquement ceux pas OK
    to_check = [cid for cid in candidate_ids if cache.get(cid, {}).get("status") != "ok"]

    if not to_check:
        return list(dict.fromkeys(server_ok_ids))

    log(f"Validation serveur : {len(to_check)} IDs à vérifier (cache OK: {len(server_ok_ids)})")

    for chunk in chunk_list(to_check, MAX_IDS_PER_REQUEST):
        try:
            items = youtube_channels_api_call(chunk)
        except Exception as e:
            log(f"[WARN] Validation serveur: erreur API, chunk ignoré : {e}")
            for cid in chunk:
                append_error_daily(
                    snapshot_utc,
                    today_utc,
                    cid,
                    "API_ERROR",
                    f"Validation serveur: erreur API, chunk ignoré: {e}",
                )
            continue

        returned_ids = set()

        for it in items:
            cid = (it.get("id") or "").strip()
            if not cid:
                continue

            returned_ids.add(cid)
            title = ((it.get("snippet") or {}).get("title") or "").strip()

            cache[cid] = {
                "status": "ok",
                "title": title,
                "last_checked_utc": now_utc,
            }

        missing = sorted(set(chunk) - returned_ids)
        for cid in missing:
            cache[cid] = {
                "status": "missing",
                "title": "",
                "last_checked_utc": now_utc,
            }
            append_error_daily(
                snapshot_utc,
                today_utc,
                cid,
                "NOT_FOUND",
                "Validation serveur: ID non retourné par l'API (typo, chaîne supprimée/privée).",
            )

        # Ajouter les IDs validés (ordre initial conservé)
        for cid in chunk:
            if cid in returned_ids:
                server_ok_ids.append(cid)

    save_validation_cache(cache)

    # Dédup en conservant l'ordre
    return list(dict.fromkeys(server_ok_ids))


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

    def pick(*vals: str) -> str:
        """Retourne la première valeur non vide (non None, non '')."""
        for v in vals:
            if v is None:
                continue
            s = str(v).strip()
            if s != "":
                return s
        return ""

    fieldnames = [
        "channel_id",
        "channel_title",
        "custom_url",
        "channel_url",
        "country",
        "channel_published_at",
        "uploads_playlist_id",
        "last_seen_utc",
    ]

    existing = {}

    if os.path.isfile(outfile):
        try:
            # Lecture en utf-8-sig pour gérer le BOM proprement
            with open(outfile, "r", encoding="utf-8-sig", newline="") as f:
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
            "channel_title": pick(api.get("channel_title"), old.get("channel_title")),
            "custom_url": pick(api.get("custom_url"), old.get("custom_url")),
            "channel_url": pick(
                api.get("channel_url"),
                old.get("channel_url"),
                f"https://www.youtube.com/channel/{cid}",
            ),
            "country": pick(api.get("country"), old.get("country")),
            "channel_published_at": pick(api.get("channel_published_at"), old.get("channel_published_at")),
            "uploads_playlist_id": pick(api.get("uploads_playlist_id"), old.get("uploads_playlist_id")),
            "last_seen_utc": pick(api.get("last_seen_utc"), old.get("last_seen_utc")),
        }

    with open(outfile, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for cid in sorted(existing.keys()):
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
    init_validation_cache_file()

    # Charge les IDs depuis le référentiel CSV (source de vérité)
    ref_rows = load_channels_reference(CHANNELS_REFERENCE_PATH)
    channel_ids = extract_channel_ids(ref_rows)

    valid_ids, invalid_ids = validate_channel_ids(channel_ids)
    valid_ids = list(dict.fromkeys(valid_ids))  # dédup en conservant l'ordre

    if invalid_ids:
        log(f"IDs invalides détectés (ignorés) : {invalid_ids}")
        for bad in invalid_ids:
            append_error_daily(
                snapshot_utc,
                today_utc,
                bad,
                "FORMAT_INVALID",
                "Channel ID format invalide (typo probable).",
            )

    if not valid_ids:
        raise RuntimeError("Aucun Channel ID valide. Ajoute au moins une chaîne (UC...) dans channels_reference.csv.")

    # Validation serveur (cache ok/missing)
    valid_ids = validate_channel_ids_server(snapshot_utc, today_utc, now_utc, valid_ids)

    if not valid_ids:
        raise RuntimeError(
            "Aucun Channel ID valide et existant côté serveur (tout est missing). Vérifie channels_reference.csv."
        )

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
                    snapshot_utc,
                    today_utc,
                    cid,
                    "API_ERROR",
                    f"Erreur API channels.list (chunk {idx}/{len(chunks)}): {e}",
                )
            continue

        returned_ids_chunk = set()

        for it in items:
            cid = (it.get("id") or "").strip()
            if not cid:
                continue

            returned_ids_chunk.add(cid)

            snippet = it.get("snippet", {}) or {}
            stats = it.get("statistics", {}) or {}
            content_details = it.get("contentDetails", {}) or {}
            related = (content_details.get("relatedPlaylists", {}) or {})

            title = (snippet.get("title") or "").strip()
            custom_url = (snippet.get("customUrl") or "").strip()
            country = (snippet.get("country") or "").strip()
            channel_published_at = (snippet.get("publishedAt") or "").strip()
            uploads_playlist_id = (related.get("uploads") or "").strip()

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
                    snapshot_utc,
                    today_utc,
                    cid,
                    "NOT_FOUND",
                    "ID non retourné par l'API (typo, chaîne supprimée/privée, ou indisponible).",
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
