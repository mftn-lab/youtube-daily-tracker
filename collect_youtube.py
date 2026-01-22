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
# + Monitoring (ajout) :
# - Journal d'erreurs daily dans data/daily/errors_daily.csv
#   -> FORMAT_INVALID / NOT_FOUND / API_ERROR
# ------------------------------------------------------------

API_KEY = os.getenv("YOUTUBE_API_KEY")

# IMPORTANT : ici je mets uniquement des Channel IDs (format UC...)
CHANNEL_IDS = [
    "UCGMxP4yf3ixAAEKQc3d76gw",
    "UC1wV5Kf18gVbL3bWH-tWKDw",
    "UC7XxmEXw7wXgh259vYCSjkQ",
    "UCZEdNvDcHUPEE7CDRpXnO6w",
    "UCZ5qxuvquQABNtjcawNMowA",
    "UCxpT79bmhxk50l8P50WG86w",
    "UCrSCIeCW1YuEviVZPVhEfGQ",
    "UCOyUCj6Wx9skJggzZFUDZyA",
    "UCFrfK9IFqO15S7C52DdnSog",
    "UCJy0lX8ThZ7lCtst7JnegWQ",
    "UCljgvfi_NnUrNJVHp_HGHwA",
    "UCqRSWOqkW_oelrdUN7KQ-aQ",
    "UCaHolOYkfcTdC0vTFAywzXw",
    "UCDfGCQqgOKPYeiaG3I4vl4w",
    "UCTOAKgwfqqCbOytF3RNGGVw",
    "UCwsQtHFlS-LKVn8xJ3TaupQ",
    "UCu6HC8ahaPWqqfSvic7nWyQ",
    "UCC5xS4j2xUyGO2UfjFh5LsQ",
    "UCNOUTy_TWRJzAaSfIGserkg",
    "UCgjhIdz3Uw9r-eMyML9M8FA",
    "UCPQsSFJurluaMOG1qL0Y7sA",
    "UCfLyhjpn3hzleNtgTGOQimA",
    "UC-pdFydi9ooxEfATh3KoiIw",
    "UCaybrunQi8xWgPMgv1AYBHw",
    "UC4q3HKdMqzll3SvA2Lf7oAg",
    "UC6joDErOGcFJC1vHNSfsYQg",
    "UCoJ4gzwVvGNH5GNqiJoQAAA",
    "UC_dzE9mydhQuQ0NwmXByDtA",
    "UCm3QK_QNta9Mue2Q70gtzVw",
    "UCQsb5TqYUeGWGpJFr8DMWsQ",
    "UCe7R7Pt-bxNT7EE83NLsmFQ",
    "UCXJ_vW2t0W5hY9hS8ePnpzw",
    "UCQZa27_VXLUowoMsYo6Y0Dw",
    "UCZsEJw8rwd0RWAga5dulOpg",
    "UCiR9chjo9kIFMsCXgspFxqg",
    "UCGjKSSIpN7aYQluu04kPrsA",
    "UCflWJq4cTuofmM-0G-GMUNQ",
    "UCrsiHZzr4IjuSjnvPy5rVUQ",
    "UCuDNiVxL-Zfw0CBVQ4oOr8w",
    "UCTmHRe6W96WgYlUNxcokpVQ",
    "UCkS6jlQ4RJTCdkzVCU2-FSQ",
    "UCSYFVeBI66YqDhGrYKxiCUQ",
    "UCBqYyDusSFntU7k2UnFGsBA",
    "UCCUlTcDhcn72O8TiNfFJWWA",
    "UC1N4EIilrmXhTEdu3h7tpPg",
    "UCA_DfRJwb5U0S4UiiH3BtUg",
    "UC2lMzO7HzJ_jamFT762vfwA",
    "UCQigF3tZJJlt6o1_tkYKkNQ",
    "UCYmw-Y8TnRzO4COTBKEgUGg",
    "UCBOGe_x7aqQV4Ly5Rfa4iow",
    "UCfbwndxvM8tOtxN_GqU1R3Q",
    "UCySZIa8Y4e6aSVg4CISXAOA",
    "UCdqC1GnhKs3aQw6j62uOvUw",
    "UCX4A8obrGg1iX37R-fyBJ6w",
    "UCUD3E77YRTGK3yPSDB6Kq5Q",
    "UCVrceZhsV6TRyVh3VhBFaKA",
    "UCUltPy138RvnbWX_FuUHtDA",
    "UCwZsbPUXohCN867pQzioGLw",
    "UCc7giV0qHsCNbhjV3_ZJ22w",
    "UCf-DN6QjyyEbhuMWCoXdXQQ",
    "UCB32y6dm508Hap4_Mn-ZnHQ",
    "UCB0ffaItcXwkgbVu8Hvq3Tw",
    "UCtPzJInyqqt__Tn0EaUlaAw",
    "UClQPWod8Gjgw7i51t7PYibQ",
    "UC5HtcZa4-moyfy6NTY7GHwA",
    "UCs7Jg5KBsgiova4CrseuwIw",
    "UCUT9pQQGw4ZEpv5hqrv-bZw",
    "UCUclUivgBePqnqDGQ3Jzu2Q",
    "UC2HXhqg87yIWz152Ig9vkaQ",
    "UCNzUonmxxvG-HIqOC3S7Thw",
    "UCagQpz-WZtgbSp8Ry0XQ4Yw",
    "UCUbwQOAmLdZ9zPOOPFt25zQ",
    "UClnRNkdVq24iwIbc1O-hjlw",
    "UC69qClfvKeB9WeyySg5iQrQ",
    "UCpaI1FRY8EGK6mEAL7Wfr9w",
    "UCEPqgSzHj7vlJj5pGX9dDdA",
    "UCOtM8pxxBIfwqu6e8mHgUEQ",
    "UCJ4p_F-vCnEn5V5OtFN8Q3w",
    "UCJ3MlRSjtv2sT-rIDsFg0fw",
    "UCRI5foOTNz6DuVz3SVQ1cMA",
    "UCb0sWM7tMRaEgn_hpptqnxA",
    "UCssuk5-zaZn9vt0_HNj-Hug",
    "UCUanZckNyJODHBdqtMzYMpA",
    "UCzICpbabqKwBTpb9XlWerjA",
    "UCa8Leo9o5Nnb-Cc1sWfT2HQ",
    "UCM6kUx3GI7lTQlqMBiumaEg",
    "UC65OszX3C9oYfNLU5AWjwYg",
    "UC1WIj7YGywcVoRLS0zZu2RQ",
    "UCR-4Ge4NbqILG5HTTvs_nFQ",
    "UCJPa4sr9B_mLjShfHhWi8yg",
    "UC_FckuJiPXwA_nOEIoTsRZg",
    "UCQvNFAufGf8mtN2_L8thysg",
    "UCMWAxO0xGJxELkiX5_pEJNA",
    "UCU8_onYYhNxwGUB3hA34C4Q",
    "UC0AFqXyYs1z2Dxx5jtQBicQ",
    "UCrwtkeoBxPfhyde5eesmvyA",
    "UCP5-zNhWXk_LUPIJpxtrTnQ",
    "UCDBYphtxNDXGQe5eyHYZ6Fw",
    "UC7rdUip5Q8R7Ki1haNbdwwg",
    "UCuE3ei3NOnVjQUefjHUCF5Q",
    "UCAOjk3Po21_t7L1MDHQ4-Og",
    "UCA25ZdZZF5XE6ihPE5pwfnw",
    "UCTER7XOnDvSRbI2YxikLaVw",
    "UCMc7IaTDRDtxftK9v0aXVqA",
    "UCU_a5aoj_HzTMohx0JG_Z4A",
    "UC3-dl8nKItitMxM9DNTDAfQ",
    "UCHJb6-VCrzQqVQASN23OskQ",
    "UC69gruQfEHEQ4Qme0liJOdw",
    "UCOm_kuKVb4ImX_DHpIgX_zg",
    "UCDY3WI0HqdzNADtE6B-BmUw",
    "UCpFq62uOig4n9jT3luIYUDQ",
    "UCIaNSaws_Wv_eoUAmDbUK-Q",
    "UCOaX4zvPzmv0eBbUWOOOB_A",
    "UCURjynFkZicGPbIIaWO9vmQ",
    "UCHGvOgxAA_MFOH1yyhjcR-Q",
    "UCz6PEeVLG1TL6jMRTvSLm4g",
    "UCbwE_k9zALnzZRvkoyDhuzw",
    "UCr7ZVUNJ9KyYD92xqfinGsQ",
    "UC6jfhJWEAfako0XlKnA2UGQ",
    "UCJLKCYMkCbOW9wzmeUj8dwQ",
    "UCrAiXFptzDmgQA6gUCOz9RQ",
    "UCchpwieM3DswmDEnJ0MRq1A",
    "UCNWE4ouC-BxzJ-Lomuuqb1Q",
    'UCR3uoYsbRO_S0wg2DaSlb8g',
    "UCPP-NkzSqMJ9ywhJkcgrpAw",
    "UCrHCRJPuJJcINMMEETEYt4g",
    "UC70eSjnRIGvRLCzT6K0SQSg",
    "UCpSRz-eAoiSW5kaHpI_3LjA",
    "UCQSA6pCffm7epqxFqouL0Xg",
    "UCOCoKBUmALR6d7UW_s5zgrA",
]

# Fichiers de sortie
DAILY_OUTFILE = "youtube_daily_snapshots.csv"
REF_OUTFILE = "channels_reference.csv"
LOGFILE = "run_log.txt"

# Monitoring des erreurs daily
DATA_DAILY_DIR = Path("data") / "daily"
ERRORS_DAILY_CSV = DATA_DAILY_DIR / "errors_daily.csv"
ERRORS_DAILY_HEADER = ["snapshot_utc", "date_utc", "channel_id", "error_type", "message"]

# Limite YouTube API : max 50 IDs par requête
MAX_IDS_PER_REQUEST = 50

# Petit retry simple (utile si l'API répond temporairement mal)
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 2


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
    """
    Crée data/daily/errors_daily.csv avec entête si absent,
    même s'il n'y a aucune erreur pendant le run.
    """
    ensure_daily_dir()
    if not ERRORS_DAILY_CSV.exists():
        with open(ERRORS_DAILY_CSV, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(ERRORS_DAILY_HEADER)


def append_error_daily(snapshot_utc: str, date_utc: str, channel_id: str, error_type: str, message: str) -> None:
    ensure_daily_dir()
    file_exists = ERRORS_DAILY_CSV.exists()
    with open(ERRORS_DAILY_CSV, "a", newline="", encoding="utf-8-sig") as f:
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


def youtube_channels_api_call(channel_ids: List[str]) -> List[dict]:
    if not API_KEY:
        raise RuntimeError(
            "Je ne trouve pas la clé API. Vérifie le secret / la variable d'environnement 'YOUTUBE_API_KEY'."
        )

    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "snippet,statistics",
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
    keys: Set[Tuple[str, str]] = set()
    if not os.path.isfile(outfile):
        return keys

    try:
        with open(outfile, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                keys.add((row.get("date_utc", ""), row.get("channel_id", "")))
    except Exception as e:
        log(f"Warning : impossible de lire {outfile} pour anti-doublon : {e}")
    return keys


def append_rows_csv(outfile: str, header: List[str], rows: List[List]) -> None:
    file_exists = os.path.isfile(outfile)
    with open(outfile, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerows(rows)


def upsert_reference(outfile: str, ref_rows: dict) -> None:
    existing = {}

    if os.path.isfile(outfile):
        try:
            with open(outfile, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = row.get("channel_id", "")
                    if cid:
                        existing[cid] = row
        except Exception as e:
            log(f"Warning : impossible de lire {outfile} (reference) : {e}")

    for cid, data in ref_rows.items():
        existing[cid] = data

    header = ["channel_id", "channel_title", "channel_url", "last_seen_utc"]
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for cid in sorted(existing.keys()):
            writer.writerow({
                "channel_id": existing[cid].get("channel_id", cid),
                "channel_title": existing[cid].get("channel_title", ""),
                "channel_url": existing[cid].get("channel_url", f"https://www.youtube.com/channel/{cid}"),
                "last_seen_utc": existing[cid].get("last_seen_utc", ""),
            })


def main() -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    snapshot_utc = datetime.now(timezone.utc).isoformat()

    log("=== Début du run ===")
    log(f"Date du snapshot (UTC) : {today_utc}")

    # ✅ Création du fichier d'erreurs dès le début (même si aucune erreur)
    init_errors_daily_file()

    valid_ids, invalid_ids = validate_channel_ids(CHANNEL_IDS)
    valid_ids = list(dict.fromkeys(valid_ids))

    if invalid_ids:
        log(f"IDs invalides détectés (ignorés) : {invalid_ids}")
        for bad in invalid_ids:
            append_error_daily(snapshot_utc, today_utc, bad, "FORMAT_INVALID", "Channel ID format invalide (typo probable).")

    if not valid_ids:
        raise RuntimeError("Aucun Channel ID valide. Renseigne au moins une chaîne (UC...).")

    existing_keys = load_existing_daily_keys(DAILY_OUTFILE)
    chunks = chunk_list(valid_ids, MAX_IDS_PER_REQUEST)

    daily_rows_to_append = []
    ref_updates = {}
    returned_ids = set()

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

        for it in items:
            cid = it.get("id", "")
            if not cid:
                continue

            returned_ids.add(cid)

            snippet = it.get("snippet", {})
            stats = it.get("statistics", {})

            title = snippet.get("title", "")
            url = f"https://www.youtube.com/channel/{cid}"

            subs = safe_int(stats.get("subscriberCount"), default=0)
            views = safe_int(stats.get("viewCount"), default=0)
            videos = safe_int(stats.get("videoCount"), default=0)

            # ✅ Toujours mettre à jour la référence si la chaîne est retournée
            ref_updates[cid] = {
                "channel_id": cid,
                "channel_title": title,
                "channel_url": url,
                "last_seen_utc": now_utc,
            }

            # Anti-doublon journalier (uniquement pour le daily CSV)
            if (today_utc, cid) in existing_keys:
                continue

            daily_rows_to_append.append([today_utc, cid, title, subs, views, videos])

        # IDs demandés mais non retournés sur ce chunk
        returned_ids_chunk = {it.get("id", "") for it in items if it.get("id")}
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

    # ✅ Référence mise à jour même si aucune nouvelle ligne daily (run relancé)
    if ref_updates:
        upsert_reference(REF_OUTFILE, ref_updates)
        log(f"Référence mise à jour : {len(ref_updates)} chaînes (titre + URL)")
    else:
        log("Référence non modifiée (aucune chaîne retournée).")

    log("=== Fin du run ===")


if __name__ == "__main__":
    main()
