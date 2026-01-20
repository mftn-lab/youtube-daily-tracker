import os
import csv
import time
import re
from datetime import datetime, timezone
from typing import List, Tuple
import requests

# ------------------------------------------------------------
# YouTube Daily Tracker
#
# Objectif :
# - Récupérer chaque jour les statistiques publiques de chaînes YouTube (thématique Tech)
# - Stocker un historique "time-series" dans youtube_daily_snapshots.csv
# - Maintenir une table de référence dans channels_reference.csv (titre, URL, dernière vue)
#
# Notes :
# - Une ligne par chaîne et par jour (UTC) pour faire des analyses dans le temps
# - La liste des chaînes suivies est définie dans CHANNEL_IDS ci-dessous
#
# Sécurité :
# - La clé API doit être fournie via la variable d'environnement : YOUTUBE_API_KEY
# - Ne jamais mettre de secret en clair dans le dépôt
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
]

# Fichiers de sortie
DAILY_OUTFILE = "youtube_daily_snapshots.csv"
REF_OUTFILE = "channels_reference.csv"
LOGFILE = "run_log.txt"

# Limite YouTube API : max 50 IDs par requête
MAX_IDS_PER_REQUEST = 50

# Petit retry simple (utile si l'API répond temporairement mal)
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 2


def log(msg: str) -> None:
    # Écrit un log simple (utile pour déboguer sans se compliquer la vie)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{stamp}] {msg}"
    print(line)
    try:
        with open(LOGFILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        # Si le log ne marche pas, je ne casse pas le script pour ça
        pass


def safe_int(value, default: int = 0) -> int:
    # Convertit une valeur en entier
    # Utile car l'API renvoie parfois des nombres sous forme de string,
    # ou certains champs peuvent être absents (ex: abonnés masqués).
    try:
        return int(value)
    except Exception:
        return default


def chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    # Découpe une liste en paquets pour respecter les limites de l'API
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
    # Appel à l'API YouTube Data v3 pour récupérer :
    # - snippet : titre, infos visibles
    # - statistics : abonnés, vues, nb de vidéos
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
    # Pour éviter les doublons si le workflow est relancé dans la même journée,
    # on stocke les clés (date_utc, channel_id) déjà présentes dans le fichier.
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
    # Ajoute des lignes à un CSV (crée le fichier avec header s'il n'existe pas)
    file_exists = os.path.isfile(outfile)
    with open(outfile, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerows(rows)


def upsert_reference(outfile: str, ref_rows: dict) -> None:
    # Écrit / met à jour la table de référence :
    # channel_id -> titre -> url -> last_seen_utc
    # Si le fichier existe, on le charge puis on met à jour / ajoute les lignes.
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

    # Fusion : je mets à jour / ajoute
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
                "channel_url": existing[cid].get(
                    "channel_url", f"https://www.youtube.com/channel/{cid}"
                ),
                "last_seen_utc": existing[cid].get("last_seen_utc", ""),
            })


def main() -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    log("=== Début du run ===")
    log(f"Date du snapshot (UTC) : {today_utc}")

    # Validation des IDs (évite les erreurs bêtes)
    valid_ids, invalid_ids = validate_channel_ids(CHANNEL_IDS)

    # Sécurité optionnelle : supprime les doublons en conservant l'ordre
    valid_ids = list(dict.fromkeys(valid_ids))

    if invalid_ids:
        log(f"IDs invalides détectés (ignorés) : {invalid_ids}")

    if not valid_ids:
        raise RuntimeError("Aucun Channel ID valide. Renseigne au moins une chaîne (UC...).")

    # Anti-doublon : si une ligne existe déjà pour (today_utc, channel_id), on n'ajoute pas
    existing_keys = load_existing_daily_keys(DAILY_OUTFILE)

    # Découpage en paquets pour l'API (max 50 IDs par requête)
    chunks = chunk_list(valid_ids, MAX_IDS_PER_REQUEST)

    daily_rows_to_append = []
    ref_updates = {}
    returned_ids = set()

    for idx, chunk in enumerate(chunks, start=1):
        log(f"Appel API chunk {idx}/{len(chunks)} — {len(chunk)} chaînes")
        items = youtube_channels_api_call(chunk)

        # Transformation de la réponse en lignes propres
        for it in items:
            cid = it.get("id", "")
            returned_ids.add(cid)

            snippet = it.get("snippet", {})
            stats = it.get("statistics", {})

            title = snippet.get("title", "")
            url = f"https://www.youtube.com/channel/{cid}"

            # Abonnés : parfois YouTube masque (subscriberCount absent)
            subs = safe_int(stats.get("subscriberCount"), default=0)
            views = safe_int(stats.get("viewCount"), default=0)
            videos = safe_int(stats.get("videoCount"), default=0)

            # Anti-doublon journalier
            if (today_utc, cid) in existing_keys:
                continue

            daily_rows_to_append.append([today_utc, cid, title, subs, views, videos])

            ref_updates[cid] = {
                "channel_id": cid,
                "channel_title": title,
                "channel_url": url,
                "last_seen_utc": now_utc,
            }

    # IDs demandés mais non retournés (typo possible ou chaîne indisponible)
    missing = sorted(set(valid_ids) - returned_ids)
    if missing:
        log(f"Attention : ces IDs n'ont pas été retournés par l'API (à vérifier) : {missing}")

    # Écriture du CSV quotidien
    daily_header = ["date_utc", "channel_id", "channel_title", "subscribers", "views", "videos"]
    if daily_rows_to_append:
        append_rows_csv(DAILY_OUTFILE, daily_header, daily_rows_to_append)
        log(f"CSV daily mis à jour : +{len(daily_rows_to_append)} lignes")
    else:
        log("Aucune nouvelle ligne à ajouter (peut-être déjà collecté aujourd'hui).")

    # Écriture / mise à jour de la table de référence
    if ref_updates:
        upsert_reference(REF_OUTFILE, ref_updates)
        log(f"Référence mise à jour : {len(ref_updates)} chaînes (titre + URL)")
    else:
        log("Référence non modifiée (aucune chaîne retournée).")

    log("=== Fin du run ===")


if __name__ == "__main__":
    main()

