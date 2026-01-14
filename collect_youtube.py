import os
import csv
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple
import requests

# ------------------------------------------------------------
# YouTube Daily Tracker — version aboutie (style "Marco")
#
# Objectif :
# - Je récupère tous les jours les stats publiques de mes chaînes YouTube
# - Je stocke l'historique dans un CSV (youtube_daily_snapshots.csv)
# - Je garde aussi une table de référence (channels_reference.csv)
#
# Les 2 idées fortes :
# 1) La collecte "daily" = une ligne par chaîne et par jour (séries temporelles)
# 2) La table "reference" = les infos "stables" (titre, url, etc.) pour dashboards
#
# Sécurité :
# - Ma clé API est dans GitHub Secrets : YOUTUBE_API_KEY
# - Jamais en clair dans le code
# ------------------------------------------------------------

API_KEY = os.getenv("YOUTUBE_API_KEY")

# IMPORTANT : ici je mets uniquement des Channel IDs (format UC...)
CHANNEL_IDS = [
    "UCwsQtHFlS-LKVn8xJ3TaupQ",
    "UCWedHS9qKebauVIK2J7383g",
    "UCu6HC8ahaPWqqfSvic7nWyQ",
    "UCNOUTy_TWRJzAaSfIGserkg",
    "UCnEHCrot2HkySxMTmDPhZyg",
    "UCEclJUeQDYh8id50Jg1tyEg",
    "UCnnIP34CpT7nIBZS5KuuhbA",
    "UCW-7TvXaVBhtzCivzhTP5zA",
    "UCN6zUULSoitsLJB03jh_I0A",
    "UCo5lEwalJKL87lOPKJIo5cw",
    "UCljgvfi_NnUrNJVHp_HGHwA",
    "UCf9h3TSEiAiwubyWpLUtdwg",
    "UCbXcGh0FCyhK09m0Qj9pQRg",
    "UCUkcknjfu4gwMbGWLdcyZrA",
    "UCWtD8JN9hkxL5TJL_ktaNZA",
    "UCNew6vrycRR0QnlYmLoMRZg",
    "UCIUnPBs55STUL16VDQOHBWg",
    "UCbpNKWuYGQmOaa6zeE1p3PQ",
    "UCM2FcP5_y9L7s5abqrtiL7w",
    "UC4AGoh9ExPFMbjUVe6kyuqg",
]

# Fichiers de sortie
DAILY_OUTFILE = "youtube_daily_snapshots.csv"
REF_OUTFILE = "channels_reference.csv"
LOGFILE = "run_log.txt"

# YouTube API : max 50 IDs par requête (bonne pratique)
MAX_IDS_PER_REQUEST = 50

# Petit retry simple (utile si l'API répond temporairement mal)
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 2


def log(msg: str) -> None:
    """J'écris un log simple (utile pour debug sans prise de tête)."""
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{stamp}] {msg}"
    print(line)
    try:
        with open(LOGFILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        # si le log ne marche pas, je ne casse pas le script pour ça
        pass


def safe_int(value, default=0) -> int:
    """
    L'API renvoie souvent des nombres sous forme de string.
    Et parfois certains champs n'existent pas (ex: abonnés masqués).
    """
    try:
        return int(value)
    except Exception:
        return default


def chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    """Je découpe ma liste en paquets pour respecter les limites de l'API."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def validate_channel_ids(ids: List[str]) -> Tuple[List[str], List[str]]:
    """
    Je filtre les IDs pour éviter de faire des appels inutiles.
    Règle simple : un Channel ID commence par 'UC' et fait souvent 24 caractères environ,
    mais je ne bloque pas trop strictement.
    """
    valid = []
    invalid = []
    for cid in ids:
        c = cid.strip()
        if c.startswith("UC") and len(c) >= 10:
            valid.append(c)
        else:
            invalid.append(c)
    return valid, invalid


def youtube_channels_api_call(channel_ids: List[str]) -> List[Dict]:
    """
    Appel à l'API YouTube Data v3 pour récupérer :
    - snippet : titre, infos visibles
    - statistics : abonnés, vues, nb de vidéos
    """
    if not API_KEY:
        raise RuntimeError(
            "Je ne trouve pas la clé API. Je vérifie le secret GitHub 'YOUTUBE_API_KEY'."
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
            # Si quota ou erreur, raise_for_status va lever une exception
            r.raise_for_status()
            data = r.json()
            return data.get("items", [])
        except Exception as e:
            last_err = e
            log(f"API call failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)

    raise RuntimeError(f"Échec API après {MAX_RETRIES} tentatives : {last_err}")


def load_existing_daily_keys(outfile: str) -> set:
    """
    Pour éviter les doublons si je relance le workflow dans la même journée.
    Je stocke des clés (date_utc, channel_id) déjà présentes.
    """
    keys = set()
    if not os.path.isfile(outfile):
        return keys

    try:
        with open(outfile, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                keys.add((row.get("date_utc", ""), row.get("channel_id", "")))
    except Exception as e:
        log(f"Warning: impossible de lire {outfile} pour anti-doublon: {e}")
    return keys


def append_rows_csv(outfile: str, header: List[str], rows: List[List]) -> None:
    """Ajoute des lignes à un CSV (crée le fichier avec header s'il n'existe pas)."""
    file_exists = os.path.isfile(outfile)
    with open(outfile, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerows(rows)


def upsert_reference(outfile: str, ref_rows: Dict[str, Dict]) -> None:
    """
    J'écris une table de référence 'channel_id -> title -> url -> date_seen'.
    Si le fichier existe, je le charge et je le mets à jour.
    """
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
            log(f"Warning: impossible de lire {outfile} (reference): {e}")

    # merge : je mets à jour / ajoute
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


def main():
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    log("=== Start run ===")
    log(f"Date du snapshot (UTC): {today_utc}")

    # 1) Je valide la liste d'IDs pour éviter les erreurs bêtes
    valid_ids, invalid_ids = validate_channel_ids(CHANNEL_IDS)
    if invalid_ids:
        log(f"IDs invalides détectés (ignorés): {invalid_ids}")

    if not valid_ids:
        raise RuntimeError("Aucun Channel ID valide. Je dois renseigner au moins une chaîne (UC...).")

    # 2) Anti-doublon : si j'ai déjà une ligne pour (today_utc, channel_id), je n'ajoute pas
    existing_keys = load_existing_daily_keys(DAILY_OUTFILE)

    # 3) Je découpe en paquets pour l'API (max 50 IDs par requête)
    chunks = chunk_list(valid_ids, MAX_IDS_PER_REQUEST)

    daily_rows_to_append = []
    ref_updates = {}
    returned_ids = set()

    for idx, chunk in enumerate(chunks, start=1):
        log(f"Appel API chunk {idx}/{len(chunks)} — {len(chunk)} chaînes")
        items = youtube_channels_api_call(chunk)

        # 4) Je transforme la réponse en lignes “propres”
        for it in items:
            cid = it.get("id", "")
            returned_ids.add(cid)

            snippet = it.get("snippet", {})
            stats = it.get("statistics", {})

            title = snippet.get("title", "")
            # URL "universelle" à partir du channel_id
            url = f"https://www.youtube.com/channel/{cid}"

            # Abonnés : parfois YouTube masque (subscriberCount absent)
            subs = safe_int(stats.get("subscriberCount"), default=0)
            views = safe_int(stats.get("viewCount"), default=0)
            videos = safe_int(stats.get("videoCount"), default=0)

            # Anti-doublon : si déjà présent pour aujourd'hui, je saute
            if (today_utc, cid) in existing_keys:
                continue

            daily_rows_to_append.append([today_utc, cid, title, subs, views, videos])

            ref_updates[cid] = {
                "channel_id": cid,
                "channel_title": title,
                "channel_url": url,
                "last_seen_utc": now_utc,
            }

    # 5) Je détecte les IDs demandés mais non retournés (typo possible)
    missing = sorted(set(valid_ids) - returned_ids)
    if missing:
        log(f"Attention: ces IDs n'ont pas été retournés par l'API (à vérifier): {missing}")

    # 6) J'écris le CSV quotidien
    daily_header = ["date_utc", "channel_id", "channel_title", "subscribers", "views", "videos"]
    if daily_rows_to_append:
        append_rows_csv(DAILY_OUTFILE, daily_header, daily_rows_to_append)
        log(f"CSV daily mis à jour : +{len(daily_rows_to_append)} lignes")
    else:
        log("Aucune nouvelle ligne à ajouter (peut-être déjà collecté aujourd'hui).")

    # 7) J'écris / mets à jour la table de référence
    if ref_updates:
        upsert_reference(REF_OUTFILE, ref_updates)
        log(f"Reference mise à jour : {len(ref_updates)} chaînes (title + url)")
    else:
        log("Reference non modifiée (aucune chaîne retournée).")

    log("=== End run ===")


if __name__ == "__main__":
    main()
