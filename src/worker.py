"""
src/worker.py — Configuration Celery et définition des tâches

Les imports des modules métier (youtube_client, searcher, scorer,
hubspot_client) sont faits À L'INTÉRIEUR de chaque tâche, au moment
de leur exécution. Cela évite deux problèmes :
    1. Les imports circulaires au démarrage de Celery
    2. Les ImportError sur des modules pas encore créés

Les tâches sont des stubs pour l'instant — elles seront complétées
au fur et à mesure que les modules seront créés aux étapes suivantes.
"""

from celery import Celery
from celery.utils.log import get_task_logger

from config import CELERY_BROKER_URL, CELERY_RESULT_BACKEND

logger = get_task_logger(__name__)

# ── Initialisation de l'app Celery ─────────────────────────────────────
celery_app = Celery(
    "trace4artist",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer            = "json",
    result_serializer          = "json",
    accept_content             = ["json"],
    timezone                   = "UTC",
    enable_utc                 = True,
    task_acks_late             = True,
    task_reject_on_worker_lost = True,
    worker_prefetch_multiplier = 4,
    result_expires             = 3600,
)


# ──────────────────────────────────────────────────────────────────────
# TÂCHE 1 : fetch_video_details
# Complétée à l'Étape 3 (youtube_client + searcher)
# ──────────────────────────────────────────────────────────────────────

@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    name="tasks.fetch_video_details",
)
def fetch_video_details(self, video_ids: list, channel_ids: list, region: str):
    """
    Récupère les détails d'un batch de vidéos + chaînes YouTube
    et les sauvegarde en base.

    TODO Étape 3 : brancher youtube_client + searcher + database
    """
    logger.info(
        f"[{region}] fetch_video_details — "
        f"{len(video_ids)} vidéo(s) [stub — Étape 3]"
    )
    return {
        "region"      : region,
        "saved_videos": 0,
        "new_artists" : 0,
        "status"      : "stub",
    }


# ──────────────────────────────────────────────────────────────────────
# TÂCHE 2 : score_pending_artists
# Complétée à l'Étape 4 (scorer v2)
# ──────────────────────────────────────────────────────────────────────

@celery_app.task(
    name="tasks.score_pending_artists",
    max_retries=2,
)
def score_pending_artists():
    """
    Score tous les artistes en statut 'discovered'.

    TODO Étape 4 : brancher scorer v2
    """
    logger.info("score_pending_artists [stub — Étape 4]")
    return {"total": 0, "qualified": 0, "status": "stub"}


# ──────────────────────────────────────────────────────────────────────
# TÂCHE 3 : sync_to_hubspot
# Complétée à l'Étape 6 (hubspot_client)
# ──────────────────────────────────────────────────────────────────────

@celery_app.task(
    name="tasks.sync_to_hubspot",
    max_retries=3,
    default_retry_delay=120,
)
def sync_to_hubspot():
    """
    Synchronise les artistes qualifiés vers HubSpot CRM.

    TODO Étape 6 : brancher hubspot_client
    """
    logger.info("sync_to_hubspot [stub — Étape 6]")
    return {"synced": 0, "status": "stub"}