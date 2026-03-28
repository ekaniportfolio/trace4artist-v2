"""
src/worker.py — Configuration Celery et définition des tâches

Étape 3 : fetch_video_details branché sur youtube_client + searcher
Étape 4 : score_pending_artists sera branché sur scorer v2
Étape 6 : sync_to_hubspot sera branché sur hubspot_client
"""

from celery import Celery
from celery.utils.log import get_task_logger

from config import CELERY_BROKER_URL, CELERY_RESULT_BACKEND

logger = get_task_logger(__name__)

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


@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    name="tasks.fetch_video_details",
)
def fetch_video_details(self, video_ids: list, channel_ids: list, region: str):
    """
    Récupère et sauvegarde les détails d'un batch de vidéos + chaînes.
    Branché sur ArtistSearcher depuis l'Étape 3.
    """
    from src.youtube_client import YouTubeClient
    from src.searcher import ArtistSearcher

    try:
        client   = YouTubeClient()
        searcher = ArtistSearcher(client=client)

        logger.info(f"[{region}] fetch_video_details — {len(video_ids)} vidéo(s)")

        result = searcher.process_batch(
            video_ids   = video_ids,
            channel_ids = channel_ids,
            region      = region,
        )

        logger.info(
            f"[{region}] ✅ {result['saved_videos']} vidéos, "
            f"{result['new_artists']} nouveaux artistes"
        )
        return result

    except Exception as exc:
        logger.error(f"[{region}] Erreur : {exc}")
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


@celery_app.task(
    name="tasks.score_pending_artists",
    max_retries=2,
)
def score_pending_artists():
    """
    Score tous les artistes en statut 'discovered'.
    Branché sur ArtistScorer depuis l'Étape 4.
    """
    from src.scorer import ArtistScorer

    scorer  = ArtistScorer()
    results = scorer.score_all_discovered()

    qualified = sum(1 for r in results if r.is_qualified)
    logger.info(f"Scoring terminé : {qualified}/{len(results)} qualifiés")
    return {"total": len(results), "qualified": qualified}


@celery_app.task(
    name="tasks.enrich_artists",
    max_retries=2,
)
def enrich_artists():
    """
    Enrichit les profils d artistes via Google Custom Search.
    Branché sur GoogleSearchEnricher depuis l Étape 6.
    """
    from src.enricher import GoogleSearchEnricher
    enricher = GoogleSearchEnricher()
    results  = enricher.enrich_qualified_artists()
    enriched = sum(1 for r in results if r.success)
    logger.info(f"Enrichissement : {enriched}/{len(results)} artistes enrichis")
    return {"total": len(results), "enriched": enriched}


@celery_app.task(
    name="tasks.sync_to_hubspot",
    max_retries=3,
    default_retry_delay=120,
)
def sync_to_hubspot():
    """
    Synchronise les artistes qualifiés vers HubSpot CRM.
    Branché sur HubSpotClient depuis l Étape 6.
    """
    from src.hubspot_client import HubSpotClient
    try:
        client = HubSpotClient()
        synced = client.sync_qualified_artists()
        logger.info(f"HubSpot sync : {synced} contact(s) synchronisé(s)")
        return {"synced": synced}
    except ValueError as e:
        logger.warning(f"HubSpot non configuré : {e}")
        return {"synced": 0, "status": "not_configured"}