"""
src/scheduler.py — Orchestrateur principal via APScheduler

Le scheduler se réveille toutes les 6h et déclenche le pipeline :
    1. Recherche YouTube → envoie des tâches Celery (fetch_video_details)
    2. Lance le scoring  (score_pending_artists) après 2 minutes
    3. Lance la sync HubSpot (sync_to_hubspot) après 5 minutes

Logique de date de recherche :
    - Premier scan    : NOW() - INITIAL_LOOKBACK_DAYS (1 an)
    - Scans suivants  : date de fin du dernier scan réussi

NOTE : _scan_region() est un stub jusqu'à l'Étape 3 où on
branchera youtube_client et searcher.
"""

import logging
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import (
    SCAN_INTERVAL_HOURS,
    INITIAL_LOOKBACK_DAYS,
    TARGET_REGIONS,
    QUOTA_COST,
    DAILY_QUOTA_LIMIT,
    SEARCH_KEYWORDS,
)
from src.database import get_last_scan_date, get_quota_used_today, get_db
from src.worker import fetch_video_details, score_pending_artists, sync_to_hubspot

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scheduler")


class ScanOrchestrator:
    """
    Orchestre un cycle de scan complet.

    _scan_region() est un stub jusqu'à l'Étape 3.
    À l'Étape 3, on y importera YouTubeClient et on fera
    le vrai appel search.list.
    """

    def run(self):
        """Point d'entrée appelé par le scheduler toutes les 6h."""
        logger.info("=" * 55)
        logger.info("  Démarrage du scan")
        logger.info("=" * 55)

        published_after = self._get_published_after()
        logger.info(f"  Recherche depuis : {published_after}")

        scan_id = self._start_scan_log(published_after)

        quota_used      = get_quota_used_today()
        quota_remaining = DAILY_QUOTA_LIMIT - quota_used
        logger.info(f"  Quota restant   : {quota_remaining} unités")

        if quota_remaining < QUOTA_COST["search.list"]:
            logger.warning("Quota insuffisant. Scan annulé.")
            self._end_scan_log(scan_id, status="failed",
                               error="Quota insuffisant")
            return

        total_videos = 0
        total_tasks  = 0

        for region in TARGET_REGIONS:
            quota_remaining = DAILY_QUOTA_LIMIT - get_quota_used_today()
            if quota_remaining < QUOTA_COST["search.list"]:
                logger.warning(f"Quota épuisé, arrêt avant {region}")
                break

            videos, tasks = self._scan_region(region, published_after)
            total_videos += videos
            total_tasks  += tasks

        logger.info(f"  Vidéos trouvées : {total_videos}")
        logger.info(f"  Tâches Celery   : {total_tasks} batch(es)")

        # Déclencher scoring et sync avec délai
        score_pending_artists.apply_async(countdown=120)
        sync_to_hubspot.apply_async(countdown=300)

        self._end_scan_log(
            scan_id,
            status       = "completed",
            videos_found = total_videos,
            quota_used   = get_quota_used_today() - quota_used,
        )
        logger.info("Scan terminé")

    def _scan_region(self, region: str, published_after: str) -> tuple[int, int]:
        """
        Recherche les vidéos pour un pays et envoie les tâches Celery.

        TODO Étape 3 : remplacer le stub par le vrai appel YouTubeClient
        """
        logger.info(f"[{region}] _scan_region [stub — Étape 3]")
        return 0, 0

    def _get_published_after(self) -> str:
        """
        Calcule la date de départ de recherche :
        - Premier scan  : NOW() - INITIAL_LOOKBACK_DAYS
        - Scans suivants: date du dernier scan réussi
        """
        last_scan = get_last_scan_date()

        if last_scan:
            logger.info(f"  Mode : incrémental (depuis {last_scan})")
            return last_scan

        start_date = datetime.now(timezone.utc) - timedelta(
            days=INITIAL_LOOKBACK_DAYS
        )
        logger.info(f"  Mode : initial ({INITIAL_LOOKBACK_DAYS}j de lookback)")
        return start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _start_scan_log(self, published_after: str) -> int:
        """Crée une entrée scan_logs et retourne son ID."""
        from sqlalchemy import text
        with get_db() as conn:
            result = conn.execute(text("""
                INSERT INTO scan_logs (scan_type, status)
                VALUES ('incremental', 'running')
                RETURNING id
            """))
            return result.scalar()

    def _end_scan_log(self, scan_id: int, status: str, **kwargs):
        """Met à jour le log de scan avec les résultats."""
        from sqlalchemy import text
        with get_db() as conn:
            conn.execute(text("""
                UPDATE scan_logs SET
                    status        = :status,
                    videos_found  = :videos_found,
                    quota_used    = :quota_used,
                    error_message = :error,
                    completed_at  = NOW()
                WHERE id = :scan_id
            """), {
                "scan_id"     : scan_id,
                "status"      : status,
                "videos_found": kwargs.get("videos_found", 0),
                "quota_used"  : kwargs.get("quota_used", 0),
                "error"       : kwargs.get("error"),
            })


def start_scheduler():
    """Démarre le scheduler en mode bloquant."""
    orchestrator = ScanOrchestrator()
    scheduler    = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        func               = orchestrator.run,
        trigger            = IntervalTrigger(hours=SCAN_INTERVAL_HOURS),
        id                 = "main_scan",
        name               = f"Scan YouTube toutes les {SCAN_INTERVAL_HOURS}h",
        misfire_grace_time = 300,
        replace_existing   = True,
    )

    logger.info(f"Scheduler démarré — scan toutes les {SCAN_INTERVAL_HOURS}h")
    orchestrator.run()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler arrêté.")
        scheduler.shutdown()


if __name__ == "__main__":
    start_scheduler()