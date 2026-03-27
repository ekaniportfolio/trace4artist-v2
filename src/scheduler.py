"""
src/scheduler.py — Orchestrateur principal via APScheduler

Le scheduler se réveille toutes les 6h et déclenche le pipeline :
    1. Recherche YouTube → envoie des tâches Celery (fetch_video_details)
    2. Lance le scoring  (score_pending_artists) après 2 minutes
    3. Lance la sync HubSpot (sync_to_hubspot) après 5 minutes

Logique de date de recherche :
    - Premier scan    : NOW() - INITIAL_LOOKBACK_DAYS (1 an)
    - Scans suivants  : date de fin du dernier scan réussi

Étape 3 : _scan_region branché sur ArtistSearcher.search_region()
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
)
from src.database import get_last_scan_date, get_quota_used_today, get_db
from src.youtube_client import QuotaExceededError
from src.searcher import ArtistSearcher
from src.worker import fetch_video_details, score_pending_artists, sync_to_hubspot
 
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scheduler")
 
 
class ScanOrchestrator:
 
    def __init__(self):
        self.searcher = ArtistSearcher()
 
    def run(self):
        logger.info("=" * 55)
        logger.info("  Démarrage du scan")
        logger.info("=" * 55)
 
        published_after = self._get_published_after()
        logger.info(f"  Recherche depuis : {published_after}")
 
        scan_id     = self._start_scan_log()
        quota_before = get_quota_used_today()
        quota_remaining = DAILY_QUOTA_LIMIT - quota_before
 
        if quota_remaining < QUOTA_COST["search.list"]:
            logger.warning("Quota insuffisant. Scan annulé.")
            self._end_scan_log(scan_id, status="failed", error="Quota insuffisant")
            return
 
        total_videos = 0
        total_tasks  = 0
 
        for region in TARGET_REGIONS:
            if DAILY_QUOTA_LIMIT - get_quota_used_today() < QUOTA_COST["search.list"]:
                logger.warning(f"Quota épuisé, arrêt avant {region}")
                break
 
            videos, tasks = self._scan_region(region, published_after)
            total_videos += videos
            total_tasks  += tasks
 
        logger.info(f"  Vidéos trouvées : {total_videos}")
        logger.info(f"  Tâches Celery   : {total_tasks} batch(es)")
 
        score_pending_artists.apply_async(countdown=120)
        sync_to_hubspot.apply_async(countdown=300)
 
        self._end_scan_log(
            scan_id,
            status       = "completed",
            videos_found = total_videos,
            quota_used   = get_quota_used_today() - quota_before,
        )
        logger.info("Scan terminé")
 
    def _scan_region(self, region: str, published_after: str) -> tuple[int, int]:
        """
        Recherche les vidéos d'un pays et envoie les batches à Celery.
        Branché sur ArtistSearcher.search_region() depuis l'Étape 3.
        """
        logger.info(f"🌍 [{region}] Recherche...")
 
        try:
            video_ids, channel_ids = self.searcher.search_region(
                region          = region,
                published_after = published_after,
            )
        except QuotaExceededError:
            logger.warning(f"[{region}] Quota dépassé")
            return 0, 0
        except Exception as e:
            logger.error(f"[{region}] Erreur : {e}")
            return 0, 0
 
        if not video_ids:
            logger.info(f"[{region}] Aucun résultat")
            return 0, 0
 
        # Envoyer en batches de 50 à Celery
        batch_size = 50
        tasks_sent = 0
 
        for i in range(0, len(video_ids), batch_size):
            fetch_video_details.delay(
                video_ids   = video_ids[i:i + batch_size],
                channel_ids = channel_ids[i:i + batch_size],
                region      = region,
            )
            tasks_sent += 1
 
        logger.info(f"[{region}] {len(video_ids)} vidéos → {tasks_sent} batch(es)")
        return len(video_ids), tasks_sent
 
    def _get_published_after(self) -> str:
        last_scan = get_last_scan_date()
        if last_scan:
            return last_scan
        start_date = datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)
        return start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
 
    def _start_scan_log(self) -> int:
        from sqlalchemy import text
        with get_db() as conn:
            result = conn.execute(text("""
                INSERT INTO scan_logs (scan_type, status)
                VALUES ('incremental', 'running')
                RETURNING id
            """))
            return result.scalar()
 
    def _end_scan_log(self, scan_id: int, status: str, **kwargs):
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
 