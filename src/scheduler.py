"""
src/scheduler.py — Scheduler Tiered Tracking v2

4 jobs APScheduler avec fréquences différentes :

    Job 1 : detect_new_videos()
        → Quotidien à l'heure configurée (défaut 00h00 UTC)
        → search.list sur tous les pays → nouvelles vidéos en base
        → Lance aussi update_all_phases() pour reclassifier les vidéos

    Job 2 : monitor_intensive()
        → Toutes les N heures (défaut 6h)
        → videos.list sur les vidéos en phase 'intensive' (< 7j)
        → Snapshots enrichis → calcul vélocité 24h

    Job 3 : monitor_growth()
        → Hebdomadaire (lundi 01h00 UTC)
        → videos.list sur les vidéos en phase 'growth' (7-90j)
        → Détection Long Tail et breakouts

    Job 4 : monitor_passive()
        → Mensuel (1er du mois, 02h00 UTC)
        → videos.list sur les vidéos en phase 'passive' (90-180j)
        → Veille minimale, artistes qualifiés inclus
"""

import logging
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import QUOTA_COST, DAILY_QUOTA_LIMIT
from src.database import (
    get_last_scan_date, get_quota_used_today,
    get_db, save_view_snapshot_enriched, save_alert,
)
from src.youtube_client import YouTubeClient, QuotaExceededError
from src.searcher import ArtistSearcher
from src.phase_manager import PhaseManager
from src.settings_manager import SettingsManager
from src.worker import (
    fetch_video_details, score_pending_artists,
    enrich_artists, sync_to_hubspot
)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scheduler")


# ──────────────────────────────────────────────────────────────────────────────
# JOB 1 — DÉTECTION QUOTIDIENNE
# ──────────────────────────────────────────────────────────────────────────────

class DetectionJob:
    """
    Détecte les nouvelles vidéos publiées depuis le dernier scan.
    Utilise search.list (100 unités/pays).
    Lance aussi la reclassification des phases.
    """

    def __init__(self):
        self.searcher = ArtistSearcher()
        self.settings = SettingsManager()
        self.phases   = PhaseManager()

    def run(self):
        logger.info("=" * 55)
        logger.info("  [Job 1] Détection quotidienne")
        logger.info("=" * 55)

        regions         = self.settings.get_regions()
        published_after = self._get_published_after()

        logger.info(f"  Pays         : {', '.join(regions)}")
        logger.info(f"  Depuis       : {published_after}")

        scan_id      = self._start_scan_log("detection")
        quota_before = get_quota_used_today()

        if DAILY_QUOTA_LIMIT - quota_before < QUOTA_COST["search.list"]:
            logger.warning("Quota insuffisant — détection annulée")
            self._end_scan_log(scan_id, "failed", error="Quota insuffisant")
            return

        total_videos = 0
        total_tasks  = 0

        for region in regions:
            remaining = DAILY_QUOTA_LIMIT - get_quota_used_today()
            if remaining < QUOTA_COST["search.list"]:
                logger.warning(f"Quota épuisé, arrêt avant {region}")
                break

            videos, tasks = self._scan_region(region, published_after)
            total_videos += videos
            total_tasks  += tasks

        logger.info(f"  Nouvelles vidéos : {total_videos}")
        logger.info(f"  Batches Celery   : {total_tasks}")

        # Reclassifier les phases APRÈS avoir ajouté les nouvelles vidéos
        logger.info("  Mise à jour des phases de tracking...")
        transitions = self.phases.update_all_phases()
        logger.info(f"  Transitions : {transitions}")

        # Séquence post-détection
        # 2min → scoring | 10min → enrichissement | 15min → HubSpot
        score_pending_artists.apply_async(countdown=120)
        enrich_artists.apply_async(countdown=600)
        sync_to_hubspot.apply_async(countdown=900)

        self._end_scan_log(
            scan_id, "completed",
            videos_found=total_videos,
            quota_used=get_quota_used_today() - quota_before,
        )

    def _scan_region(self, region: str, published_after: str) -> tuple[int, int]:
        try:
            video_ids, channel_ids = self.searcher.search_region(
                region          = region,
                published_after = published_after,
                max_results     = self.settings.get_max_results(),
                keywords        = self.settings.get_keywords(),
            )
        except QuotaExceededError:
            logger.warning(f"[{region}] Quota dépassé")
            return 0, 0
        except Exception as e:
            logger.error(f"[{region}] Erreur : {e}")
            return 0, 0

        if not video_ids:
            return 0, 0

        tasks_sent = 0
        for i in range(0, len(video_ids), 50):
            fetch_video_details.delay(
                video_ids   = video_ids[i:i+50],
                channel_ids = channel_ids[i:i+50],
                region      = region,
            )
            tasks_sent += 1

        logger.info(f"[{region}] {len(video_ids)} vidéos → {tasks_sent} batch(es)")
        return len(video_ids), tasks_sent

    def _get_published_after(self) -> str:
        from datetime import timedelta
        last_scan = get_last_scan_date()
        if last_scan:
            return last_scan
        lookback   = self.settings.get_lookback_days()
        start_date = datetime.now(timezone.utc) - timedelta(days=lookback)
        return start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _start_scan_log(self, scan_type: str) -> int:
        from sqlalchemy import text
        with get_db() as conn:
            result = conn.execute(text("""
                INSERT INTO scan_logs (scan_type, status)
                VALUES (:scan_type, 'running') RETURNING id
            """), {"scan_type": scan_type})
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


# ──────────────────────────────────────────────────────────────────────────────
# JOB 2 — MONITORING INTENSIF (< 7 jours)
# ──────────────────────────────────────────────────────────────────────────────

class IntensiveMonitoringJob:
    """
    Surveille les vidéos récentes (< 7j) toutes les 6h.
    C'est ici qu'on calcule la vélocité 24h et détecte
    les vidéos qui "cassent" l'algorithme.
    Utilise videos.list (1 unité pour 50 vidéos).
    """

    def __init__(self):
        self.client   = YouTubeClient()
        self.phases   = PhaseManager()
        self.settings = SettingsManager()

    def run(self):
        logger.info("─" * 55)
        logger.info("  [Job 2] Monitoring intensif (< 7j)")

        videos = self.phases.get_phase_videos("intensive")
        if not videos:
            logger.info("  Aucune vidéo en phase intensive")
            return

        logger.info(f"  Vidéos à monitorer : {len(videos)}")
        self._snapshot_batch(videos)

    def _snapshot_batch(self, videos: list):
        """Récupère les stats actuelles et enregistre les snapshots."""
        video_ids  = [v["video_id"] for v in videos]
        videos_map = {v["video_id"]: v for v in videos}

        # Traitement par batches de 50 (limite YouTube)
        breakout_threshold = float(
            self.settings.get("tracking.breakout_threshold")
        )

        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]

            try:
                response = self.client.get_video_details(batch_ids)
            except QuotaExceededError:
                logger.warning("Quota dépassé — monitoring intensif interrompu")
                return
            except Exception as e:
                logger.error(f"Erreur monitoring intensif : {e}")
                continue

            for item in response.get("items", []):
                vid_id = item["id"]
                stats  = item.get("statistics", {})
                meta   = videos_map.get(vid_id, {})

                view_count = int(stats.get("viewCount",    0))
                like_count = int(stats.get("likeCount",    0))
                cmt_count  = int(stats.get("commentCount", 0))
                sub_count  = meta.get("subscriber_count",  0)

                # Snapshot enrichi
                save_view_snapshot_enriched(
                    video_id         = vid_id,
                    view_count       = view_count,
                    like_count       = like_count,
                    comment_count    = cmt_count,
                    subscriber_count = sub_count,
                )

                # Détection breakout sur vidéos > 7j
                prev_views = meta.get("view_count", 0)
                if prev_views > 0:
                    velocity = (view_count - prev_views) / prev_views
                    if velocity > breakout_threshold:
                        save_alert(
                            video_id   = vid_id,
                            channel_id = meta.get("channel_id", ""),
                            alert_type = "breakout",
                            details    = {
                                "velocity_24h": round(velocity, 4),
                                "view_count"  : view_count,
                                "prev_views"  : prev_views,
                                "phase"       : "intensive",
                            },
                        )
                        logger.info(
                            f"  Breakout : {vid_id} "
                            f"(+{velocity*100:.1f}%)"
                        )

        logger.info(f"  Snapshots enregistrés : {len(video_ids)}")


# ──────────────────────────────────────────────────────────────────────────────
# JOB 3 — MONITORING CROISSANCE (7-90 jours)
# ──────────────────────────────────────────────────────────────────────────────

class GrowthMonitoringJob:
    """
    Surveille les vidéos de 7 à 90 jours — une fois par semaine.
    Détecte la Long Tail et les breakouts tardifs
    (ex: reprise dans une playlist majeure).
    """

    def __init__(self):
        self.client   = YouTubeClient()
        self.phases   = PhaseManager()
        self.settings = SettingsManager()

    def run(self):
        logger.info("─" * 55)
        logger.info("  [Job 3] Monitoring croissance (7-90j)")

        videos = self.phases.get_phase_videos("growth")
        if not videos:
            logger.info("  Aucune vidéo en phase croissance")
            return

        logger.info(f"  Vidéos à monitorer : {len(videos)}")
        breakout_threshold = float(
            self.settings.get("tracking.breakout_threshold")
        )
        video_ids  = [v["video_id"] for v in videos]
        videos_map = {v["video_id"]: v for v in videos}

        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            try:
                response = self.client.get_video_details(batch_ids)
            except QuotaExceededError:
                logger.warning("Quota dépassé — monitoring croissance interrompu")
                return
            except Exception as e:
                logger.error(f"Erreur monitoring croissance : {e}")
                continue

            for item in response.get("items", []):
                vid_id = item["id"]
                stats  = item.get("statistics", {})
                meta   = videos_map.get(vid_id, {})

                view_count = int(stats.get("viewCount",    0))
                like_count = int(stats.get("likeCount",    0))
                cmt_count  = int(stats.get("commentCount", 0))
                sub_count  = meta.get("subscriber_count",  0)

                save_view_snapshot_enriched(
                    video_id         = vid_id,
                    view_count       = view_count,
                    like_count       = like_count,
                    comment_count    = cmt_count,
                    subscriber_count = sub_count,
                )

                # Breakout tardif — signal fort sur une vieille vidéo
                prev_views = meta.get("view_count", 0)
                if prev_views > 0:
                    weekly_growth = (view_count - prev_views) / prev_views
                    if weekly_growth > breakout_threshold:
                        save_alert(
                            video_id   = vid_id,
                            channel_id = meta.get("channel_id", ""),
                            alert_type = "breakout",
                            details    = {
                                "velocity_weekly": round(weekly_growth, 4),
                                "view_count"     : view_count,
                                "prev_views"     : prev_views,
                                "phase"          : "growth",
                                "note"           : "Long Tail detected",
                            },
                        )
                        logger.info(
                            f"  Long Tail : {vid_id} "
                            f"(+{weekly_growth*100:.1f}% cette semaine)"
                        )

        logger.info(f"  Snapshots croissance : {len(video_ids)}")


# ──────────────────────────────────────────────────────────────────────────────
# JOB 4 — MONITORING PASSIF (90-180 jours)
# ──────────────────────────────────────────────────────────────────────────────

class PassiveMonitoringJob:
    """
    Veille minimale mensuelle sur les vidéos de 90 à 180 jours.
    Inclut les artistes qualifiés au-delà de 180 jours
    (règle keep_qualified).
    """

    def __init__(self):
        self.client = YouTubeClient()
        self.phases = PhaseManager()

    def run(self):
        logger.info("─" * 55)
        logger.info("  [Job 4] Monitoring passif (90-180j)")

        videos = self.phases.get_phase_videos("passive")
        if not videos:
            logger.info("  Aucune vidéo en phase passive")
            return

        logger.info(f"  Vidéos à monitorer : {len(videos)}")
        video_ids = [v["video_id"] for v in videos]

        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            try:
                response = self.client.get_video_details(batch_ids)
            except QuotaExceededError:
                logger.warning("Quota dépassé — monitoring passif interrompu")
                return
            except Exception as e:
                logger.error(f"Erreur monitoring passif : {e}")
                continue

            for item in response.get("items", []):
                vid_id = item["id"]
                stats  = item.get("statistics", {})

                save_view_snapshot_enriched(
                    video_id  = vid_id,
                    view_count= int(stats.get("viewCount",    0)),
                    like_count= int(stats.get("likeCount",    0)),
                    comment_count= int(stats.get("commentCount", 0)),
                )

        logger.info(f"  Snapshots passifs : {len(video_ids)}")


# ──────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE — SCHEDULER PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────

def start_scheduler():
    # Démarrer le health check HTTP pour Cloud Run
    import os
    from src.health_server import start_health_server
    port = int(os.getenv("PORT", "8080"))
    start_health_server(port)
    print(f"Health check server démarré sur port {port}")

    """
    Configure et démarre les 4 jobs APScheduler.

    Séquence type sur 24h :
        00h00 → Job1 (détection) + Job2 (intensif)
        06h00 → Job2 (intensif)
        12h00 → Job2 (intensif)
        18h00 → Job2 (intensif)
        Lundi → Job3 (croissance)
        1er   → Job4 (passif)
    """
    settings = SettingsManager()
    scheduler = BlockingScheduler(timezone="UTC")

    detection_hour    = int(settings.get("tracking.detection_hour"))
    intensive_interval= int(settings.get("tracking.intensive_interval"))

    detection_job  = DetectionJob()
    intensive_job  = IntensiveMonitoringJob()
    growth_job     = GrowthMonitoringJob()
    passive_job    = PassiveMonitoringJob()

    # Job 1 — Détection : quotidien à l'heure configurée
    scheduler.add_job(
        func               = detection_job.run,
        trigger            = CronTrigger(hour=detection_hour, minute=0),
        id                 = "detect_new_videos",
        name               = f"Détection quotidienne ({detection_hour}h UTC)",
        misfire_grace_time = 600,
        replace_existing   = True,
    )

    # Job 2 — Monitoring intensif : toutes les N heures
    scheduler.add_job(
        func               = intensive_job.run,
        trigger            = IntervalTrigger(hours=intensive_interval),
        id                 = "monitor_intensive",
        name               = f"Monitoring intensif (toutes les {intensive_interval}h)",
        misfire_grace_time = 300,
        replace_existing   = True,
    )

    # Job 3 — Monitoring croissance : hebdomadaire (lundi 01h00)
    scheduler.add_job(
        func               = growth_job.run,
        trigger            = CronTrigger(day_of_week="mon", hour=1, minute=0),
        id                 = "monitor_growth",
        name               = "Monitoring croissance (hebdo, lundi 01h)",
        misfire_grace_time = 3600,
        replace_existing   = True,
    )

    # Job 4 — Monitoring passif : mensuel (1er du mois, 02h00)
    scheduler.add_job(
        func               = passive_job.run,
        trigger            = CronTrigger(day=1, hour=2, minute=0),
        id                 = "monitor_passive",
        name               = "Monitoring passif (mensuel, 1er 02h)",
        misfire_grace_time = 7200,
        replace_existing   = True,
    )

    logger.info("Scheduler Tiered Tracking démarré :")
    logger.info(f"  Job 1 — Détection   : quotidien à {detection_hour}h UTC")
    logger.info(f"  Job 2 — Intensif    : toutes les {intensive_interval}h")
    logger.info( "  Job 3 — Croissance  : lundi 01h UTC")
    logger.info( "  Job 4 — Passif      : 1er du mois 02h UTC")

    # Lancer un monitoring intensif immédiatement au démarrage
    logger.info("Démarrage immédiat du monitoring intensif...")
    intensive_job.run()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler arrêté.")
        scheduler.shutdown()


if __name__ == "__main__":
    start_scheduler()