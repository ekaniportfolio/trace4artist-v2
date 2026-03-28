"""
tests/test_scheduler.py — Tests du scheduler

On teste uniquement ce qui existe réellement dans le code :
- La logique de calcul de la date de départ (premier scan vs incrémental)
- La protection par quota
- L'envoi des tâches Celery (via mock)

ScanOrchestrator a été remplacé par DetectionJob dans le scheduler
Tiered Tracking. On teste ici la même logique de date de départ
et de protection par quota, maintenant portée par DetectionJob.
"""
 
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
 
from src.scheduler import DetectionJob
 
 
@pytest.fixture
def job():
    """Crée un DetectionJob avec client et settings mockés."""
    detection_job = DetectionJob()
    detection_job.searcher  = MagicMock()
    detection_job.settings  = MagicMock()
    detection_job.phases    = MagicMock()
    detection_job.settings.get_regions.return_value     = ["CM", "NG"]
    detection_job.settings.get_max_results.return_value = 50
    detection_job.settings.get_keywords.return_value    = "official video"
    detection_job.settings.get_lookback_days.return_value = 365
    return detection_job
 
 
# ──────────────────────────────────────────────────────────────────────
# TESTS : LOGIQUE DE DATE DE DÉPART
# ──────────────────────────────────────────────────────────────────────
 
class TestPublishedAfterLogic:
 
    def test_first_scan_uses_lookback_period(self, job):
        """Sans historique, on remonte get_lookback_days() en arrière."""
        with patch("src.scheduler.get_last_scan_date", return_value=None):
            date_str = job._get_published_after()
 
        date  = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        now   = datetime.now(timezone.utc)
        delta = now - date
 
        assert 364 <= delta.days <= 366
 
    def test_incremental_scan_uses_last_scan_date(self, job):
        """Après un premier scan, on repart de la date du dernier."""
        last_scan = "2024-06-01T12:00:00+00:00"
        with patch("src.scheduler.get_last_scan_date", return_value=last_scan):
            date_str = job._get_published_after()
 
        assert date_str == last_scan
 
    def test_incremental_takes_priority_over_lookback(self, job):
        """Un scan récent prime toujours sur le lookback initial."""
        recent = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        with patch("src.scheduler.get_last_scan_date", return_value=recent):
            date_str = job._get_published_after()
 
        assert date_str == recent
 
 
# ──────────────────────────────────────────────────────────────────────
# TESTS : PROTECTION PAR QUOTA
# ──────────────────────────────────────────────────────────────────────
 
class TestQuotaProtection:
 
    def test_scan_aborts_when_no_quota(self, job):
        """Quota épuisé → aucun appel à search_region."""
        with patch("src.scheduler.get_quota_used_today", return_value=9950), \
             patch("src.scheduler.get_last_scan_date", return_value=None), \
             patch.object(job, "_start_scan_log", return_value=1), \
             patch.object(job, "_end_scan_log"), \
             patch.object(job, "_scan_region") as mock_scan:
 
            job.run()
 
        mock_scan.assert_not_called()
 
    def test_scan_logs_failure_when_no_quota(self, job):
        """Le scan_log doit être marqué 'failed' si quota épuisé."""
        with patch("src.scheduler.get_quota_used_today", return_value=9950), \
             patch("src.scheduler.get_last_scan_date", return_value=None), \
             patch.object(job, "_start_scan_log", return_value=1), \
             patch.object(job, "_end_scan_log") as mock_end, \
             patch.object(job, "_scan_region"):
 
            job.run()
 
        args = mock_end.call_args
        # Le 2ème argument positionnel ou kwarg 'status' doit être 'failed'
        status = args[0][1] if args[0] else args[1].get("status")
        assert status == "failed"
 
 
# ──────────────────────────────────────────────────────────────────────
# TESTS : DÉCLENCHEMENT DES TÂCHES CELERY
# ──────────────────────────────────────────────────────────────────────
 
class TestCeleryTasksTriggered:
 
    def test_scoring_triggered_after_detection(self, job):
        """Après détection, score_pending_artists doit être planifié."""
        with patch("src.scheduler.get_quota_used_today", return_value=0), \
             patch("src.scheduler.get_last_scan_date", return_value=None), \
             patch.object(job, "_start_scan_log", return_value=1), \
             patch.object(job, "_end_scan_log"), \
             patch.object(job, "_scan_region", return_value=(0, 0)), \
             patch("src.scheduler.score_pending_artists") as mock_score, \
             patch("src.scheduler.sync_to_hubspot"):
 
            job.run()
 
        mock_score.apply_async.assert_called_once_with(countdown=120)
 
    def test_hubspot_sync_triggered_after_detection(self, job):
        """sync_to_hubspot doit être planifié après la détection."""
        with patch("src.scheduler.get_quota_used_today", return_value=0), \
             patch("src.scheduler.get_last_scan_date", return_value=None), \
             patch.object(job, "_start_scan_log", return_value=1), \
             patch.object(job, "_end_scan_log"), \
             patch.object(job, "_scan_region", return_value=(0, 0)), \
             patch("src.scheduler.score_pending_artists"), \
             patch("src.scheduler.sync_to_hubspot") as mock_sync:
 
            job.run()
 
        mock_sync.apply_async.assert_called_once_with(countdown=900)
 
    def test_phases_updated_after_detection(self, job):
        """update_all_phases doit être appelé après chaque détection."""
        with patch("src.scheduler.get_quota_used_today", return_value=0), \
             patch("src.scheduler.get_last_scan_date", return_value=None), \
             patch.object(job, "_start_scan_log", return_value=1), \
             patch.object(job, "_end_scan_log"), \
             patch.object(job, "_scan_region", return_value=(0, 0)), \
             patch("src.scheduler.score_pending_artists"), \
             patch("src.scheduler.sync_to_hubspot"):
 
            job.run()
 
        job.phases.update_all_phases.assert_called_once()