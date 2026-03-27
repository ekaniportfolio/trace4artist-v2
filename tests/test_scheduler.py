"""
tests/test_scheduler.py — Tests du scheduler

On teste uniquement ce qui existe réellement dans le code :
- La logique de calcul de la date de départ (premier scan vs incrémental)
- La protection par quota
- L'envoi des tâches Celery (via mock)

Les tests de _scan_region() seront écrits à l'Étape 3
quand YouTubeClient existera.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

from src.scheduler import ScanOrchestrator


@pytest.fixture
def orchestrator():
    return ScanOrchestrator()


# ──────────────────────────────────────────────────────────────────────
# TESTS : LOGIQUE DE DATE DE DÉPART
# ──────────────────────────────────────────────────────────────────────

class TestPublishedAfterLogic:

    def test_first_scan_uses_lookback_period(self, orchestrator):
        """Sans historique, on remonte INITIAL_LOOKBACK_DAYS en arrière."""
        with patch("src.scheduler.get_last_scan_date", return_value=None):
            date_str = orchestrator._get_published_after()

        date  = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        now   = datetime.now(timezone.utc)
        delta = now - date

        assert 364 <= delta.days <= 366

    def test_incremental_scan_uses_last_scan_date(self, orchestrator):
        """Après un premier scan, on repart de la date du dernier scan."""
        last_scan = "2024-06-01T12:00:00+00:00"
        with patch("src.scheduler.get_last_scan_date", return_value=last_scan):
            date_str = orchestrator._get_published_after()

        assert date_str == last_scan

    def test_incremental_takes_priority_over_lookback(self, orchestrator):
        """Un scan récent (6h) doit primer sur le lookback initial."""
        recent = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        with patch("src.scheduler.get_last_scan_date", return_value=recent):
            date_str = orchestrator._get_published_after()

        assert date_str == recent


# ──────────────────────────────────────────────────────────────────────
# TESTS : PROTECTION PAR QUOTA
# ──────────────────────────────────────────────────────────────────────

class TestQuotaProtection:

    def test_scan_aborts_when_no_quota(self, orchestrator):
        """
        Quota épuisé → scan annulé immédiatement.
        _scan_region ne doit jamais être appelé.
        """
        with patch("src.scheduler.get_quota_used_today", return_value=9950), \
             patch("src.scheduler.get_last_scan_date", return_value=None), \
             patch.object(orchestrator, "_start_scan_log", return_value=1), \
             patch.object(orchestrator, "_end_scan_log"), \
             patch.object(orchestrator, "_scan_region") as mock_scan:

            orchestrator.run()

        mock_scan.assert_not_called()

    def test_scan_logs_failure_when_no_quota(self, orchestrator):
        """Le scan_log doit être marqué 'failed' quand le quota est épuisé."""
        with patch("src.scheduler.get_quota_used_today", return_value=9950), \
             patch("src.scheduler.get_last_scan_date", return_value=None), \
             patch.object(orchestrator, "_start_scan_log", return_value=1), \
             patch.object(orchestrator, "_end_scan_log") as mock_end, \
             patch.object(orchestrator, "_scan_region"):

            orchestrator.run()

        call_kwargs = mock_end.call_args
        assert call_kwargs[1]["status"] == "failed" or \
               call_kwargs[0][1] == "failed"


# ──────────────────────────────────────────────────────────────────────
# TESTS : DÉCLENCHEMENT DES TÂCHES CELERY
# ──────────────────────────────────────────────────────────────────────

class TestCeleryTasksTriggered:

    def test_scoring_task_triggered_after_scan(self, orchestrator):
        """
        Après un scan réussi, score_pending_artists
        doit être planifié avec un délai.
        """
        with patch("src.scheduler.get_quota_used_today", return_value=0), \
             patch("src.scheduler.get_last_scan_date", return_value=None), \
             patch.object(orchestrator, "_start_scan_log", return_value=1), \
             patch.object(orchestrator, "_end_scan_log"), \
             patch.object(orchestrator, "_scan_region", return_value=(0, 0)), \
             patch("src.scheduler.score_pending_artists") as mock_score, \
             patch("src.scheduler.sync_to_hubspot"):

            orchestrator.run()

        mock_score.apply_async.assert_called_once_with(countdown=120)

    def test_hubspot_sync_triggered_after_scan(self, orchestrator):
        """sync_to_hubspot doit être planifié après le scoring."""
        with patch("src.scheduler.get_quota_used_today", return_value=0), \
             patch("src.scheduler.get_last_scan_date", return_value=None), \
             patch.object(orchestrator, "_start_scan_log", return_value=1), \
             patch.object(orchestrator, "_end_scan_log"), \
             patch.object(orchestrator, "_scan_region", return_value=(0, 0)), \
             patch("src.scheduler.score_pending_artists"), \
             patch("src.scheduler.sync_to_hubspot") as mock_sync:

            orchestrator.run()

        mock_sync.apply_async.assert_called_once_with(countdown=300)