"""
tests/test_tiered_tracking.py — Tests du Tiered Tracking Scheduler

On teste :
    - PhaseManager : calcul des phases selon l'âge
    - Transitions de phase avec log (Option B)
    - Règle keep_qualified (artistes qualifiés continuent en passive)
    - Comportement des jobs face au quota insuffisant
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

from src.phase_manager import PhaseManager


@pytest.fixture
def phase_manager():
    pm = PhaseManager()
    pm.settings = MagicMock()
    pm.settings.get_lookback_days.return_value = 365
    pm.settings.get.side_effect = lambda key: {
        "tracking.intensive_max_days": "7",
        "tracking.growth_max_days"   : "90",
        "tracking.passive_max_days"  : "180",
        "tracking.keep_qualified"    : "true",
        "tracking.breakout_threshold": "0.20",
    }[key]
    return pm


# ──────────────────────────────────────────────────────────────────────
# TESTS : CALCUL DES PHASES
# ──────────────────────────────────────────────────────────────────────

class TestPhaseComputation:

    def test_new_video_is_intensive(self, phase_manager):
        """Vidéo de 2 jours → phase intensive."""
        phase = phase_manager._compute_phase(2, 7, 90, 180, True, "discovered")
        assert phase == "intensive"

    def test_video_at_intensive_boundary(self, phase_manager):
        """Exactement 7 jours → encore intensive."""
        phase = phase_manager._compute_phase(7, 7, 90, 180, True, "discovered")
        assert phase == "intensive"

    def test_video_enters_growth(self, phase_manager):
        """8 jours → phase croissance."""
        phase = phase_manager._compute_phase(8, 7, 90, 180, True, "discovered")
        assert phase == "growth"

    def test_video_at_growth_boundary(self, phase_manager):
        """90 jours → encore croissance."""
        phase = phase_manager._compute_phase(90, 7, 90, 180, True, "discovered")
        assert phase == "growth"

    def test_video_enters_passive(self, phase_manager):
        """91 jours → phase passive."""
        phase = phase_manager._compute_phase(91, 7, 90, 180, True, "discovered")
        assert phase == "passive"

    def test_old_video_stops(self, phase_manager):
        """181 jours + artiste non qualifié → stopped."""
        phase = phase_manager._compute_phase(181, 7, 90, 180, True, "discovered")
        assert phase == "stopped"

    def test_old_video_qualified_stays_passive(self, phase_manager):
        """
        181 jours + artiste qualifié + keep_qualified=True
        → reste en passive (on continue à tracker).
        """
        phase = phase_manager._compute_phase(181, 7, 90, 180, True, "qualified")
        assert phase == "passive"

    def test_keep_qualified_false_stops_anyway(self, phase_manager):
        """
        181 jours + artiste qualifié + keep_qualified=False
        → stopped (la règle est désactivée).
        """
        phase = phase_manager._compute_phase(181, 7, 90, 180, False, "qualified")
        assert phase == "stopped"


# ──────────────────────────────────────────────────────────────────────
# TESTS : TRANSITIONS DE PHASE (Option B)
# ──────────────────────────────────────────────────────────────────────

class TestPhaseTransitions:

    def test_transition_logged_when_phase_changes(self, phase_manager):
        """
        Quand une vidéo change de phase, update_tracking_phase
        doit être appelé (ce qui crée une alerte 'phase_change').
        """
        now = datetime.now(timezone.utc)

        # Vidéo de 8 jours — devrait passer de 'intensive' à 'growth'
        videos = [{
            "video_id"     : "vid001",
            "channel_id"   : "UCtest",
            "published_at" : (now - timedelta(days=8)).isoformat(),
            "tracking_phase": "intensive",
            "artist_status": "discovered",
        }]

        with patch.object(phase_manager, "_get_all_trackable_videos",
                          return_value=videos), \
             patch("src.phase_manager.update_tracking_phase") as mock_update:

            phase_manager.update_all_phases()

        mock_update.assert_called_once_with(
            video_id  = "vid001",
            new_phase = "growth",
            old_phase = "intensive",
        )

    def test_no_transition_when_phase_unchanged(self, phase_manager):
        """
        Si la phase calculée == phase actuelle,
        update_tracking_phase ne doit PAS être appelé.
        """
        now = datetime.now(timezone.utc)

        videos = [{
            "video_id"     : "vid001",
            "channel_id"   : "UCtest",
            "published_at" : (now - timedelta(days=3)).isoformat(),
            "tracking_phase": "intensive",   # âge 3j → reste intensive
            "artist_status": "discovered",
        }]

        with patch.object(phase_manager, "_get_all_trackable_videos",
                          return_value=videos), \
             patch("src.phase_manager.update_tracking_phase") as mock_update:

            phase_manager.update_all_phases()

        mock_update.assert_not_called()

    def test_transition_counts_returned(self, phase_manager):
        """update_all_phases retourne le compte des transitions."""
        now = datetime.now(timezone.utc)

        videos = [
            {   # intensive → growth
                "video_id": "v1", "channel_id": "UC1",
                "published_at": (now - timedelta(days=8)).isoformat(),
                "tracking_phase": "intensive", "artist_status": "discovered",
            },
            {   # growth → passive
                "video_id": "v2", "channel_id": "UC2",
                "published_at": (now - timedelta(days=95)).isoformat(),
                "tracking_phase": "growth", "artist_status": "discovered",
            },
        ]

        with patch.object(phase_manager, "_get_all_trackable_videos",
                          return_value=videos), \
             patch("src.phase_manager.update_tracking_phase"):

            result = phase_manager.update_all_phases()

        assert result["to_growth"]  == 1
        assert result["to_passive"] == 1
        assert result["to_stopped"] == 0


# ──────────────────────────────────────────────────────────────────────
# TESTS : JOBS DE MONITORING
# ──────────────────────────────────────────────────────────────────────

class TestMonitoringJobs:

    def test_intensive_job_skips_when_no_videos(self):
        """Pas de vidéos intensives → aucun appel YouTube."""
        from src.scheduler import IntensiveMonitoringJob

        job = IntensiveMonitoringJob()
        job.phases   = MagicMock()
        job.settings = MagicMock()
        job.settings.get.return_value = "0.20"
        job.phases.get_phase_videos.return_value = []

        with patch.object(job.client, "get_video_details") as mock_yt:
            job.run()

        mock_yt.assert_not_called()

    def test_growth_job_skips_when_no_videos(self):
        """Pas de vidéos en croissance → aucun appel YouTube."""
        from src.scheduler import GrowthMonitoringJob

        job = GrowthMonitoringJob()
        job.phases   = MagicMock()
        job.settings = MagicMock()
        job.settings.get.return_value = "0.20"
        job.phases.get_phase_videos.return_value = []

        with patch.object(job.client, "get_video_details") as mock_yt:
            job.run()

        mock_yt.assert_not_called()

    def test_intensive_job_stops_on_quota_exceeded(self):
        """QuotaExceededError → le job s'arrête proprement."""
        from src.scheduler import IntensiveMonitoringJob
        from src.youtube_client import QuotaExceededError

        job = IntensiveMonitoringJob()
        job.phases   = MagicMock()
        job.settings = MagicMock()
        job.settings.get.return_value = "0.20"
        job.phases.get_phase_videos.return_value = [
            {"video_id": "v1", "channel_id": "UC1",
             "view_count": 1000, "subscriber_count": 5000}
        ]

        with patch.object(job.client, "get_video_details",
                          side_effect=QuotaExceededError("Quota épuisé")), \
             patch("src.scheduler.save_view_snapshot_enriched") as mock_snap:

            job.run()   # Ne doit pas lever d'exception

        mock_snap.assert_not_called()
