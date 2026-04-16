"""
tests/test_api.py — Tests de l'API FastAPI

On utilise le TestClient de FastAPI/Starlette qui simule
des vraies requêtes HTTP sans démarrer un vrai serveur.
La base de données est mockée dans chaque test.
"""

import pytest
from starlette.testclient import TestClient
from unittest.mock import patch, MagicMock

from src.api import app
from src.auth import create_access_token

client = TestClient(app)

# Token admin valide pour les tests des routes protégées
ADMIN_TOKEN   = create_access_token(user_id=1, username="admin", role="admin")
ADMIN_HEADERS = {"Authorization": f"Bearer {ADMIN_TOKEN}"}


# ──────────────────────────────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_artists():
    return [
        {
            "channel_id"      : "UCabc",
            "artist_name"     : "Artiste CM",
            "country"         : "CM",
            "subscriber_count": 45_000,
            "total_views"     : 850_000,
            "email"           : "contact@artiste.cm",
            "website"         : "https://artiste.cm",
            "instagram"       : "artiste_cm",
            "status"          : "qualified",
            "hubspot_contact_id": "hs_001",
            "created_at"      : "2024-01-01T00:00:00Z",
            "updated_at"      : "2024-06-01T00:00:00Z",
            "score"           : 72.5,
            "segment"         : "standard",
            "enrichment_data" : None,
        }
    ]


def make_mock_conn(rows=None, scalar_value=0):
    """Helper : crée une connexion DB mockée."""
    mock_conn  = MagicMock()
    mock_rows  = [MagicMock(_mapping=row) for row in (rows or [])]
    mock_conn.execute.return_value.fetchall.return_value = mock_rows
    mock_conn.execute.return_value.fetchone.return_value = (
        MagicMock(_mapping=(rows[0] if rows else {}))
    )
    mock_conn.execute.return_value.scalar.return_value = scalar_value
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__  = MagicMock(return_value=False)
    return mock_conn


# ──────────────────────────────────────────────────────────────────────
# TESTS : HEALTH CHECK
# ──────────────────────────────────────────────────────────────────────

class TestHealthCheck:

    def test_health_ok(self):
        with patch("src.api.get_db") as mock_db:
            mock_db.return_value = make_mock_conn()
            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"]   == "ok"
        assert data["database"] == "ok"

    def test_health_degraded_when_db_down(self):
        with patch("src.api.get_db") as mock_db:
            mock_db.return_value.__enter__ = MagicMock(
                side_effect=Exception("DB down")
            )
            response = client.get("/health")

        assert response.status_code == 200
        assert response.json()["status"] == "degraded"


# ──────────────────────────────────────────────────────────────────────
# TESTS : ARTISTES
# ──────────────────────────────────────────────────────────────────────

class TestArtistsEndpoints:

    def test_list_artists_returns_200(self, sample_artists):
        with patch("src.api.get_db") as mock_db:
            mock_conn = make_mock_conn(sample_artists, scalar_value=1)
            mock_db.return_value = mock_conn

            response = client.get("/artists")

        assert response.status_code == 200
        data = response.json()
        assert "total"  in data
        assert "items"  in data
        assert "limit"  in data
        assert "offset" in data

    def test_list_artists_pagination_params(self):
        """Les paramètres limit/offset sont bien transmis."""
        with patch("src.api.get_db") as mock_db:
            mock_db.return_value = make_mock_conn([], scalar_value=0)
            response = client.get("/artists?limit=10&offset=20")

        assert response.status_code == 200

    def test_list_artists_invalid_limit(self):
        """limit > 200 doit retourner 422."""
        response = client.get("/artists?limit=500")
        assert response.status_code == 422

    def test_get_artist_not_found(self):
        with patch("src.api.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = None
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            response = client.get("/artists/UCinexistant")

        assert response.status_code == 404

    def test_get_top_artists(self, sample_artists):
        with patch("src.api.get_db") as mock_db:
            mock_db.return_value = make_mock_conn(sample_artists)
            response = client.get("/artists/top?limit=5")

        assert response.status_code == 200


# ──────────────────────────────────────────────────────────────────────
# TESTS : SETTINGS
# ──────────────────────────────────────────────────────────────────────

class TestSettingsEndpoints:

    def test_get_all_settings(self):
        mock_settings = [
            {"key": "scan.lookback_days", "value": "365",
             "description": "...", "updated_at": None},
        ]
        with patch("src.api.SettingsManager") as MockSM:
            MockSM.return_value.get_all.return_value = mock_settings
            response = client.get("/settings")

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_update_setting_valid(self):
        with patch("src.auth.get_db") as mock_db,              patch("src.api.SettingsManager") as MockSM:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = (1, True)
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn
            MockSM.return_value.set.return_value = {
                "key": "scan.lookback_days", "value": "30"
            }
            response = client.patch(
                "/settings/scan.lookback_days",
                headers=ADMIN_HEADERS,
                json={"value": "30"},
            )

        assert response.status_code == 200
        assert response.json()["status"] == "updated"

    def test_update_setting_unknown_key(self):
        with patch("src.auth.get_db") as mock_db,              patch("src.api.SettingsManager") as MockSM:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = (1, True)
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn
            MockSM.return_value.set.side_effect = \
                KeyError("Paramètre inconnu")
            response = client.patch(
                "/settings/scan.inexistant",
                headers=ADMIN_HEADERS,
                json={"value": "test"},
            )

        assert response.status_code == 404

    def test_update_setting_invalid_value(self):
        with patch("src.auth.get_db") as mock_db,              patch("src.api.SettingsManager") as MockSM:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = (1, True)
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn
            MockSM.return_value.set.side_effect = \
                ValueError("Doit être un entier")
            response = client.patch(
                "/settings/scan.interval_hours",
                headers=ADMIN_HEADERS,
                json={"value": "pas_un_nombre"},
            )

        assert response.status_code == 422


# ──────────────────────────────────────────────────────────────────────
# TESTS : STATS DASHBOARD
# ──────────────────────────────────────────────────────────────────────

class TestDashboardStats:

    def test_dashboard_returns_expected_keys(self):
        with patch("src.api.get_db") as mock_db:
            mock_conn = MagicMock()
            # artists count
            mock_conn.execute.return_value.fetchone.return_value = \
                MagicMock(_mapping={
                    "total": 100, "qualified": 30, "rejected": 50,
                    "pending": 15, "activated": 5, "with_email": 20,
                })
            mock_conn.execute.return_value.fetchall.return_value = []
            mock_conn.execute.return_value.scalar.return_value = 1500
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            response = client.get("/stats/dashboard")

        assert response.status_code == 200
        data = response.json()
        assert "artists"      in data
        assert "quota"        in data
        assert "segments"     in data
        assert "generated_at" in data

    def test_quota_has_correct_structure(self):
        with patch("src.api.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = \
                MagicMock(_mapping={
                    "total": 0, "qualified": 0, "rejected": 0,
                    "pending": 0, "activated": 0, "with_email": 0,
                })
            mock_conn.execute.return_value.fetchall.return_value = []
            mock_conn.execute.return_value.scalar.return_value = 3000
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            response = client.get("/stats/dashboard")

        quota = response.json()["quota"]
        assert quota["limit"]  == 10_000
        assert "used"      in quota
        assert "remaining" in quota
        assert "percent"   in quota


# ──────────────────────────────────────────────────────────────────────
# TESTS : ALERTES
# ──────────────────────────────────────────────────────────────────────

class TestAlertsEndpoints:

    def test_get_alerts_returns_list(self):
        alerts = [{
            "id": 1, "video_id": "v1", "channel_id": "UC1",
            "alert_type": "breakout", "details": {},
            "is_processed": False, "detected_at": "2024-06-01T00:00:00Z",
            "artist_name": "Test",
        }]
        with patch("src.api.get_db") as mock_db:
            mock_db.return_value = make_mock_conn(alerts)
            response = client.get("/alerts")

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_mark_alert_processed(self):
        with patch("src.auth.get_db") as mock_auth_db,              patch("src.api.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = (1, True)
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_auth_db.return_value = mock_conn
            mock_db.return_value = mock_conn

            response = client.patch("/alerts/1/process", headers=ADMIN_HEADERS)

        assert response.status_code == 200
        assert response.json()["status"] == "processed"

    def test_mark_alert_not_found(self):
        with patch("src.auth.get_db") as mock_auth_db,              patch("src.api.get_db") as mock_db:
            # mock_auth_db retourne user actif
            mock_auth_conn = MagicMock()
            mock_auth_conn.execute.return_value.fetchone.return_value = (1, True)
            mock_auth_conn.__enter__ = lambda s: mock_auth_conn
            mock_auth_conn.__exit__  = MagicMock(return_value=False)
            mock_auth_db.return_value = mock_auth_conn
            # mock_db retourne None (alerte introuvable)
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = None
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            response = client.patch("/alerts/999/process", headers=ADMIN_HEADERS)

        assert response.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# TESTS : VIDÉOS RÉCENTES
# ──────────────────────────────────────────────────────────────────────

class TestRecentVideos:

    def test_recent_videos_returns_list(self):
        with patch("src.api.get_db") as mock_db:
            mock_db.return_value = make_mock_conn([{
                "video_id"    : "v1",
                "title"       : "Clip Test",
                "view_count"  : 50_000,
                "like_count"  : 2_000,
                "comment_count": 100,
                "published_at": "2024-06-01T00:00:00Z",
                "tracking_phase": "intensive",
                "is_suspicious" : False,
                "artist_name" : "Artiste Test",
                "country"     : "CM",
                "channel_id"  : "UCtest",
                "spr"         : 2.5,
                "latest_snapshot_views": 48_000,
                "snapshot_at" : "2024-06-01T06:00:00Z",
            }])
            response = client.get("/videos/recent")

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_recent_videos_phase_filter(self):
        with patch("src.api.get_db") as mock_db:
            mock_db.return_value = make_mock_conn([])
            response = client.get("/videos/recent?phase=intensive")

        assert response.status_code == 200


# ──────────────────────────────────────────────────────────────────────
# TESTS : BOT CONTROL
# ──────────────────────────────────────────────────────────────────────

class TestBotControl:

    def test_bot_status_returns_expected_keys(self):
        from datetime import datetime, timezone, timedelta

        completed = datetime.now(timezone.utc) - timedelta(hours=2)
        with patch("src.api.get_db") as mock_db:
            mock_db.return_value = make_mock_conn([{
                "status"       : "completed",
                "scan_type"    : "incremental",
                "started_at"   : datetime.now(timezone.utc),
                "completed_at" : completed,
                "error_message": None,
                "videos_found" : 42,
                "quota_used"   : 102,
                "count"        : 3,
                "total_videos" : 120,
            }])

            response = client.get("/bot/status")

        assert response.status_code == 200
        data = response.json()
        assert "status"     in data
        assert "last_scan"  in data
        assert "activity_24h" in data

    def test_bot_schedule_returns_4_jobs(self):
        with patch("src.api.get_db") as mock_db, \
             patch("src.api.SettingsManager") as MockSM:

            MockSM.return_value.get.side_effect = lambda k: {
                "tracking.detection_hour"    : "0",
                "tracking.intensive_interval": "6",
            }[k]
            MockSM.return_value.get_regions.return_value = ["CM", "NG"]
            mock_db.return_value = make_mock_conn([])

            response = client.get("/bot/schedule")

        assert response.status_code == 200
        data = response.json()
        assert len(data["schedule"]) == 4

    def test_bot_stop_returns_status(self):
        with patch("src.auth.get_db") as mock_auth_db,              patch("src.api.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = (1, True)
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_auth_db.return_value = mock_conn
            mock_db.return_value = make_mock_conn([])
            response = client.post("/bot/stop", headers=ADMIN_HEADERS)

        assert response.status_code == 200
        assert response.json()["status"] == "stop_requested"


# ──────────────────────────────────────────────────────────────────────
# TESTS : STATS HEBDOMADAIRES
# ──────────────────────────────────────────────────────────────────────

class TestWeeklyStats:

    def test_weekly_stats_returns_expected_keys(self):
        with patch("src.api.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchall.return_value = []
            mock_conn.execute.return_value.fetchone.return_value = None
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            response = client.get("/stats/weekly")

        assert response.status_code == 200
        data = response.json()
        assert "videos_by_day"    in data
        assert "quota_by_day"     in data
        assert "qualified_by_day" in data
        assert "comparison"       in data

    def test_weekly_comparison_has_growth_rate(self):
        with patch("src.api.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchall.return_value = []
            mock_conn.execute.return_value.fetchone.return_value = None
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            response = client.get("/stats/weekly")

        comparison = response.json()["comparison"]
        assert "growth_rate" in comparison


# ──────────────────────────────────────────────────────────────────────
# TESTS : SANTÉ DES APIS
# ──────────────────────────────────────────────────────────────────────

class TestApiHealth:

    def test_api_health_returns_all_services(self):
        with patch("src.api.get_db") as mock_db, \
             patch("config.YOUTUBE_API_KEY", "fake_key"), \
             patch("config.HUBSPOT_API_KEY", "fake_key"), \
             patch("config.SPOTIFY_CLIENT_ID", "fake_id"), \
             patch("config.GOOGLE_SEARCH_API_KEY", "fake_key"):

            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = \
                MagicMock(_mapping={"calls_today": 5, "units_today": 500})
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            response = client.get("/stats/api-health")

        assert response.status_code == 200
        data = response.json()
        assert "overall" in data
        assert "youtube"       in data["apis"]
        assert "hubspot"       in data["apis"]
        assert "spotify"       in data["apis"]
        assert "google_search" in data["apis"]

    def test_api_health_not_configured(self):
        with patch("src.api.get_db") as mock_db, \
             patch("config.YOUTUBE_API_KEY", ""), \
             patch("config.HUBSPOT_API_KEY", ""), \
             patch("config.SPOTIFY_CLIENT_ID", ""), \
             patch("config.GOOGLE_SEARCH_API_KEY", ""):

            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = \
                MagicMock(_mapping={"calls_today": 0, "units_today": 0})
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            response = client.get("/stats/api-health")

        assert response.status_code == 200
        assert response.json()["overall"] == "partial"


# ──────────────────────────────────────────────────────────────────────
# TESTS : STATS COMMERCIALES HUBSPOT
# ──────────────────────────────────────────────────────────────────────

class TestCommercialStats:

    def test_returns_not_configured_without_hubspot(self):
        with patch("config.HUBSPOT_API_KEY", ""):
            response = client.get("/stats/commercial")

        assert response.status_code == 200
        assert response.json()["status"] == "not_configured"

    def test_weekly_returns_not_configured_without_hubspot(self):
        with patch("config.HUBSPOT_API_KEY", ""):
            response = client.get("/stats/commercial/weekly")

        assert response.status_code == 200
        assert response.json()["status"] == "not_configured"