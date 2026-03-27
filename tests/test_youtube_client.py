"""
tests/test_youtube_client.py — Tests du client YouTube v2

Nouveautés testées vs v1 :
    - Cache Redis : un hit cache évite l'appel API
    - Cache Redis : une indisponibilité Redis ne plante pas le programme
    - Quota loggé en PostgreSQL (mocké)
"""

import pytest
from unittest.mock import patch, MagicMock

from src.youtube_client import YouTubeClient, QuotaExceededError


@pytest.fixture
def client():
    return YouTubeClient()


@pytest.fixture
def mock_video_response():
    return {
        "items": [{
            "id"            : "vid001",
            "snippet"       : {"title": "Test Clip", "publishedAt": "2024-01-01T00:00:00Z"},
            "statistics"    : {"viewCount": "50000", "likeCount": "2000"},
            "contentDetails": {"duration": "PT3M30S"},
        }]
    }


# ──────────────────────────────────────────────────────────────────────
# TESTS : QUOTA
# ──────────────────────────────────────────────────────────────────────

class TestQuotaManagement:

    def test_raises_when_quota_exceeded(self, client):
        with patch("src.youtube_client.get_quota_used_today", return_value=9950):
            with pytest.raises(QuotaExceededError):
                client.search_music_videos(
                    region          = "CM",
                    published_after = "2024-01-01T00:00:00Z",
                    keywords        = "official video",
                )

    def test_quota_logged_after_successful_call(self, client, mock_video_response):
        with patch("src.youtube_client.get_quota_used_today", return_value=0), \
             patch("src.youtube_client.log_quota_usage") as mock_log, \
             patch.object(client, "_get_service") as mock_svc, \
             patch.object(client, "_cache_get", return_value=None), \
             patch.object(client, "_cache_set"):

            mock_svc.return_value.videos.return_value.list \
                .return_value.execute.return_value = mock_video_response

            client.get_video_details(["vid001"])

        mock_log.assert_called_once_with("videos.list", 1)

    def test_quota_not_logged_on_api_error(self, client):
        from googleapiclient.errors import HttpError
        from unittest.mock import Mock

        mock_resp      = Mock()
        mock_resp.status = 400
        http_error     = HttpError(resp=mock_resp, content=b"Bad Request")

        with patch("src.youtube_client.get_quota_used_today", return_value=0), \
             patch("src.youtube_client.log_quota_usage") as mock_log, \
             patch.object(client, "_get_service") as mock_svc, \
             patch.object(client, "_cache_get", return_value=None):

            mock_svc.return_value.videos.return_value.list \
                .return_value.execute.side_effect = http_error

            with pytest.raises(ValueError):
                client.get_video_details(["vid001"])

        mock_log.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# TESTS : CACHE REDIS
# ──────────────────────────────────────────────────────────────────────

class TestRedisCache:

    def test_cache_hit_skips_api_call(self, client, mock_video_response):
        """
        Si le cache contient déjà la réponse, on ne doit
        pas appeler YouTube du tout.
        """
        with patch.object(client, "_cache_get", return_value=mock_video_response), \
             patch.object(client, "_execute") as mock_execute:

            result = client.get_video_details(["vid001"])

        mock_execute.assert_not_called()
        assert result == mock_video_response

    def test_cache_miss_calls_api_and_stores_result(self, client, mock_video_response):
        """
        Cache vide → appel API → résultat mis en cache.
        """
        with patch("src.youtube_client.get_quota_used_today", return_value=0), \
             patch("src.youtube_client.log_quota_usage"), \
             patch.object(client, "_cache_get", return_value=None), \
             patch.object(client, "_cache_set") as mock_set, \
             patch.object(client, "_get_service") as mock_svc:

            mock_svc.return_value.videos.return_value.list \
                .return_value.execute.return_value = mock_video_response

            result = client.get_video_details(["vid001"])

        mock_set.assert_called_once()
        assert result == mock_video_response

    def test_redis_unavailable_does_not_crash(self, client, mock_video_response):
        """
        Si Redis est down, le programme doit continuer normalement
        en appelant YouTube directement — le cache est non critique.
        """
        with patch("src.youtube_client.get_quota_used_today", return_value=0), \
             patch("src.youtube_client.log_quota_usage"), \
             patch.object(client, "_get_redis",
                          side_effect=Exception("Redis connection refused")), \
             patch.object(client, "_get_service") as mock_svc:

            mock_svc.return_value.videos.return_value.list \
                .return_value.execute.return_value = mock_video_response

            # Ne doit pas lever d'exception malgré Redis down
            result = client.get_video_details(["vid001"])

        assert result == mock_video_response

    def test_empty_video_ids_returns_without_cache_call(self, client):
        """Liste vide → retour immédiat, pas de cache, pas d'API."""
        with patch.object(client, "_cache_get") as mock_cache, \
             patch.object(client, "_execute") as mock_execute:

            result = client.get_video_details([])

        assert result == {"items": []}
        mock_cache.assert_not_called()
        mock_execute.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# TESTS : SEARCHER V2
# ──────────────────────────────────────────────────────────────────────

class TestSearcherV2:
    """
    Tests du module searcher adapté pour PostgreSQL et snapshots de vues.
    """

    def test_process_batch_saves_snapshots(self):
        """
        process_batch doit enregistrer un snapshot de vues
        pour chaque vidéo traitée.
        """
        from src.searcher import ArtistSearcher

        mock_client = MagicMock()
        mock_client.get_video_details.return_value = {"items": [{
            "id"            : "vid001",
            "snippet"       : {"title": "Clip", "publishedAt": "2024-01-01T00:00:00Z"},
            "statistics"    : {"viewCount": "80000", "likeCount": "3000", "commentCount": "200"},
            "contentDetails": {"duration": "PT4M"},
        }]}
        mock_client.get_channel_details.return_value = {"items": [{
            "id"        : "UCchan01",
            "snippet"   : {"title": "Artiste", "description": ""},
            "statistics": {"subscriberCount": "20000", "viewCount": "500000", "videoCount": "30"},
            "brandingSettings": {"channel": {}},
        }]}

        searcher = ArtistSearcher(client=mock_client)

        with patch("src.searcher.save_artist", return_value=True), \
             patch("src.searcher.save_video"), \
             patch("src.searcher.save_view_snapshot") as mock_snapshot:

            result = searcher.process_batch(
                video_ids   = ["vid001"],
                channel_ids = ["UCchan01"],
                region      = "CM",
            )

        # Un snapshot doit avoir été enregistré pour vid001
        mock_snapshot.assert_called_once_with("vid001", 80000)
        assert result["saved_videos"] == 1
        assert result["new_artists"]  == 1

    def test_process_batch_empty_returns_zeros(self):
        """Liste vide → aucun traitement, retour immédiat."""
        from src.searcher import ArtistSearcher

        searcher = ArtistSearcher(client=MagicMock())
        result   = searcher.process_batch([], [], "CM")

        assert result == {"saved_videos": 0, "new_artists": 0, "updated_artists": 0}