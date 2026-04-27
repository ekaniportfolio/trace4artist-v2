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
            "contentDetails": {"relatedPlaylists": {"uploads": "PL001"}},
        }]}
        # _fetch_recent_videos fait un appel playlist — on le mock
        mock_client.get_playlist_videos.return_value = {"items": []}

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

    def test_fetch_recent_videos_called_for_new_artists(self):
        """
        _fetch_recent_videos doit être appelé uniquement
        pour les nouveaux artistes (is_new = True).
        """
        from src.searcher import ArtistSearcher

        mock_client = MagicMock()
        mock_client.get_video_details.return_value = {"items": [{
            "id"            : "vid001",
            "snippet"       : {"title": "Clip", "publishedAt": "2024-01-01T00:00:00Z"},
            "statistics"    : {"viewCount": "50000", "likeCount": "1000", "commentCount": "50"},
            "contentDetails": {"duration": "PT3M"},
        }]}
        mock_client.get_channel_details.return_value = {"items": [{
            "id"        : "UCnew",
            "snippet"   : {"title": "Nouvel Artiste", "description": ""},
            "statistics": {"subscriberCount": "5000", "viewCount": "100000", "videoCount": "10"},
            "brandingSettings": {"channel": {}},
        }]}

        searcher = ArtistSearcher(client=mock_client)

        with patch("src.searcher.save_artist", return_value=True), \
             patch("src.searcher.save_video"), \
             patch("src.searcher.save_view_snapshot"), \
             patch.object(searcher, "_fetch_recent_videos") as mock_fetch:

            searcher.process_batch(["vid001"], ["UCnew"], "NG")

        # Nouvel artiste → _fetch_recent_videos appelé
        mock_fetch.assert_called_once_with("UCnew")

    def test_fetch_recent_videos_not_called_for_existing_artists(self):
        """
        _fetch_recent_videos ne doit PAS être appelé
        pour les artistes déjà connus (is_new = False).
        """
        from src.searcher import ArtistSearcher

        mock_client = MagicMock()
        mock_client.get_video_details.return_value = {"items": [{
            "id"            : "vid002",
            "snippet"       : {"title": "Clip", "publishedAt": "2024-01-01T00:00:00Z"},
            "statistics"    : {"viewCount": "30000", "likeCount": "800", "commentCount": "30"},
            "contentDetails": {"duration": "PT3M"},
        }]}
        mock_client.get_channel_details.return_value = {"items": [{
            "id"        : "UCexist",
            "snippet"   : {"title": "Artiste Existant", "description": ""},
            "statistics": {"subscriberCount": "8000", "viewCount": "200000", "videoCount": "20"},
            "brandingSettings": {"channel": {}},
            "contentDetails": {"relatedPlaylists": {"uploads": "PL_exist"}},
        }]}

        searcher = ArtistSearcher(client=mock_client)

        with patch("src.searcher.save_artist", return_value=False), \
             patch("src.searcher.save_video"), \
             patch("src.searcher.save_view_snapshot"), \
             patch.object(searcher, "_fetch_recent_videos") as mock_fetch:

            searcher.process_batch(["vid002"], ["UCexist"], "CM")

        # Artiste existant → _fetch_recent_videos PAS appelé
        mock_fetch.assert_not_called()

    def test_fetch_recent_videos_fails_silently(self):
        """
        _fetch_recent_videos ne doit pas planter le scan
        si l'API YouTube retourne une erreur.
        """
        from src.searcher import ArtistSearcher

        mock_client = MagicMock()
        mock_client.get_channel_details.return_value = {"items": [{
            "id": "UCtest",
            "snippet": {"title": "Test", "description": ""},
            "statistics": {"subscriberCount": "1000", "viewCount": "50000", "videoCount": "5"},
            "brandingSettings": {"channel": {}},
        }]}
        mock_client.get_video_details.return_value = {"items": [{
            "id"            : "vid003",
            "snippet"       : {"title": "Test", "publishedAt": "2024-01-01T00:00:00Z"},
            "statistics"    : {"viewCount": "10000", "likeCount": "200", "commentCount": "10"},
            "contentDetails": {"duration": "PT2M"},
        }]}
        # get_playlist_videos lève une erreur → doit être silencieux
        mock_client.get_playlist_videos.side_effect = Exception("API Error")

        searcher = ArtistSearcher(client=mock_client)

        with patch("src.searcher.save_artist", return_value=True), \
             patch("src.searcher.save_video"), \
             patch("src.searcher.save_view_snapshot"):

            # Ne doit pas lever d'exception
            result = searcher.process_batch(["vid003"], ["UCtest"], "KE")

        assert result["saved_videos"] == 1

# ──────────────────────────────────────────────────────────────────────
# TESTS : EXTRACTION DE CONTACTS ENRICHIE
# ──────────────────────────────────────────────────────────────────────

class TestContactExtraction:
    """
    Tests de la nouvelle méthode _extract_contacts qui exploite
    les profileLinks YouTube en plus de la description.
    """

    def test_extracts_instagram_from_description(self):
        """Instagram doit être extrait depuis la description."""
        from src.searcher import ArtistSearcher
        text = "Retrouvez-moi sur https://www.instagram.com/monartiste_officiel"
        contacts = ArtistSearcher._extract_contacts(text)
        assert contacts["instagram"] == "monartiste_officiel"

    def test_extracts_website_from_description(self):
        """Le site officiel doit être extrait depuis la description."""
        from src.searcher import ArtistSearcher
        text = "Site officiel : https://monartiste.cm/booking"
        contacts = ArtistSearcher._extract_contacts(text)
        assert contacts["website"] == "https://monartiste.cm/booking"

    def test_linktree_counts_as_website(self):
        """Linktree est accepté comme site de contact."""
        from src.searcher import ArtistSearcher
        text = "Tous mes liens : https://linktr.ee/monartiste"
        contacts = ArtistSearcher._extract_contacts(text)
        assert contacts["website"] == "https://linktr.ee/monartiste"

    def test_extracts_email_from_description(self):
        """Email extrait depuis la description."""
        from src.searcher import ArtistSearcher
        text = "Pour tout contact : booking@monartiste.cm"
        contacts = ArtistSearcher._extract_contacts(text)
        assert contacts["email"] == "booking@monartiste.cm"

    def test_prefers_pro_email_over_personal(self):
        """Email professionnel (booking@) préféré à email personnel (gmail)."""
        from src.searcher import ArtistSearcher
        text = "perso@gmail.com | booking@label.com"
        contacts = ArtistSearcher._extract_contacts(text)
        assert contacts["email"] == "booking@label.com"

    def test_extracts_obfuscated_email(self):
        """Emails obfusqués (contact[at]domain[dot]com) correctement décodés."""
        from src.searcher import ArtistSearcher
        text = "contact[at]monlabel[dot]com"
        contacts = ArtistSearcher._extract_contacts(text)
        assert contacts["email"] == "contact@monlabel.com"

    def test_instagram_url_preferred_over_handle(self):
        """URL Instagram complète préférée à un simple @handle."""
        from src.searcher import ArtistSearcher
        text = "instagram.com/vrai_handle | @autre_handle"
        contacts = ArtistSearcher._extract_contacts(text)
        assert contacts["instagram"] == "vrai_handle"

    def test_excludes_youtube_internal_emails(self):
        """Emails youtube.com ne doivent pas être extraits."""
        from src.searcher import ArtistSearcher
        text = "noreply@youtube.com"
        contacts = ArtistSearcher._extract_contacts(text)
        assert contacts["email"] is None

    def test_excludes_social_platforms_from_website(self):
        """Instagram, TikTok, Facebook ne comptent pas comme site officiel."""
        from src.searcher import ArtistSearcher
        text = ("https://www.instagram.com/artiste "
                "https://www.tiktok.com/@artiste "
                "https://monartiste.com")
        contacts = ArtistSearcher._extract_contacts(text)
        assert contacts["website"] == "https://monartiste.com"

    def test_returns_none_when_nothing_found(self):
        """Sans contact, retourne None pour chaque champ."""
        from src.searcher import ArtistSearcher
        contacts = ArtistSearcher._extract_contacts(
            "Welcome to my channel! New music every Friday."
        )
        assert contacts["email"]     is None
        assert contacts["website"]   is None
        assert contacts["instagram"] is None