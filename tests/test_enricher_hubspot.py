"""
tests/test_enricher_hubspot.py — Tests de l'enrichissement et HubSpot

On teste :
    - L'extraction de contacts depuis les résultats Google
    - L'extraction des liens plateformes (Spotify, Apple Music)
    - La gestion du quota Google (arrêt propre)
    - La construction des propriétés HubSpot
    - La logique create vs update (par email ou par hubspot_contact_id)
    - La gestion des erreurs API HubSpot

Aucun vrai appel Google ou HubSpot n'est effectué.
"""

import pytest
from unittest.mock import patch, MagicMock

from src.enricher import GoogleSearchEnricher, EnrichmentResult
from src.hubspot_client import HubSpotClient, SyncResult


# ──────────────────────────────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def enricher():
    e = GoogleSearchEnricher()
    return e


@pytest.fixture
def sample_artist():
    return {
        "channel_id"      : "UCtest123",
        "artist_name"     : "Artiste Test",
        "email"           : None,
        "website"         : None,
        "country"         : "CM",
        "subscriber_count": 15_000,
        "score"           : 72.5,
        "segment"         : "standard",
        "hubspot_contact_id": None,
        "enrichment_data" : None,
    }


@pytest.fixture
def google_results():
    """Résultats Google réalistes."""
    return [
        {
            "link"   : "https://artistetest.cm",
            "title"  : "Artiste Test — Site Officiel",
            "snippet": "Booking et management : booking@artistetest.cm",
        },
        {
            "link"   : "https://open.spotify.com/artist/abc123",
            "title"  : "Artiste Test sur Spotify",
            "snippet": "Écoutez Artiste Test sur Spotify.",
        },
    ]


# ──────────────────────────────────────────────────────────────────────
# TESTS : EXTRACTION DE CONTACTS
# ──────────────────────────────────────────────────────────────────────

class TestContactExtraction:

    def test_extracts_website_from_results(self, enricher, google_results):
        result = EnrichmentResult(channel_id="UC1", artist_name="Test")
        enricher._extract_contact_info(google_results, result)
        assert result.found.get("website") == "https://artistetest.cm"

    def test_extracts_email_from_snippet(self, enricher, google_results):
        result = EnrichmentResult(channel_id="UC1", artist_name="Test")
        enricher._extract_contact_info(google_results, result)
        assert result.found.get("email") == "booking@artistetest.cm"

    def test_ignores_social_media_as_website(self, enricher):
        items = [{
            "link"   : "https://instagram.com/artiste",
            "title"  : "Artiste sur Instagram",
            "snippet": "",
        }]
        result = EnrichmentResult(channel_id="UC1", artist_name="Test")
        enricher._extract_contact_info(items, result)
        assert "website" not in result.found

    def test_extracts_spotify_link(self, enricher, google_results):
        result = EnrichmentResult(channel_id="UC1", artist_name="Test")
        enricher._extract_platform_links(google_results, result)
        assert result.found.get("spotify_url") == \
               "https://open.spotify.com/artist/abc123"

    def test_no_results_leaves_found_empty(self, enricher):
        result = EnrichmentResult(channel_id="UC1", artist_name="Test")
        enricher._extract_contact_info([], result)
        assert result.found == {}


# ──────────────────────────────────────────────────────────────────────
# TESTS : GESTION DU QUOTA GOOGLE
# ──────────────────────────────────────────────────────────────────────

class TestGoogleQuota:

    def test_search_returns_none_when_quota_exhausted(self, enricher):
        """Quota Google épuisé → _search retourne None."""
        enricher._quota_used_today = 100  # quota max atteint
        result = enricher._search("artiste test")
        assert result is None

    def test_enrichment_stops_near_quota_limit(self, enricher):
        """
        Quand le quota est presque épuisé (< 3 requêtes restantes),
        l'enrichissement s'arrête sans lever d'exception.
        """
        enricher._quota_used_today = 98  # 2 requêtes restantes

        artists = [
            {"channel_id": "UC1", "artist_name": "A1",
             "email": None, "website": None, "segment": "standard"},
            {"channel_id": "UC2", "artist_name": "A2",
             "email": None, "website": None, "segment": "standard"},
        ]

        with patch.object(enricher, "_get_artists_to_enrich",
                          return_value=artists), \
             patch.object(enricher, "_save_enrichment"), \
             patch("src.enricher.GOOGLE_SEARCH_API_KEY", "fake_key"), \
             patch("src.enricher.GOOGLE_SEARCH_CX", "fake_cx"):

            results = enricher.enrich_qualified_artists()

        # On doit s'être arrêté avant de traiter tous les artistes
        assert len(results) < len(artists)

    def test_returns_empty_when_not_configured(self, enricher):
        """Sans clé API → retour immédiat sans appel réseau."""
        with patch("src.enricher.GOOGLE_SEARCH_API_KEY", ""), \
             patch("src.enricher.GOOGLE_SEARCH_CX", ""):
            results = enricher.enrich_qualified_artists()

        assert results == []


# ──────────────────────────────────────────────────────────────────────
# TESTS : HUBSPOT — CONSTRUCTION DES PROPRIÉTÉS
# ──────────────────────────────────────────────────────────────────────

class TestHubSpotProperties:

    def test_builds_required_properties(self, sample_artist):
        """Les propriétés obligatoires doivent toujours être présentes."""
        with patch("src.hubspot_client.HUBSPOT_API_KEY", "pat-fake"):
            client = HubSpotClient()

        props = client._build_properties(sample_artist)

        assert "artist_name"         in props
        assert "youtube_channel_id"  in props
        assert "youtube_channel_url" in props
        assert "source_platform"     in props
        assert props["source_platform"] == "YouTube"

    def test_youtube_url_built_from_channel_id(self, sample_artist):
        with patch("src.hubspot_client.HUBSPOT_API_KEY", "pat-fake"):
            client = HubSpotClient()

        props = client._build_properties(sample_artist)
        assert props["youtube_channel_url"] == \
               "https://youtube.com/channel/UCtest123"

    def test_empty_values_excluded(self, sample_artist):
        """Les valeurs vides ne doivent pas être envoyées à HubSpot."""
        with patch("src.hubspot_client.HUBSPOT_API_KEY", "pat-fake"):
            client = HubSpotClient()

        sample_artist["email"] = None
        props = client._build_properties(sample_artist)

        assert "email" not in props

    def test_enrichment_data_included(self, sample_artist):
        """Les données enrichies par Google doivent être dans les props."""
        sample_artist["enrichment_data"] = {
            "spotify_url"    : "https://open.spotify.com/artist/xyz",
            "apple_music_url": "https://music.apple.com/artist/xyz",
            "label"          : "African Heat Records",
        }
        with patch("src.hubspot_client.HUBSPOT_API_KEY", "pat-fake"):
            client = HubSpotClient()

        props = client._build_properties(sample_artist)

        assert props.get("spotify_url")     == "https://open.spotify.com/artist/xyz"
        assert props.get("apple_music_url") == "https://music.apple.com/artist/xyz"
        assert props.get("label")           == "African Heat Records"


# ──────────────────────────────────────────────────────────────────────
# TESTS : HUBSPOT — LOGIQUE CREATE / UPDATE
# ──────────────────────────────────────────────────────────────────────

class TestHubSpotSync:

    def _make_client(self):
        with patch("src.hubspot_client.HUBSPOT_API_KEY", "pat-fake"):
            client = HubSpotClient()
        client._client = MagicMock()
        return client

    def test_creates_new_contact_when_no_existing_id(self, sample_artist):
        """Artiste sans hubspot_contact_id → création."""
        client = self._make_client()

        mock_contact    = MagicMock()
        mock_contact.id = "hs_001"
        client._client.crm.contacts.basic_api.create.return_value = mock_contact

        with patch.object(client, "_find_by_email", return_value=None), \
             patch.object(client, "_save_hubspot_id") as mock_save:

            result = client._sync_artist(sample_artist)

        assert result.action     == "created"
        assert result.hubspot_id == "hs_001"
        mock_save.assert_called_once_with("UCtest123", "hs_001")

    def test_updates_existing_contact_by_hubspot_id(self, sample_artist):
        """Artiste avec hubspot_contact_id → mise à jour."""
        client = self._make_client()
        sample_artist["hubspot_contact_id"] = "hs_existing"

        result = client._sync_artist(sample_artist)

        client._client.crm.contacts.basic_api.update.assert_called_once()
        assert result.action     == "updated"
        assert result.hubspot_id == "hs_existing"

    def test_updates_by_email_if_contact_exists(self, sample_artist):
        """Email déjà dans HubSpot → mise à jour sans créer de doublon."""
        client = self._make_client()
        sample_artist["email"] = "contact@artiste.cm"

        with patch.object(client, "_find_by_email", return_value="hs_found"), \
             patch.object(client, "_save_hubspot_id"):

            result = client._sync_artist(sample_artist)

        assert result.action     == "updated"
        assert result.hubspot_id == "hs_found"
        client._client.crm.contacts.basic_api.create.assert_not_called()

    def test_error_captured_without_crashing(self, sample_artist):
        """Une erreur HubSpot ne doit pas planter la sync globale."""
        from hubspot.crm.contacts.exceptions import ApiException

        client = self._make_client()
        client._client.crm.contacts.basic_api.create.side_effect = \
            ApiException(status=400, reason="Bad Request")

        with patch.object(client, "_find_by_email", return_value=None):
            result = client._sync_artist(sample_artist)

        assert result.action == "error"
        assert result.error  is not None

    def test_skips_when_no_artists_to_sync(self):
        """Aucun artiste qualifié → retour immédiat."""
        with patch("src.hubspot_client.HUBSPOT_API_KEY", "pat-fake"):
            client = HubSpotClient()

        with patch.object(client, "_get_artists_to_sync", return_value=[]):
            count = client.sync_qualified_artists()

        assert count == 0
