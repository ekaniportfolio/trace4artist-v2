"""
tests/test_enricher_hubspot.py — Tests enrichissement et HubSpot

Couvre :
    - SpotifyEnricher : détection du type de contact
    - GoogleSearchEnricher : extraction contact/presse
    - ArtistEnricher : gestion quota, fusion des sources
    - HubSpotClient : propriétés natives + 10 custom, create/update
"""

import pytest
from unittest.mock import patch, MagicMock

from src.enricher import (
    ArtistEnricher, GoogleSearchEnricher,
    SpotifyEnricher, EnrichmentResult,
)
from src.hubspot_client import HubSpotClient, SyncResult


# ──────────────────────────────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def google_enricher():
    return GoogleSearchEnricher()


@pytest.fixture
def spotify_enricher():
    return SpotifyEnricher()


@pytest.fixture
def artist_enricher():
    e = ArtistEnricher()
    e.google  = MagicMock()
    e.spotify = MagicMock()
    # Initialiser l'attribut int explicitement — MagicMock() retournerait
    # un Mock à la place d'un int, ce qui casse les comparaisons >= 
    e.google._quota_used_today = 0
    return e


@pytest.fixture
def sample_artist():
    return {
        "channel_id"         : "UCtest123",
        "artist_name"        : "Artiste Test",
        "email"              : None,
        "website"            : None,
        "country"            : "CM",
        "subscriber_count"   : 15_000,
        "score"              : 72.5,
        "segment"            : "standard",
        "hubspot_contact_id" : None,
        "enrichment_data"    : None,
        "video_views"        : 50_000,
        "latest_video_url"   : "https://youtube.com/watch?v=abc",
        "criteria_breakdown" : {"spr": 15.0, "engagement": 12.0,
                                "velocity_24h": 8.0, "velocity_7d": 5.0,
                                "regularity": 8.0, "channel": 7.0,
                                "web_contact": 2.5},
    }


@pytest.fixture
def google_results():
    return [
        {
            "link"   : "https://artistetest.cm",
            "title"  : "Artiste Test — Site Officiel",
            "snippet": "Booking : booking@artistetest.cm | Management Trace",
        },
    ]


# ──────────────────────────────────────────────────────────────────────
# TESTS : GOOGLE SEARCH ENRICHER
# ──────────────────────────────────────────────────────────────────────

class TestGoogleExtraction:

    def test_extracts_website(self, google_enricher, google_results):
        found = {}
        google_enricher._extract_contact(google_results, found)
        assert found.get("website") == "https://artistetest.cm"

    def test_extracts_email_from_snippet(self, google_enricher, google_results):
        found = {}
        google_enricher._extract_contact(google_results, found)
        assert found.get("email") == "booking@artistetest.cm"

    def test_ignores_social_media_as_website(self, google_enricher):
        items = [{"link": "https://instagram.com/artiste",
                  "title": "", "snippet": ""}]
        found = {}
        google_enricher._extract_contact(items, found)
        assert "website" not in found

    def test_returns_empty_when_not_configured(self, google_enricher):
        with patch("src.enricher.GOOGLE_SEARCH_API_KEY", ""), \
             patch("src.enricher.GOOGLE_SEARCH_CX", ""):
            result = google_enricher.search_artist("Artiste Test")
        assert result == {}

    def test_stops_when_quota_exhausted(self, google_enricher):
        google_enricher._quota_used_today = 100
        result = google_enricher._search("test")
        assert result is None


# ──────────────────────────────────────────────────────────────────────
# TESTS : DÉTECTION DU TYPE DE CONTACT
# ──────────────────────────────────────────────────────────────────────

class TestContactTypeDetection:

    def test_detects_manager_from_email_prefix(self, artist_enricher):
        artist = {"channel_id": "UC1", "email": None}
        found  = {"email": "booking@songsong.cm"}
        result = artist_enricher._detect_contact_type(artist, found)
        assert result == "manager"

    def test_detects_label_from_email(self, artist_enricher):
        artist = {"channel_id": "UC1", "email": None}
        found  = {
            "label": "Afrobeat Records",
            "email": "contact@afrobeatrecords.cm",
        }
        result = artist_enricher._detect_contact_type(artist, found)
        assert result == "label"

    def test_defaults_to_artist(self, artist_enricher):
        artist = {"channel_id": "UC1", "email": None}
        found  = {"email": "monartiste@gmail.com"}
        result = artist_enricher._detect_contact_type(artist, found)
        assert result == "artist"

    def test_management_keyword_detected(self, artist_enricher):
        artist = {"channel_id": "UC1", "email": None}
        found  = {"email": "management@artiste.cm"}
        result = artist_enricher._detect_contact_type(artist, found)
        assert result == "manager"


# ──────────────────────────────────────────────────────────────────────
# TESTS : ARTIST ENRICHER ORCHESTRATION
# ──────────────────────────────────────────────────────────────────────

class TestArtistEnricher:

    def test_spotify_called_first(self, artist_enricher):
        """Spotify est appelé avant Google (pas de quota)."""
        artist_enricher.spotify.search_artist.return_value = None
        artist_enricher.spotify.get_artist_label.return_value = None
        artist_enricher.google.search_artist.return_value = {}

        with patch.object(artist_enricher, "_get_artists_to_enrich",
                          return_value=[{
                              "channel_id": "UC1",
                              "artist_name": "Test",
                              "email": None, "website": None,
                              "segment": "standard",
                          }]), \
             patch.object(artist_enricher, "_save"):

            artist_enricher.enrich_qualified_artists()

        artist_enricher.spotify.search_artist.assert_called_once_with("Test")

    def test_google_fills_missing_data(self, artist_enricher):
        """
        Google complète ce que Spotify n'a pas trouvé
        (ex: Spotify n'a pas l'email, Google l'a).
        """
        artist_enricher.spotify.search_artist.return_value = {
            "spotify_url": "https://open.spotify.com/artist/xyz",
            "popularity" : 45,
        }
        artist_enricher.spotify.get_artist_label.return_value = None
        artist_enricher.google.search_artist.return_value = {
            "email"  : "contact@artiste.cm",
            "website": "https://artiste.cm",
        }

        with patch.object(artist_enricher, "_get_artists_to_enrich",
                          return_value=[{
                              "channel_id": "UC1", "artist_name": "Test",
                              "email": None, "website": None, "segment": "standard",
                          }]), \
             patch.object(artist_enricher, "_save") as mock_save:

            results = artist_enricher.enrich_qualified_artists()

        assert results[0].found.get("email")       == "contact@artiste.cm"
        assert results[0].found.get("spotify_url") == \
               "https://open.spotify.com/artist/xyz"

    def test_returns_empty_when_no_artists(self, artist_enricher):
        with patch.object(artist_enricher, "_get_artists_to_enrich",
                          return_value=[]):
            results = artist_enricher.enrich_qualified_artists()
        assert results == []


# ──────────────────────────────────────────────────────────────────────
# TESTS : HUBSPOT PROPRIÉTÉS
# ──────────────────────────────────────────────────────────────────────

class TestHubSpotProperties:

    def _make_client(self):
        with patch("src.hubspot_client.HUBSPOT_API_KEY", "pat-fake"):
            client = HubSpotClient()
        client._client = MagicMock()
        return client

    def test_uses_native_firstname_for_artist_name(self, sample_artist):
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        assert props.get("firstname") == "Artiste Test"
        assert "artist_name" not in props   # Pas de custom prop redondante

    def test_uses_native_company_for_label(self, sample_artist):
        sample_artist["enrichment_data"] = {"label": "Afrobeat Records"}
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        assert props.get("company") == "Afrobeat Records"

    def test_contact_type_included(self, sample_artist):
        """contact_type doit être en Title Case pour HubSpot (CONTACT_TYPE_MAP)."""
        sample_artist["enrichment_data"] = {"contact_type": "manager"}
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        assert props.get("contact_type") == "Manager"   # Title Case, pas "manager"

    def test_contact_type_artist_default(self, sample_artist):
        """Sans contact_type, la valeur par défaut est 'Artist'."""
        sample_artist["enrichment_data"] = {}
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        assert props.get("contact_type") == "Artist"

    def test_prospect_segment_title_case(self, sample_artist):
        """prospect_segment doit être en Title Case (SEGMENT_MAP)."""
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        # sample_artist["segment"] = "standard" → doit devenir "Standard"
        assert props.get("prospect_segment") == "Standard"

    def test_prospect_segment_high_potential(self, sample_artist):
        """high_potential → 'High' (pas 'High_Potential')."""
        sample_artist["segment"] = "high_potential"
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        assert props.get("prospect_segment") == "High"

    def test_source_platform_casing(self, sample_artist):
        """source_platform doit être 'Youtube' et non 'YouTube'."""
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        assert props.get("source_platform") == "Youtube"

    def test_spr_score_read_from_criteria_breakdown(self, sample_artist):
        """spr_score doit être lu depuis criteria_breakdown, pas depuis artist."""
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        # criteria_breakdown["spr"] = 15.0 → spr_score = "15.0"
        assert props.get("spr_score") == "15.0"

    def test_spr_score_zero_when_no_breakdown(self, sample_artist):
        """Sans criteria_breakdown, spr_score doit valoir '0.0' (exclu du push)."""
        sample_artist["criteria_breakdown"] = None
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        # "0.0" est filtré par {k: v if v and v != "0.0"}
        assert "spr_score" not in props

    def test_exactly_10_custom_properties_max(self, sample_artist):
        """On ne doit pas dépasser 10 propriétés custom."""
        from src.hubspot_client import HUBSPOT_CUSTOM_PROPERTIES
        assert len(HUBSPOT_CUSTOM_PROPERTIES) == 10

    def test_youtube_url_built_correctly(self, sample_artist):
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        assert props["youtube_channel_url"] == \
               "https://youtube.com/channel/UCtest123"

    def test_empty_values_excluded(self, sample_artist):
        sample_artist["email"] = None
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        assert "email" not in props

    def test_no_spotify_apple_music_properties(self, sample_artist):
        """Ces propriétés ont été retirées des custom props HubSpot."""
        client = self._make_client()
        props  = client._build_properties(sample_artist)
        assert "spotify_url"     not in props
        assert "apple_music_url" not in props


# ──────────────────────────────────────────────────────────────────────
# TESTS : HUBSPOT SYNC CREATE / UPDATE
# ──────────────────────────────────────────────────────────────────────

class TestHubSpotSync:

    def _make_client(self):
        with patch("src.hubspot_client.HUBSPOT_API_KEY", "pat-fake"):
            client = HubSpotClient()
        client._client = MagicMock()
        return client

    def test_creates_new_contact(self, sample_artist):
        client              = self._make_client()
        mock_contact        = MagicMock()
        mock_contact.id     = "hs_001"
        client._client.crm.contacts.basic_api.create.return_value = mock_contact

        with patch.object(client, "_find_by_email", return_value=None), \
             patch.object(client, "_save_hubspot_id"):
            result = client._sync_artist(sample_artist)

        assert result.action     == "created"
        assert result.hubspot_id == "hs_001"

    def test_updates_by_existing_hubspot_id(self, sample_artist):
        sample_artist["hubspot_contact_id"] = "hs_existing"
        client = self._make_client()
        result = client._sync_artist(sample_artist)

        client._client.crm.contacts.basic_api.update.assert_called_once()
        assert result.action     == "updated"
        assert result.hubspot_id == "hs_existing"

    def test_updates_by_email_no_duplicate(self, sample_artist):
        sample_artist["email"] = "contact@artiste.cm"
        client = self._make_client()

        with patch.object(client, "_find_by_email", return_value="hs_found"), \
             patch.object(client, "_save_hubspot_id"):
            result = client._sync_artist(sample_artist)

        assert result.action == "updated"
        client._client.crm.contacts.basic_api.create.assert_not_called()

    def test_error_captured_without_crashing(self, sample_artist):
        from hubspot.crm.contacts.exceptions import ApiException
        client = self._make_client()
        client._client.crm.contacts.basic_api.create.side_effect = \
            ApiException(status=400, reason="Bad Request")

        with patch.object(client, "_find_by_email", return_value=None):
            result = client._sync_artist(sample_artist)

        assert result.action == "error"
        assert result.error  is not None

    def test_returns_zero_when_no_artists(self):
        with patch("src.hubspot_client.HUBSPOT_API_KEY", "pat-fake"):
            client = HubSpotClient()
        with patch.object(client, "_get_artists_to_sync", return_value=[]):
            count = client.sync_qualified_artists()
        assert count == 0