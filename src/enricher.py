"""
src/enricher.py — Enrichissement des profils via Google Search + Spotify

Deux sources complémentaires :

    1. GoogleSearchEnricher
       → Liste de sites ciblés (presse africaine, booking, industry)
       → Trouve : email de contact, site officiel, articles de presse
       → Quota : 100 requêtes/jour (gratuit)

    2. SpotifyEnricher
       → API Spotify officielle (Client Credentials Flow)
       → Trouve : label officiel, popularité, lien profil Spotify
       → Confirme l'identité de l'artiste
       → Quota : illimité (pas de quota sur la recherche)

Les deux sont orchestrés par ArtistEnricher qui les appelle
dans l'ordre optimal et sauvegarde les résultats fusionnés.
"""

import logging
import re
import time
from dataclasses import dataclass, field

import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from config import (
    GOOGLE_SEARCH_API_KEY,
    GOOGLE_SEARCH_CX,
    GOOGLE_SEARCH_DAILY_LIMIT,
    SPOTIFY_CLIENT_ID,
    SPOTIFY_CLIENT_SECRET,
)
from src.database import get_db
from sqlalchemy import text

logger = logging.getLogger(__name__)

SEARCH_DELAY_SECONDS = 1.0

# ── Sites ciblés pour le Programmable Search Engine ───────────────────
# À configurer aussi dans la console Google :
#   console.cloud.google.com → Programmable Search Engine → Sites à rechercher
TARGETED_SITES = [
    # Presse musicale africaine
    "pulse.ng", "pulse.cm", "notjustok.com",
    "tooxclusive.com", "bellanaija.com",
    "africanmusic.org", "afrobeats.com",
    "pulsecameroun.com", "pulsecivoire.com",
    # Industry & booking
    "linkedin.com", "reverbnation.com",
    # Plateformes musicales africaines
    "audiomack.com", "boomplay.com",
]


# ──────────────────────────────────────────────────────────────────────
# DATACLASS
# ──────────────────────────────────────────────────────────────────────

@dataclass
class EnrichmentResult:
    channel_id  : str
    artist_name : str
    found       : dict = field(default_factory=dict)
    queries_used: int  = 0
    success     : bool = False


# ──────────────────────────────────────────────────────────────────────
# SPOTIFY ENRICHER
# ──────────────────────────────────────────────────────────────────────

class SpotifyEnricher:
    """
    Enrichit les profils via l'API Spotify officielle.
    Pas de quota — on peut l'appeler sur tous les artistes qualifiés.

    Utilise le Client Credentials Flow (machine-to-machine)
    → pas de login utilisateur requis.
    """

    def __init__(self):
        self._sp = None

    def _get_client(self) -> spotipy.Spotify | None:
        """Initialise le client Spotify (lazy)."""
        if self._sp is not None:
            return self._sp

        if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
            return None

        try:
            auth = SpotifyClientCredentials(
                client_id     = SPOTIFY_CLIENT_ID,
                client_secret = SPOTIFY_CLIENT_SECRET,
            )
            self._sp = spotipy.Spotify(auth_manager=auth)
            return self._sp
        except Exception as e:
            logger.warning(f"Spotify init impossible : {e}")
            return None

    def search_artist(self, artist_name: str) -> dict | None:
        """
        Recherche un artiste sur Spotify par nom.

        Returns:
            Dict avec spotify_url, label, popularity
            ou None si non trouvé / non configuré.
        """
        sp = self._get_client()
        if not sp:
            return None

        try:
            results = sp.search(
                q      = f"artist:{artist_name}",
                type   = "artist",
                limit  = 3,
                market = "FR",   # Marché pour les résultats
            )
            artists = results.get("artists", {}).get("items", [])
            if not artists:
                return None

            # Prendre le premier résultat dont le nom correspond
            # approximativement (évite les faux positifs)
            name_lower = artist_name.lower()
            for artist in artists:
                spotify_name = artist.get("name", "").lower()
                if name_lower in spotify_name or spotify_name in name_lower:
                    return {
                        "spotify_url" : artist.get("external_urls", {})
                                              .get("spotify"),
                        "popularity"  : artist.get("popularity", 0),
                        "genres"      : artist.get("genres", []),
                        "followers"   : artist.get("followers", {})
                                              .get("total", 0),
                    }

        except Exception as e:
            logger.warning(f"Spotify search erreur pour {artist_name} : {e}")

        return None

    def get_artist_label(self, artist_name: str) -> str | None:
        """
        Cherche le label de l'artiste via ses albums sur Spotify.
        Le label est sur l'album, pas sur l'artiste directement.
        """
        sp = self._get_client()
        if not sp:
            return None

        try:
            # Chercher les albums de l'artiste
            results = sp.search(
                q     = f"artist:{artist_name}",
                type  = "album",
                limit = 5,
            )
            albums = results.get("albums", {}).get("items", [])

            for album in albums:
                # Vérifier que l'album correspond bien à cet artiste
                album_artists = [
                    a.get("name", "").lower()
                    for a in album.get("artists", [])
                ]
                if artist_name.lower() in " ".join(album_artists):
                    # Récupérer les détails de l'album pour avoir le label
                    album_detail = sp.album(album["id"])
                    label = album_detail.get("label", "")
                    if label and label.lower() not in (
                        "self-released", "independent", artist_name.lower()
                    ):
                        return label

        except Exception as e:
            logger.warning(f"Spotify label search erreur pour {artist_name} : {e}")

        return None


# ──────────────────────────────────────────────────────────────────────
# GOOGLE SEARCH ENRICHER
# ──────────────────────────────────────────────────────────────────────

class GoogleSearchEnricher:
    """
    Enrichit via Google Programmable Search Engine
    configuré avec une liste de sites ciblés.
    """

    def __init__(self):
        self._quota_used_today = 0

    def search_artist(self, artist_name: str) -> dict:
        """
        Lance 2 requêtes Google ciblées pour un artiste.
        Returns: dict avec website, email, press_article (si trouvés)
        """
        found = {}

        if not GOOGLE_SEARCH_API_KEY or not GOOGLE_SEARCH_CX:
            return found

        if self._quota_used_today >= GOOGLE_SEARCH_DAILY_LIMIT - 2:
            return found

        # Requête 1 : Contact et site officiel
        items = self._search(
            f'"{artist_name}" artiste musique contact booking'
        )
        if items:
            self._extract_contact(items, found)

        # Requête 2 : Articles de presse récents
        press_items = self._search(
            f'"{artist_name}" musique 2024 OR 2025',
            date_restrict="y1",
        )
        if press_items:
            self._extract_press(press_items, found)

        return found

    def _search(
        self,
        query        : str,
        num_results  : int = 5,
        date_restrict: str = None,
    ) -> list[dict] | None:
        if self._quota_used_today >= GOOGLE_SEARCH_DAILY_LIMIT:
            return None

        params = {
            "key": GOOGLE_SEARCH_API_KEY,
            "cx" : GOOGLE_SEARCH_CX,
            "q"  : query,
            "num": num_results,
        }
        if date_restrict:
            params["dateRestrict"] = date_restrict

        try:
            response = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params=params, timeout=10,
            )
            self._quota_used_today += 1
            time.sleep(SEARCH_DELAY_SECONDS)

            if response.status_code == 429:
                logger.warning("Google Search : rate limit")
                return None

            response.raise_for_status()
            return response.json().get("items", [])

        except Exception as e:
            logger.error(f"Google Search erreur : {e}")
            return None

    @staticmethod
    def _extract_contact(items: list, found: dict):
        email_pattern = re.compile(
            r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
        )
        social_domains = (
            "youtube.com", "instagram.com", "facebook.com",
            "twitter.com", "tiktok.com", "wikipedia.org",
        )
        for item in items:
            url     = item.get("link", "")
            snippet = item.get("snippet", "")

            if "website" not in found and url:
                if not any(d in url for d in social_domains):
                    found["website"] = url

            if "email" not in found:
                m = email_pattern.search(snippet)
                if m:
                    found["email"] = m.group(0)

    @staticmethod
    def _extract_press(items: list, found: dict):
        social_domains = ("youtube.com", "instagram.com", "facebook.com")
        for item in items:
            url = item.get("link", "")
            if url and not any(d in url for d in social_domains):
                found["press_article"] = {
                    "title"  : item.get("title", ""),
                    "url"    : url,
                    "snippet": item.get("snippet", "")[:200],
                }
                break


# ──────────────────────────────────────────────────────────────────────
# ORCHESTRATEUR PRINCIPAL
# ──────────────────────────────────────────────────────────────────────

class ArtistEnricher:
    """
    Orchestre l'enrichissement depuis Google Search + Spotify.

    Utilisation :
        enricher = ArtistEnricher()
        results  = enricher.enrich_qualified_artists()
    """

    def __init__(self):
        self.google  = GoogleSearchEnricher()
        self.spotify = SpotifyEnricher()

    def enrich_qualified_artists(self) -> list[EnrichmentResult]:
        """
        Enrichit les artistes qualifiés dont le profil est incomplet.
        Priorité : high_potential en premier.
        """
        artists = self._get_artists_to_enrich()
        if not artists:
            logger.info("Aucun artiste à enrichir")
            return []

        logger.info(f"Enrichissement de {len(artists)} artiste(s)...")
        results = []

        for artist in artists:
            # Stop si quota Google presque épuisé
            if self.google._quota_used_today >= GOOGLE_SEARCH_DAILY_LIMIT - 2:
                logger.warning("Quota Google Search épuisé — arrêt")
                break

            result = self._enrich_one(artist)
            if result.found:
                self._save(result)
            results.append(result)

        enriched = sum(1 for r in results if r.success)
        logger.info(
            f"Enrichissement terminé : {enriched}/{len(results)} enrichis\n"
            f"  Google quota : {self.google._quota_used_today} requêtes"
        )
        return results

    def _enrich_one(self, artist: dict) -> EnrichmentResult:
        """Enrichit un artiste depuis les deux sources."""
        name   = artist.get("artist_name", "")
        result = EnrichmentResult(
            channel_id  = artist["channel_id"],
            artist_name = name,
        )

        # Source 1 : Spotify (pas de quota — toujours en premier)
        spotify_data = self.spotify.search_artist(name)
        if spotify_data:
            result.found.update({
                k: v for k, v in spotify_data.items() if v
            })
            # Chercher le label uniquement si Spotify a trouvé l'artiste
            label = self.spotify.get_artist_label(name)
            if label:
                result.found["label"] = label

        # Source 2 : Google Search (quota limité)
        google_data = self.google.search_artist(name)
        result.queries_used = self.google._quota_used_today

        # Fusion : Google complète ce que Spotify n'a pas trouvé
        for key, value in google_data.items():
            if key not in result.found and value:
                result.found[key] = value

        # Détecter le type de contact depuis les données trouvées
        result.found["contact_type"] = self._detect_contact_type(
            artist, result.found
        )

        result.success = bool(result.found)
        return result

    @staticmethod
    def _detect_contact_type(artist: dict, found: dict) -> str:
        """
        Détecte si le contact trouvé est l'artiste, son manager, ou son label.
        Logique heuristique basée sur l'email et le label trouvé.

        Peut être corrigé manuellement dans HubSpot ensuite.
        """
        email = found.get("email", "") or artist.get("email", "") or ""
        label = found.get("label", "")

        # Email d'un label connu → contact = label
        # On compare chaque mot du nom du label avec l'email
        if label:
            label_words = [
                w for w in label.lower().split() 
                if len(w) > 3  # Ignorer les petits mots (Records, Music...)
            ]
            if any(word in email.lower() for word in label_words):
                return "label"

        # Email de type "booking@", "management@", "promo@"
        management_keywords = ["booking", "management", "promo", "press", "label"]
        email_prefix = email.split("@")[0].lower() if "@" in email else ""
        if any(kw in email_prefix for kw in management_keywords):
            return "manager"

        # Par défaut : on suppose que c'est l'artiste lui-même
        return "artist"

    def _get_artists_to_enrich(self) -> list[dict]:
        with get_db() as conn:
            result = conn.execute(text("""
                SELECT
                    a.channel_id, a.artist_name,
                    a.email, a.website, a.instagram,
                    s.segment
                FROM artists a
                LEFT JOIN LATERAL (
                    SELECT segment FROM scores
                    WHERE channel_id = a.channel_id
                    ORDER BY calculated_at DESC LIMIT 1
                ) s ON true
                WHERE a.status = 'qualified'
                  AND (a.email IS NULL OR a.website IS NULL)
                ORDER BY
                    CASE s.segment
                        WHEN 'high_potential' THEN 1
                        WHEN 'standard'       THEN 2
                        WHEN 'emerging'       THEN 3
                        ELSE 4
                    END,
                    a.updated_at DESC
                LIMIT 45  -- 45 × 2 req Google = 90/100 quota journalier
            """))
            return [dict(row._mapping) for row in result.fetchall()]

    def _save(self, result: EnrichmentResult):
        """Sauvegarde les données enrichies en base."""
        import json
        found = result.found
        with get_db() as conn:
            # Mise à jour des colonnes directes
            conn.execute(text("""
                UPDATE artists SET
                    email      = COALESCE(:email,   email),
                    website    = COALESCE(:website, website),
                    updated_at = NOW()
                WHERE channel_id = :channel_id
            """), {
                "channel_id": result.channel_id,
                "email"     : found.get("email"),
                "website"   : found.get("website"),
            })

            # Enrichment data (label, spotify, presse...)
            enrichment_keys = (
                "label", "spotify_url", "popularity",
                "press_article", "contact_type", "genres",
            )
            enrichment = {k: found[k] for k in enrichment_keys if k in found}
            if enrichment:
                conn.execute(text("""
                    UPDATE artists SET
                        enrichment_data = CAST(:data AS jsonb)
                    WHERE channel_id = :channel_id
                """), {
                    "channel_id": result.channel_id,
                    "data"      : json.dumps(enrichment),
                })