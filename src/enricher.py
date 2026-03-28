"""
src/enricher.py — Enrichissement des profils via Google Custom Search

Pour chaque artiste qualifié sans données complètes, on effectue
des recherches Google ciblées pour trouver :
    - Le label ou management
    - Le site officiel manquant
    - La présence sur Spotify / Apple Music
    - Des articles de presse récents (validation de notoriété)

Quota Google Custom Search : 100 requêtes gratuites/jour
→ On n'enrichit que les artistes qualifiés sans email
  pour maximiser l'utilité du quota.
"""

import logging
import time
from dataclasses import dataclass, field

import requests

from config import (
    GOOGLE_SEARCH_API_KEY,
    GOOGLE_SEARCH_CX,
    GOOGLE_SEARCH_DAILY_LIMIT,
)
from src.database import get_db
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Délai entre les requêtes Google (évite le rate limiting)
SEARCH_DELAY_SECONDS = 1.0


@dataclass
class EnrichmentResult:
    channel_id  : str
    artist_name : str
    found       : dict = field(default_factory=dict)
    # found peut contenir : website, label, spotify_url,
    #                       apple_music_url, press_article
    queries_used: int  = 0
    success     : bool = False


class GoogleSearchEnricher:
    """
    Enrichit les profils d'artistes via Google Custom Search API.

    Utilisation :
        enricher = GoogleSearchEnricher()
        results  = enricher.enrich_qualified_artists()
    """

    def __init__(self):
        self._quota_used_today = 0

    def enrich_qualified_artists(self) -> list[EnrichmentResult]:
        """
        Enrichit les artistes qualifiés dont les données sont incomplètes.

        Priorise :
            1. Artistes qualifiés sans email (le plus utile)
            2. Artistes qualifiés sans site web
            3. Artistes high_potential en priorité absolue
        """
        if not GOOGLE_SEARCH_API_KEY or not GOOGLE_SEARCH_CX:
            logger.warning(
                "Google Search non configuré — "
                "GOOGLE_SEARCH_API_KEY ou GOOGLE_SEARCH_CX manquant"
            )
            return []

        artists = self._get_artists_to_enrich()
        if not artists:
            logger.info("Aucun artiste à enrichir")
            return []

        logger.info(f"Enrichissement de {len(artists)} artiste(s)...")
        results = []

        for artist in artists:
            # Vérifier le quota avant chaque artiste
            # (chaque artiste peut consommer 2-3 requêtes)
            if self._quota_used_today >= GOOGLE_SEARCH_DAILY_LIMIT - 3:
                logger.warning(
                    f"Quota Google Search épuisé "
                    f"({self._quota_used_today}/{GOOGLE_SEARCH_DAILY_LIMIT})"
                )
                break

            result = self._enrich_artist(artist)
            if result.found:
                self._save_enrichment(result)
            results.append(result)

        enriched = sum(1 for r in results if r.success)
        logger.info(
            f"Enrichissement terminé : {enriched}/{len(results)} artistes enrichis\n"
            f"  Quota utilisé : {self._quota_used_today} requêtes"
        )
        return results

    def _enrich_artist(self, artist: dict) -> EnrichmentResult:
        """
        Lance les recherches Google pour un artiste.
        Stratégie : 2-3 requêtes ciblées plutôt qu'une requête générique.
        """
        name   = artist.get("artist_name", "")
        result = EnrichmentResult(
            channel_id  = artist["channel_id"],
            artist_name = name,
        )

        # Requête 1 : Site officiel + label
        if not artist.get("website") or not artist.get("email"):
            data = self._search(
                f'"{name}" artiste musique contact booking site officiel'
            )
            result.queries_used += 1
            if data:
                self._extract_contact_info(data, result)

        # Requête 2 : Présence sur les plateformes musicales
        spotify_data = self._search(
            f'"{name}" site:open.spotify.com OR site:music.apple.com'
        )
        result.queries_used += 1
        if spotify_data:
            self._extract_platform_links(spotify_data, result)

        # Requête 3 : Articles de presse récents (validation notoriété)
        press_data = self._search(
            f'"{name}" musique africaine 2024 OR 2025',
            date_restrict="y1",   # Dernière année uniquement
        )
        result.queries_used += 1
        if press_data:
            self._extract_press_info(press_data, result)

        result.success = bool(result.found)
        return result

    def _search(
        self,
        query       : str,
        num_results : int = 5,
        date_restrict: str = None,
    ) -> list[dict] | None:
        """
        Effectue une requête Google Custom Search.

        Returns:
            Liste de résultats ou None si erreur/quota dépassé.
        """
        if self._quota_used_today >= GOOGLE_SEARCH_DAILY_LIMIT:
            return None

        params = {
            "key" : GOOGLE_SEARCH_API_KEY,
            "cx"  : GOOGLE_SEARCH_CX,
            "q"   : query,
            "num" : num_results,
        }
        if date_restrict:
            params["dateRestrict"] = date_restrict

        try:
            response = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params  = params,
                timeout = 10,
            )
            self._quota_used_today += 1
            time.sleep(SEARCH_DELAY_SECONDS)

            if response.status_code == 429:
                logger.warning("Google Search : rate limit atteint")
                return None

            response.raise_for_status()
            data = response.json()
            return data.get("items", [])

        except requests.exceptions.Timeout:
            logger.warning(f"Google Search timeout pour : {query}")
            return None
        except Exception as e:
            logger.error(f"Google Search erreur : {e}")
            return None

    # ──────────────────────────────────────────────────────────────────
    # EXTRACTEURS
    # ──────────────────────────────────────────────────────────────────

    def _extract_contact_info(self, items: list, result: EnrichmentResult):
        """Extrait site web, label et email depuis les résultats."""
        import re

        email_pattern = re.compile(
            r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
        )
        # Mots-clés qui indiquent un label ou management
        label_keywords = ["label", "management", "booking", "records", "music group"]

        for item in items:
            url     = item.get("link", "")
            snippet = item.get("snippet", "")
            title   = item.get("title", "")

            # Site officiel : éviter YouTube, Instagram, etc.
            social_domains = (
                "youtube.com", "instagram.com", "facebook.com",
                "twitter.com", "tiktok.com", "wikipedia.org"
            )
            if (url and "website" not in result.found
                    and not any(d in url for d in social_domains)):
                result.found["website"] = url

            # Email dans le snippet
            if "email" not in result.found:
                email_match = email_pattern.search(snippet)
                if email_match:
                    result.found["email"] = email_match.group(0)

            # Label/management
            if "label" not in result.found:
                text_lower = (snippet + " " + title).lower()
                if any(kw in text_lower for kw in label_keywords):
                    result.found["label"] = title

    def _extract_platform_links(self, items: list, result: EnrichmentResult):
        """Extrait les liens Spotify et Apple Music."""
        for item in items:
            url = item.get("link", "")
            if "spotify.com" in url and "spotify_url" not in result.found:
                result.found["spotify_url"] = url
            if "music.apple.com" in url and "apple_music_url" not in result.found:
                result.found["apple_music_url"] = url

    def _extract_press_info(self, items: list, result: EnrichmentResult):
        """Extrait le premier article de presse pertinent."""
        for item in items:
            url     = item.get("link", "")
            title   = item.get("title", "")
            snippet = item.get("snippet", "")

            # Exclure YouTube et les réseaux sociaux
            social_domains = ("youtube.com", "instagram.com", "facebook.com")
            if url and not any(d in url for d in social_domains):
                result.found["press_article"] = {
                    "title"  : title,
                    "url"    : url,
                    "snippet": snippet[:200],
                }
                break   # Un seul article suffit

    # ──────────────────────────────────────────────────────────────────
    # BASE DE DONNÉES
    # ──────────────────────────────────────────────────────────────────

    def _get_artists_to_enrich(self) -> list[dict]:
        """
        Artistes qualifiés dont le profil est incomplet.
        Ordonnés par segment (high_potential en premier).
        """
        with get_db() as conn:
            result = conn.execute(text("""
                SELECT
                    a.channel_id,
                    a.artist_name,
                    a.email,
                    a.website,
                    a.instagram,
                    s.segment
                FROM artists a
                LEFT JOIN LATERAL (
                    SELECT segment FROM scores
                    WHERE channel_id = a.channel_id
                    ORDER BY calculated_at DESC
                    LIMIT 1
                ) s ON true
                WHERE a.status = 'qualified'
                  AND (
                    a.email   IS NULL
                    OR a.website IS NULL
                  )
                ORDER BY
                    CASE s.segment
                        WHEN 'high_potential' THEN 1
                        WHEN 'standard'       THEN 2
                        WHEN 'emerging'       THEN 3
                        ELSE 4
                    END,
                    a.updated_at DESC
                LIMIT 30
            """))
            return [dict(row._mapping) for row in result.fetchall()]

    def _save_enrichment(self, result: EnrichmentResult):
        """Sauvegarde les données enrichies en base."""
        found = result.found
        with get_db() as conn:
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

            # Stocker les données enrichies supplémentaires
            # (label, spotify, apple_music, press) dans une colonne JSONB
            # qu'on ajoute via migration
            if any(k in found for k in
                   ("label", "spotify_url", "apple_music_url", "press_article")):
                conn.execute(text("""
                    UPDATE artists SET
                        enrichment_data = :data::jsonb
                    WHERE channel_id = :channel_id
                """), {
                    "channel_id": result.channel_id,
                    "data"      : __import__("json").dumps({
                        k: found[k] for k in
                        ("label", "spotify_url", "apple_music_url", "press_article")
                        if k in found
                    }),
                })
