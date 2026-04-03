"""
src/youtube_client.py — Client YouTube Data API v3 avec cache Redis

Différences vs v1 :
    - Cache Redis sur videos.list et channels.list (TTL 6h)
    - Quota loggé en PostgreSQL (via database.py)
    - QuotaExceededError inchangée — même interface qu'en v1
"""

import json
import time

import redis
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import (
    YOUTUBE_API_KEY,
    YOUTUBE_API_SERVICE_NAME,
    YOUTUBE_API_VERSION,
    QUOTA_COST,
    DAILY_QUOTA_LIMIT,
    REQUEST_DELAY_SECONDS,
    REDIS_URL,
    REDIS_CACHE_TTL,
)
from src.database import get_quota_used_today, log_quota_usage


class QuotaExceededError(Exception):
    """Levée quand le quota YouTube journalier est épuisé."""
    pass


class YouTubeClient:
    """
    Client YouTube avec quota management et cache Redis.

    Utilisation :
        client  = YouTubeClient()
        results = client.search_music_videos(region="CM", ...)
    """

    def __init__(self):
        self._service      = None   # Connexion lazy au service Google
        self._redis        = None   # Connexion lazy à Redis

    # ──────────────────────────────────────────────────────────────────
    # CONNEXIONS LAZY
    # ──────────────────────────────────────────────────────────────────

    def _get_service(self):
        """Initialise le service Google API à la première utilisation."""
        if self._service is None:
            self._service = build(
                YOUTUBE_API_SERVICE_NAME,
                YOUTUBE_API_VERSION,
                developerKey=YOUTUBE_API_KEY,
            )
        return self._service

    def _get_redis(self) -> redis.Redis:
        """Initialise la connexion Redis à la première utilisation."""
        if self._redis is None:
            self._redis = redis.from_url(REDIS_URL, decode_responses=True)
        return self._redis

    # ──────────────────────────────────────────────────────────────────
    # CACHE REDIS
    # ──────────────────────────────────────────────────────────────────

    def _cache_get(self, key: str) -> dict | None:
        """
        Tente de récupérer un résultat depuis le cache Redis.
        Retourne None si la clé n'existe pas ou si Redis est indisponible.
        On ne laisse jamais le cache faire planter le programme.
        """
        try:
            value = self._get_redis().get(key)
            return json.loads(value) if value else None
        except Exception:
            return None   # Redis indisponible → on continue sans cache

    def _cache_set(self, key: str, value: dict):
        """
        Stocke un résultat dans Redis avec TTL configuré.
        Silencieux si Redis est indisponible.
        """
        try:
            self._get_redis().setex(
                key,
                REDIS_CACHE_TTL,
                json.dumps(value),
            )
        except Exception:
            pass   # Cache non critique — l'appel a déjà réussi

    # ──────────────────────────────────────────────────────────────────
    # QUOTA
    # ──────────────────────────────────────────────────────────────────

    def _check_quota(self, endpoint: str):
        """Vérifie le quota avant chaque appel API."""
        cost      = QUOTA_COST.get(endpoint, 1)
        used      = get_quota_used_today()
        remaining = DAILY_QUOTA_LIMIT - used

        if cost > remaining:
            raise QuotaExceededError(
                f"Quota insuffisant pour {endpoint} — "
                f"coût : {cost}, restant : {remaining}"
            )

    def _execute(self, endpoint: str, request) -> dict:
        """
        Exécute une requête YouTube avec :
        1. Vérification quota
        2. Exécution
        3. Log de consommation
        4. Délai anti-rate-limiting
        """
        self._check_quota(endpoint)

        try:
            response = request.execute()
            log_quota_usage(endpoint, QUOTA_COST.get(endpoint, 1))
            time.sleep(REQUEST_DELAY_SECONDS)
            return response

        except HttpError as e:
            if e.resp.status == 403:
                raise QuotaExceededError(
                    f"YouTube a refusé la requête (403) — "
                    f"clé invalide ou quota dépassé"
                )
            elif e.resp.status == 400:
                raise ValueError(f"Paramètre invalide : {e}")
            else:
                raise RuntimeError(f"Erreur YouTube API ({e.resp.status}) : {e}")

    # ──────────────────────────────────────────────────────────────────
    # MÉTHODES PUBLIQUES
    # ──────────────────────────────────────────────────────────────────

    def search_music_videos(
        self,
        region          : str,
        published_after : str,
        keywords        : str,
        max_results     : int = 50,
        page_token      : str = None,
    ) -> dict:
        """
        Recherche des clips musicaux (search.list — 100 unités).
        Pas de cache sur search.list : on veut toujours les résultats frais.
        """
        service = self._get_service()
        request = service.search().list(
            part            = "snippet",
            type            = "video",
            videoCategoryId = "10",
            regionCode      = region,
            publishedAfter  = published_after,
            q               = keywords,
            maxResults      = max_results,
            order           = "date",
            pageToken       = page_token,
        )
        return self._execute("search.list", request)

    def get_video_details(self, video_ids: list) -> dict:
        """
        Détails de vidéos (videos.list — 1 unité).
        Cache Redis : TTL 6h. Un video_id déjà vu ce cycle
        ne coûte aucune unité de quota.
        """
        if not video_ids:
            return {"items": []}

        cache_key = f"videos:{','.join(sorted(video_ids[:50]))}"
        cached    = self._cache_get(cache_key)
        if cached:
            return cached

        ids_str = ",".join(video_ids[:50])
        service = self._get_service()
        request = service.videos().list(
            part = "statistics,contentDetails,snippet",
            id   = ids_str,
        )
        response = self._execute("videos.list", request)
        self._cache_set(cache_key, response)
        return response

    def get_channel_details(self, channel_ids: list) -> dict:
        """
        Infos de chaînes (channels.list — 1 unité).
        Inclut contentDetails pour accéder à la playlist uploads.
        Cache Redis : TTL 6h. Les infos de chaîne changent rarement
        — inutile de les refetcher à chaque scan.
        """
        if not channel_ids:
            return {"items": []}

        cache_key = f"channels:{','.join(sorted(channel_ids[:50]))}"
        cached    = self._cache_get(cache_key)
        if cached:
            return cached

        ids_str = ",".join(channel_ids[:50])
        service = self._get_service()
        request = service.channels().list(
            part = "snippet,statistics,brandingSettings,topicDetails,contentDetails",
            id   = ids_str,
        )
        response = self._execute("channels.list", request)
        self._cache_set(cache_key, response)
        return response

    def get_playlist_videos(
        self,
        playlist_id: str,
        max_results : int = 5,
    ) -> dict:
        """
        Récupère les dernières vidéos d'une playlist (playlistItems.list — 1 unité).
        Utilisé pour récupérer les vidéos récentes d'un artiste via sa
        playlist uploads, afin de calculer la régularité dès le premier scoring.
        """
        cache_key = f"playlist:{playlist_id}:{max_results}"
        cached    = self._cache_get(cache_key)
        if cached:
            return cached

        service = self._get_service()
        request = service.playlistItems().list(
            part       = "contentDetails",
            playlistId = playlist_id,
            maxResults = min(max_results, 10),
        )
        response = self._execute("playlistItems.list", request)
        # Cache court — 1h car les nouvelles vidéos apparaissent souvent
        self._cache_set(cache_key, response, ttl=3600)
        return response

    def get_quota_status(self) -> dict:
        """Résumé du quota consommé aujourd'hui."""
        used      = get_quota_used_today()
        remaining = DAILY_QUOTA_LIMIT - used
        return {
            "used"              : used,
            "remaining"         : remaining,
            "limit"             : DAILY_QUOTA_LIMIT,
            "searches_remaining": remaining // QUOTA_COST["search.list"],
        }