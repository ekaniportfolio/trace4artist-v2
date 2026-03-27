"""
src/searcher.py — Module de recherche d'artistes africains v2

Différences vs v1 :
    - Sauvegarde dans PostgreSQL (via database.py SQLAlchemy)
    - Snapshot des vues à chaque passage (pour la vélocité)
    - Utilisé par _scan_region() dans scheduler.py
    - Extraction de contacts identique à v1 (regex éprouvées)
"""

import re
from src.database import save_artist, save_video, save_view_snapshot
from src.youtube_client import YouTubeClient, QuotaExceededError
from config import SEARCH_KEYWORDS, MAX_RESULTS_PER_SEARCH


# Patterns de contact — identiques à v1, validés sur de vraies données
EMAIL_PATTERN     = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
INSTAGRAM_PATTERN = re.compile(r"(?:instagram\.com/|(?<!\w)@)([\w.]+)", re.IGNORECASE)
WEBSITE_PATTERN   = re.compile(
    r"https?://(?!(?:www\.)?(?:youtube|instagram|facebook|twitter|tiktok))"
    r"[\w\-.]+"
)


class ArtistSearcher:
    """
    Recherche et sauvegarde les artistes détectés sur YouTube.

    Utilisé de deux façons :
        1. Par _scan_region() dans scheduler.py (pipeline automatique)
        2. Par fetch_video_details() dans worker.py (traitement Celery)
    """

    def __init__(self, client: YouTubeClient = None):
        self.client = client or YouTubeClient()

    def process_batch(
        self,
        video_ids  : list,
        channel_ids: list,
        region     : str,
    ) -> dict:
        """
        Traite un batch de vidéos et chaînes YouTube :
        1. Récupère les détails (avec cache Redis)
        2. Parse les données
        3. Sauvegarde en PostgreSQL
        4. Enregistre un snapshot des vues

        Appelé par la tâche Celery fetch_video_details.

        Returns:
            dict avec saved_videos, new_artists, updated_artists
        """
        if not video_ids:
            return {"saved_videos": 0, "new_artists": 0, "updated_artists": 0}

        # Appels groupés — économie de quota maximale
        videos_response   = self.client.get_video_details(video_ids)
        channels_response = self.client.get_channel_details(
            list(set(channel_ids))
        )

        videos_map   = self._index_by_id(videos_response.get("items", []))
        channels_map = self._index_by_id(channels_response.get("items", []))

        saved_videos    = 0
        new_artists     = 0
        updated_artists = 0
        seen_channels   = set()

        for video_id, channel_id in zip(video_ids, channel_ids):

            # ── Artiste ──────────────────────────────────────────────
            if channel_id not in seen_channels:
                channel_data = channels_map.get(channel_id, {})
                artist_data  = self._parse_artist(channel_id, region, channel_data)
                is_new       = save_artist(artist_data)

                if is_new:
                    new_artists += 1
                else:
                    updated_artists += 1
                seen_channels.add(channel_id)

            # ── Vidéo ─────────────────────────────────────────────────
            video_data = videos_map.get(video_id)
            if video_data:
                parsed = self._parse_video(video_id, channel_id, video_data)
                save_video(parsed)

                # Snapshot des vues pour calcul de vélocité futur
                save_view_snapshot(video_id, parsed["view_count"])
                saved_videos += 1

        return {
            "saved_videos"   : saved_videos,
            "new_artists"    : new_artists,
            "updated_artists": updated_artists,
        }

    def search_region(self, region: str, published_after: str) -> tuple[list, list]:
        """
        Lance search.list pour un pays et retourne les IDs.
        Appelé par _scan_region() dans scheduler.py.

        Returns:
            (video_ids, channel_ids) — listes parallèles
        """
        try:
            response = self.client.search_music_videos(
                region          = region,
                published_after = published_after,
                keywords        = SEARCH_KEYWORDS,
                max_results     = MAX_RESULTS_PER_SEARCH,
            )
        except QuotaExceededError:
            raise
        except Exception as e:
            raise RuntimeError(f"[{region}] Erreur search : {e}")

        items       = response.get("items", [])
        video_ids   = [
            i["id"]["videoId"]
            for i in items if i.get("id", {}).get("videoId")
        ]
        channel_ids = [
            i["snippet"]["channelId"]
            for i in items if i.get("snippet", {}).get("channelId")
        ]
        return video_ids, channel_ids

    # ──────────────────────────────────────────────────────────────────
    # PARSERS
    # ──────────────────────────────────────────────────────────────────

    def _parse_artist(self, channel_id: str, region: str, channel: dict) -> dict:
        snippet  = channel.get("snippet", {})
        stats    = channel.get("statistics", {})
        description = snippet.get("description", "")

        return {
            "channel_id"      : channel_id,
            "artist_name"     : snippet.get("title", ""),
            "country"         : region,
            "description"     : description[:500] if description else "",
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "total_views"     : int(stats.get("viewCount", 0)),
            "video_count"     : int(stats.get("videoCount", 0)),
            "email"           : self._extract_email(description),
            "website"         : self._extract_website(description),
            "instagram"       : self._extract_instagram(description),
        }

    def _parse_video(self, video_id: str, channel_id: str, video: dict) -> dict:
        snippet = video.get("snippet", {})
        stats   = video.get("statistics", {})
        details = video.get("contentDetails", {})

        return {
            "video_id"    : video_id,
            "channel_id"  : channel_id,
            "title"       : snippet.get("title", ""),
            "view_count"  : int(stats.get("viewCount", 0)),
            "like_count"  : int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
            "published_at": snippet.get("publishedAt"),
            "duration"    : details.get("duration", ""),
        }

    # ──────────────────────────────────────────────────────────────────
    # UTILITAIRES
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _index_by_id(items: list) -> dict:
        return {item["id"]: item for item in items if item.get("id")}

    @staticmethod
    def _extract_email(text: str) -> str | None:
        match = EMAIL_PATTERN.search(text)
        return match.group(0) if match else None

    @staticmethod
    def _extract_instagram(text: str) -> str | None:
        match = INSTAGRAM_PATTERN.search(text)
        return match.group(1) if match else None

    @staticmethod
    def _extract_website(text: str) -> str | None:
        match = WEBSITE_PATTERN.search(text)
        return match.group(0) if match else None