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
                    # Récupérer les 5 dernières vidéos de la chaîne pour
                    # permettre le calcul de régularité dès le premier scoring
                    self._fetch_recent_videos(channel_id)
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

    def search_region(self, region: str, published_after: str, max_results: int = None, keywords: str = None) -> tuple[list, list]:
        """
        Lance search.list pour un pays et retourne les IDs.
        max_results et keywords sont lus depuis SettingsManager si non fournis.
        Valeurs par défaut issues de config.py.

        Returns:
            (video_ids, channel_ids) — listes parallèles
        """
        try:
            response = self.client.search_music_videos(
                region          = region,
                published_after = published_after,
                keywords        = keywords or SEARCH_KEYWORDS,
                max_results     = max_results or MAX_RESULTS_PER_SEARCH,
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

    def _fetch_recent_videos(self, channel_id: str, max_videos: int = 5):
        """
        Récupère les N dernières vidéos d'une chaîne.
        Permet au scorer de calculer la régularité des publications
        dès le premier passage (évite regularity = 0 par manque de données).

        Coût quota : 1 unité (playlistItems.list) + 1 unité (videos.list)

        Note : la playlist uploads d'une chaîne YouTube suit toujours la règle :
            channel_id  = UC + suffix  (ex. UCxxxxxxxxxxxxxxxxxxxxxxxx)
            uploads_id  = UU + suffix  (ex. UUxxxxxxxxxxxxxxxxxxxxxxxx)
        On dérive directement l'ID sans appel API supplémentaire — ce qui
        évite le bug du cache Redis qui retournait d'anciennes réponses
        sans le champ contentDetails.
        """
        try:
            # Dériver l'uploads playlist ID directement depuis channel_id
            # Règle YouTube : UCxxxxxx → UUxxxxxx (UC → UU)
            if not channel_id.startswith("UC"):
                return
            uploads_playlist = "UU" + channel_id[2:]

            # Récupérer les dernières vidéos via la playlist uploads
            playlist_resp = self.client.get_playlist_videos(
                playlist_id = uploads_playlist,
                max_results = max_videos,
            )
            items = playlist_resp.get("items", [])
            if not items:
                return

            recent_video_ids = [
                item["contentDetails"]["videoId"]
                for item in items
                if item.get("contentDetails", {}).get("videoId")
            ]
            if not recent_video_ids:
                return

            # Récupérer les stats de ces vidéos et les sauvegarder
            videos_resp = self.client.get_video_details(recent_video_ids)
            for item in videos_resp.get("items", []):
                vid_id = item["id"]
                parsed = self._parse_video(vid_id, channel_id, item)
                save_video(parsed)
                save_view_snapshot(vid_id, parsed["view_count"])

        except Exception as e:
            # Non bloquant — la vidéo principale est déjà sauvegardée
            import logging
            logging.getLogger(__name__).warning(
                f"[{channel_id}] _fetch_recent_videos échoué : {e}"
            )

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