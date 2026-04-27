"""
src/searcher.py — Module de recherche d'artistes africains v2

Différences vs v1 :
    - Sauvegarde dans PostgreSQL (via database.py SQLAlchemy)
    - Snapshot des vues à chaque passage (pour la vélocité)
    - Utilisé par _scan_region() dans scheduler.py

Extraction de contacts :
    - Description de la chaîne YouTube (regex)
    - Email obfusqué (contact[at]domain[dot]com)
    - Email professionnel (booking@, contact@) prioritaire
    Note : YouTube API ne retourne pas les liens sociaux (profileLinks)
    via clé API — uniquement via OAuth du propriétaire de la chaîne.
    L'enrichissement complet est géré par GoogleSearchEnricher (enricher.py).
"""

import re
import logging
from src.database import save_artist, save_video, save_view_snapshot
from src.youtube_client import YouTubeClient, QuotaExceededError
from config import SEARCH_KEYWORDS, MAX_RESULTS_PER_SEARCH

logger = logging.getLogger(__name__)

# ── Patterns de contact ────────────────────────────────────────────────
EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
)
EMAIL_OBFUSCATED = re.compile(
    r"([a-zA-Z0-9_.+-]+)\s*[\[\(]at[\]\)]\s*([a-zA-Z0-9-]+)\s*[\[\(]dot[\]\)]\s*([a-zA-Z0-9-.]+)",
    re.IGNORECASE,
)
INSTAGRAM_PATTERN = re.compile(
    r"(?:instagram\.com/|(?<!\w)@)([\w.]{2,30})",
    re.IGNORECASE,
)
WEBSITE_PATTERN = re.compile(
    r"https?://(?!(?:www\.)?(?:youtube|instagram|facebook|twitter|tiktok|"
    r"linktr\.ee|open\.spotify|music\.apple|soundcloud|boomplay|audiomack|"
    r"deezer|spotify))[\w\-.]+"
    r"(?:\.com|\.net|\.org|\.io|\.co|\.cm|\.ng|\.ci|\.sn|\.gh|\.ke|\.za)"
    r"(?:/[\w\-._~:/?#\[\]@!$&\'()*+,;=%]*)?",
    re.IGNORECASE,
)
LINKTREE_PATTERN = re.compile(
    r"https?://(?:www\.)?linktr\.ee/[\w.-]+",
    re.IGNORECASE,
)
BOOKING_PREFIXES = re.compile(
    r"(?:booking|contact|management|manager|press|promo|info|"
    r"label|artiste|artist|officiel|official|music)[\s:@]",
    re.IGNORECASE,
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
        snippet     = channel.get("snippet", {})
        stats       = channel.get("statistics", {})
        description = snippet.get("description", "") or ""

        contacts = self._extract_contacts(description)

        return {
            "channel_id"      : channel_id,
            "artist_name"     : snippet.get("title", ""),
            "country"         : region,
            "description"     : description[:500],
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "total_views"     : int(stats.get("viewCount", 0)),
            "video_count"     : int(stats.get("videoCount", 0)),
            "email"           : contacts["email"],
            "website"         : contacts["website"],
            "instagram"       : contacts["instagram"],
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

    @classmethod
    def _extract_contacts(cls, text: str) -> dict:
        """
        Extrait email, site web et Instagram depuis la description YouTube.

        Note : YouTube API ne retourne pas les liens sociaux (profileLinks)
        via clé API publique. L'enrichissement complet des contacts passe
        par GoogleSearchEnricher (POST /admin/enrich-artists).

        Stratégie :
        1. Email professionnel (booking@, contact@) en priorité
        2. Email obfusqué (contact[at]domain[dot]com)
        3. Email personnel en fallback
        4. Site officiel (hors plateformes sociales)
        5. Linktree comme site de contact
        6. Instagram depuis la description
        """
        result = {"email": None, "website": None, "instagram": None}

        # ── EMAIL ─────────────────────────────────────────────────────────
        all_emails = EMAIL_PATTERN.findall(text)
        valid_emails = [
            e for e in all_emails
            if "youtube.com" not in e
            and "youtu.be"   not in e
            and "example.com" not in e
            and len(e) > 6
        ]
        if valid_emails:
            pro = [
                e for e in valid_emails
                if e.split("@")[0].lower() in (
                    "booking", "contact", "management", "manager",
                    "press", "promo", "info", "label", "official"
                )
            ]
            result["email"] = pro[0] if pro else valid_emails[0]

        # Fallback : email obfusqué (contact[at]domain[dot]com)
        if not result["email"]:
            m = EMAIL_OBFUSCATED.search(text)
            if m:
                result["email"] = f"{m.group(1)}@{m.group(2)}.{m.group(3)}"

        # ── WEBSITE ───────────────────────────────────────────────────────
        m = WEBSITE_PATTERN.search(text)
        if m:
            result["website"] = m.group(0)
        elif not result["website"]:
            m = LINKTREE_PATTERN.search(text)
            if m:
                result["website"] = m.group(0)

        # ── INSTAGRAM ─────────────────────────────────────────────────────
        m = INSTAGRAM_PATTERN.search(text)
        if m:
            handle = m.group(1)
            if handle.lower() not in ("com", "fr", "en", "music", "official"):
                result["instagram"] = handle

        return result

    # ── Méthodes statiques conservées pour compatibilité ──────────────────
    @staticmethod
    def _extract_email(text: str) -> str | None:
        m = EMAIL_PATTERN.search(text)
        return m.group(0) if m else None

    @staticmethod
    def _extract_instagram(text: str) -> str | None:
        m = INSTAGRAM_PATTERN.search(text)
        return m.group(1) if m else None

    @staticmethod
    def _extract_website(text: str) -> str | None:
        m = WEBSITE_PATTERN.search(text)
        return m.group(0) if m else None