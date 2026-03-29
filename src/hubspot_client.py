"""
src/hubspot_client.py — Intégration HubSpot CRM

Utilise les propriétés natives HubSpot + exactement 10 custom properties
(limite du compte gratuit).

Propriétés natives utilisées :
    firstname   → nom d'artiste
    email       → email de contact
    company     → label / management
    website     → site officiel
    country     → pays détecté
    phone       → WhatsApp si disponible
    createdate  → auto-géré par HubSpot (= detection_date)

10 propriétés custom Trace4Artist :
    1.  youtube_channel_id
    2.  youtube_channel_url
    3.  prospect_score
    4.  prospect_segment
    5.  contact_type        ← artist | manager | label
    6.  source_platform
    7.  video_views
    8.  channel_subscribers
    9.  latest_video_url
    10. spr_score
"""

import logging
from dataclasses import dataclass

from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInputForCreate
from hubspot.crm.contacts.exceptions import ApiException

from config import HUBSPOT_API_KEY
from src.database import get_db
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Les 10 propriétés custom à créer dans HubSpot
# Settings → Properties → Contact properties → Create property
HUBSPOT_CUSTOM_PROPERTIES = [
    "youtube_channel_id",
    "youtube_channel_url",
    "prospect_score",
    "prospect_segment",
    "contact_type",
    "source_platform",
    "video_views",
    "channel_subscribers",
    "latest_video_url",
    "spr_score",
]


@dataclass
class SyncResult:
    channel_id  : str
    artist_name : str
    action      : str = "skipped"   # 'created' | 'updated' | 'skipped' | 'error'
    hubspot_id  : str | None = None
    error       : str | None = None


class HubSpotClient:
    """
    Synchronise les artistes qualifiés vers HubSpot CRM.

    Utilisation :
        client = HubSpotClient()
        synced = client.sync_qualified_artists()
    """

    def __init__(self):
        if not HUBSPOT_API_KEY:
            raise ValueError(
                "HUBSPOT_API_KEY manquante dans .env\n"
                "Crée une Private App dans HubSpot et copie le token."
            )
        self._client = HubSpot(access_token=HUBSPOT_API_KEY)

    def sync_qualified_artists(self) -> int:
        """Synchronise tous les artistes qualifiés vers HubSpot."""
        artists = self._get_artists_to_sync()

        if not artists:
            logger.info("Aucun artiste à synchroniser avec HubSpot")
            return 0

        logger.info(f"Synchronisation HubSpot : {len(artists)} artiste(s)...")
        results = [self._sync_artist(a) for a in artists]

        created = sum(1 for r in results if r.action == "created")
        updated = sum(1 for r in results if r.action == "updated")
        errors  = sum(1 for r in results if r.action == "error")

        logger.info(
            f"HubSpot sync : {created} créés, "
            f"{updated} mis à jour, {errors} erreurs"
        )
        return created + updated

    def _sync_artist(self, artist: dict) -> SyncResult:
        """Crée ou met à jour un contact HubSpot."""
        result = SyncResult(
            channel_id  = artist["channel_id"],
            artist_name = artist.get("artist_name", ""),
        )
        properties = self._build_properties(artist)

        try:
            existing_id = artist.get("hubspot_contact_id")

            if existing_id:
                self._client.crm.contacts.basic_api.update(
                    contact_id=existing_id,
                    simple_public_object_input={"properties": properties},
                )
                result.action     = "updated"
                result.hubspot_id = existing_id

            else:
                email = artist.get("email")
                if email:
                    found_id = self._find_by_email(email)
                    if found_id:
                        self._client.crm.contacts.basic_api.update(
                            contact_id=found_id,
                            simple_public_object_input={"properties": properties},
                        )
                        result.action     = "updated"
                        result.hubspot_id = found_id
                        self._save_hubspot_id(artist["channel_id"], found_id)
                        return result

                contact = self._client.crm.contacts.basic_api.create(
                    simple_public_object_input_for_create=
                    SimplePublicObjectInputForCreate(properties=properties)
                )
                result.action     = "created"
                result.hubspot_id = contact.id
                self._save_hubspot_id(artist["channel_id"], contact.id)

        except ApiException as e:
            result.action = "error"
            result.error  = str(e)
            logger.error(
                f"HubSpot erreur [{artist.get('artist_name')}] : {e}"
            )

        return result

    def _build_properties(self, artist: dict) -> dict:
        """
        Construit les propriétés HubSpot depuis les données artiste.

        Priorité native > custom pour rester dans la limite des 10.
        """
        enrichment = artist.get("enrichment_data") or {}

        props = {
            # ── Propriétés NATIVES HubSpot ─────────────────────────────
            "firstname"  : artist.get("artist_name", ""),
            "email"      : artist.get("email", ""),
            "company"    : enrichment.get("label", ""),  # Label / management
            "website"    : artist.get("website", ""),
            "country"    : artist.get("country", ""),

            # ── 10 propriétés CUSTOM Trace4Artist ──────────────────────
            "youtube_channel_id"  : artist.get("channel_id", ""),
            "youtube_channel_url" : (
                f"https://youtube.com/channel/{artist.get('channel_id', '')}"
            ),
            "prospect_score"      : str(round(artist.get("score", 0), 1)),
            "prospect_segment"    : artist.get("segment", ""),
            "contact_type"        : enrichment.get("contact_type", "artist"),
            "source_platform"     : "YouTube",
            "video_views"         : str(artist.get("video_views", 0)),
            "channel_subscribers" : str(artist.get("subscriber_count", 0)),
            "latest_video_url"    : artist.get("latest_video_url", ""),
            "spr_score"           : str(round(artist.get("spr_score", 0), 2)),
        }

        # Supprimer les valeurs vides pour ne pas écraser HubSpot
        return {k: v for k, v in props.items() if v and v != "0" and v != "0.0"}

    def _find_by_email(self, email: str) -> str | None:
        """Recherche un contact HubSpot par email."""
        try:
            result = self._client.crm.contacts.search_api.do_search(
                public_object_search_request={
                    "filterGroups": [{
                        "filters": [{
                            "propertyName": "email",
                            "operator"    : "EQ",
                            "value"       : email,
                        }]
                    }],
                    "limit": 1,
                }
            )
            if result.results:
                return result.results[0].id
        except ApiException:
            pass
        return None

    def _save_hubspot_id(self, channel_id: str, hubspot_id: str):
        with get_db() as conn:
            conn.execute(text("""
                UPDATE artists SET
                    hubspot_contact_id = :hubspot_id,
                    updated_at         = NOW()
                WHERE channel_id = :channel_id
            """), {"channel_id": channel_id, "hubspot_id": hubspot_id})

    def _get_artists_to_sync(self) -> list[dict]:
        """Récupère les artistes qualifiés avec leurs métriques."""
        with get_db() as conn:
            result = conn.execute(text("""
                SELECT
                    a.channel_id,
                    a.artist_name,
                    a.email,
                    a.website,
                    a.country,
                    a.subscriber_count,
                    a.hubspot_contact_id,
                    a.enrichment_data,
                    s.score,
                    s.segment,
                    MAX(v.view_count)  as video_views,
                    MAX(v.video_id)    as latest_video_id
                FROM artists a
                LEFT JOIN LATERAL (
                    SELECT score, segment FROM scores
                    WHERE channel_id = a.channel_id
                    ORDER BY calculated_at DESC LIMIT 1
                ) s ON true
                LEFT JOIN videos v ON a.channel_id = v.channel_id
                WHERE a.status = 'qualified'
                GROUP BY
                    a.channel_id, a.artist_name, a.email, a.website,
                    a.country, a.subscriber_count, a.hubspot_contact_id,
                    a.enrichment_data, s.score, s.segment
                ORDER BY s.score DESC NULLS LAST
            """))
            artists = [dict(row._mapping) for row in result.fetchall()]

        # Construire l'URL de la dernière vidéo
        for a in artists:
            if a.get("latest_video_id"):
                a["latest_video_url"] = (
                    f"https://youtube.com/watch?v={a['latest_video_id']}"
                )

        return artists