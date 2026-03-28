"""
src/hubspot_client.py — Intégration HubSpot CRM

Synchronise les artistes qualifiés vers HubSpot comme contacts.
Crée les propriétés custom si elles n'existent pas.
Assigne le bon segment pour déclencher les workflows automatisés.

Prérequis HubSpot :
    1. Créer une Private App dans HubSpot
    2. Scopes requis :
       - crm.objects.contacts.write
       - crm.objects.contacts.read
    3. Copier le token dans .env : HUBSPOT_API_KEY=pat-xxx
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

# Propriétés custom HubSpot — à créer manuellement dans HubSpot
# (Settings → Properties → Create property)
# ou via l'API Properties au premier lancement
HUBSPOT_CUSTOM_PROPERTIES = {
    "artist_name"         : "Nom d'artiste",
    "youtube_channel_id"  : "ID Chaîne YouTube",
    "youtube_channel_url" : "URL Chaîne YouTube",
    "prospect_score"      : "Score Trace4Artist (0-100)",
    "prospect_segment"    : "Segment (high_potential/standard/emerging)",
    "source_platform"     : "Plateforme source",
    "country_detected"    : "Pays détecté",
    "video_views"         : "Vues meilleure vidéo",
    "channel_subscribers" : "Abonnés chaîne",
    "detection_date"      : "Date première détection",
    "spotify_url"         : "Lien Spotify",
    "apple_music_url"     : "Lien Apple Music",
    "label"               : "Label / Management détecté",
}


@dataclass
class SyncResult:
    channel_id  : str
    artist_name : str
    action : str = "skipped"    # 'created' | 'updated' | 'skipped' | 'error'
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
        """
        Synchronise tous les artistes qualifiés non encore dans HubSpot.

        Returns:
            Nombre de contacts créés ou mis à jour.
        """
        artists = self._get_artists_to_sync()

        if not artists:
            logger.info("Aucun artiste à synchroniser avec HubSpot")
            return 0

        logger.info(f"Synchronisation HubSpot : {len(artists)} artiste(s)...")

        results  = [self._sync_artist(a) for a in artists]
        created  = sum(1 for r in results if r.action == "created")
        updated  = sum(1 for r in results if r.action == "updated")
        errors   = sum(1 for r in results if r.action == "error")

        logger.info(
            f"HubSpot sync terminé : "
            f"{created} créés, {updated} mis à jour, {errors} erreurs"
        )
        return created + updated

    def _sync_artist(self, artist: dict) -> SyncResult:
        """
        Crée ou met à jour un contact HubSpot pour un artiste.

        Logique :
            - Si hubspot_contact_id existe en base → UPDATE
            - Sinon → recherche par email → CREATE ou UPDATE
        """
        result = SyncResult(
            channel_id  = artist["channel_id"],
            artist_name = artist.get("artist_name", ""),
        )

        properties = self._build_properties(artist)

        try:
            existing_id = artist.get("hubspot_contact_id")

            if existing_id:
                # Mise à jour d'un contact existant
                self._client.crm.contacts.basic_api.update(
                    contact_id    = existing_id,
                    simple_public_object_input = {"properties": properties},
                )
                result.action     = "updated"
                result.hubspot_id = existing_id

            else:
                # Vérifier si le contact existe déjà par email
                email = artist.get("email")
                if email:
                    existing = self._find_by_email(email)
                    if existing:
                        self._client.crm.contacts.basic_api.update(
                            contact_id    = existing,
                            simple_public_object_input = {"properties": properties},
                        )
                        result.action     = "updated"
                        result.hubspot_id = existing
                        self._save_hubspot_id(artist["channel_id"], existing)
                        return result

                # Création d'un nouveau contact
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
                f"HubSpot erreur pour {artist.get('artist_name')} : {e}"
            )

        return result

    def _build_properties(self, artist: dict) -> dict:
        """
        Construit le dictionnaire de propriétés HubSpot
        depuis les données de l'artiste.
        """
        enrichment = artist.get("enrichment_data") or {}

        props = {
            # Propriétés standard HubSpot
            "firstname"    : artist.get("artist_name", ""),
            "email"        : artist.get("email", ""),

            # Propriétés custom Trace4Artist
            "artist_name"        : artist.get("artist_name", ""),
            "youtube_channel_id" : artist.get("channel_id", ""),
            "youtube_channel_url": (
                f"https://youtube.com/channel/{artist.get('channel_id', '')}"
            ),
            "prospect_score"     : str(artist.get("score", 0)),
            "prospect_segment"   : artist.get("segment", ""),
            "source_platform"    : "YouTube",
            "country_detected"   : artist.get("country", ""),
            "channel_subscribers": str(artist.get("subscriber_count", 0)),
        }

        # Données enrichies par Google Search
        if enrichment.get("spotify_url"):
            props["spotify_url"] = enrichment["spotify_url"]
        if enrichment.get("apple_music_url"):
            props["apple_music_url"] = enrichment["apple_music_url"]
        if enrichment.get("label"):
            props["label"] = enrichment["label"]

        # Supprimer les valeurs vides pour ne pas écraser
        # des données existantes dans HubSpot
        return {k: v for k, v in props.items() if v}

    def _find_by_email(self, email: str) -> str | None:
        """Cherche un contact HubSpot par email. Retourne l'ID ou None."""
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
        """Sauvegarde l'ID HubSpot en base pour les futures mises à jour."""
        with get_db() as conn:
            conn.execute(text("""
                UPDATE artists SET
                    hubspot_contact_id = :hubspot_id,
                    updated_at         = NOW()
                WHERE channel_id = :channel_id
            """), {"channel_id": channel_id, "hubspot_id": hubspot_id})

    def _get_artists_to_sync(self) -> list[dict]:
        """
        Récupère les artistes qualifiés à synchroniser
        avec leur dernier score et segment.
        """
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
                    s.segment
                FROM artists a
                LEFT JOIN LATERAL (
                    SELECT score, segment FROM scores
                    WHERE channel_id = a.channel_id
                    ORDER BY calculated_at DESC
                    LIMIT 1
                ) s ON true
                WHERE a.status = 'qualified'
                ORDER BY s.score DESC NULLS LAST
            """))
            return [dict(row._mapping) for row in result.fetchall()]
