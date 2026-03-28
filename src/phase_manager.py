"""
src/phase_manager.py — Gestionnaire des phases de tracking

Responsabilités :
    1. Calculer la phase courante de chaque vidéo selon son âge
    2. Détecter et enregistrer les changements de phase (Option B)
    3. Retourner les vidéos à monitorer pour chaque phase

Phases :
    intensive  → vidéo < intensive_max_days (défaut 7j)
    growth     → vidéo < growth_max_days    (défaut 90j)
    passive    → vidéo < passive_max_days   (défaut 180j)
    stopped    → vidéo > passive_max_days
                 (sauf artiste qualifié → reste en passive)
"""

import logging
from datetime import datetime, timezone, timedelta

from sqlalchemy import text

from src.database import get_db, update_tracking_phase
from src.settings_manager import SettingsManager

logger = logging.getLogger(__name__)


class PhaseManager:
    """
    Calcule et met à jour les phases de tracking des vidéos.

    Utilisation :
        pm = PhaseManager()
        pm.update_all_phases()          # Recalcule toutes les phases
        videos = pm.get_phase_videos("intensive")  # Vidéos à monitorer
    """

    def __init__(self):
        self.settings = SettingsManager()

    def update_all_phases(self) -> dict:
        """
        Recalcule la phase de toutes les vidéos actives
        et enregistre les transitions (Option B).

        Returns:
            Statistiques des transitions effectuées.
        """
        intensive_days = self.settings.get_lookback_days()  # réutilise le setting
        intensive_max  = int(self.settings.get("tracking.intensive_max_days"))
        growth_max     = int(self.settings.get("tracking.growth_max_days"))
        passive_max    = int(self.settings.get("tracking.passive_max_days"))
        keep_qualified = self.settings.get("tracking.keep_qualified") == "true"

        transitions = {
            "to_growth"  : 0,
            "to_passive" : 0,
            "to_stopped" : 0,
        }

        videos = self._get_all_trackable_videos()
        now    = datetime.now(timezone.utc)

        for video in videos:
            pub_at = video.get("published_at")
            if not pub_at:
                continue

            if isinstance(pub_at, str):
                try:
                    pub_at = datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
                except ValueError:
                    continue

            age_days     = (now - pub_at).days
            current_phase= video.get("tracking_phase", "intensive")
            new_phase    = self._compute_phase(
                age_days, intensive_max, growth_max, passive_max,
                keep_qualified, video.get("artist_status", "discovered")
            )

            if new_phase != current_phase:
                update_tracking_phase(
                    video_id  = video["video_id"],
                    new_phase = new_phase,
                    old_phase = current_phase,
                )
                key = f"to_{new_phase}"
                if key in transitions:
                    transitions[key] += 1
                logger.info(
                    f"[{video['video_id']}] Phase : "
                    f"{current_phase} → {new_phase} "
                    f"(âge : {age_days}j)"
                )

        total = sum(transitions.values())
        if total > 0:
            logger.info(
                f"Transitions de phase : "
                f"{transitions['to_growth']} → growth, "
                f"{transitions['to_passive']} → passive, "
                f"{transitions['to_stopped']} → stopped"
            )

        return transitions

    def get_phase_videos(self, phase: str) -> list[dict]:
        """
        Retourne les vidéos d'une phase donnée à monitorer.

        Args:
            phase : 'intensive' | 'growth' | 'passive'

        Returns:
            Liste de dicts avec video_id, channel_id, view_count,
            like_count, comment_count, subscriber_count
        """
        with get_db() as conn:
            result = conn.execute(text("""
                SELECT
                    v.video_id,
                    v.channel_id,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    v.published_at,
                    v.tracking_phase,
                    a.subscriber_count,
                    a.status as artist_status
                FROM videos v
                JOIN artists a ON v.channel_id = a.channel_id
                WHERE v.tracking_phase = :phase
                ORDER BY v.published_at DESC
            """), {"phase": phase})
            return [dict(row._mapping) for row in result.fetchall()]

    # ──────────────────────────────────────────────────────────────────
    # UTILITAIRES PRIVÉS
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _compute_phase(
        age_days      : int,
        intensive_max : int,
        growth_max    : int,
        passive_max   : int,
        keep_qualified: bool,
        artist_status : str,
    ) -> str:
        """
        Calcule la phase d'une vidéo selon son âge.

        Règle spéciale keep_qualified :
            Si l'artiste est qualifié et que la vidéo dépasserait
            'stopped', on la maintient en 'passive' pour continuer
            à tracker son évolution long terme.
        """
        if age_days <= intensive_max:
            return "intensive"

        if age_days <= growth_max:
            return "growth"

        if age_days <= passive_max:
            return "passive"

        # Au-delà de passive_max
        if keep_qualified and artist_status == "qualified":
            return "passive"   # On continue à tracker l'artiste qualifié

        return "stopped"

    def _get_all_trackable_videos(self) -> list[dict]:
        """Récupère toutes les vidéos qui ne sont pas encore 'stopped'."""
        with get_db() as conn:
            result = conn.execute(text("""
                SELECT
                    v.video_id,
                    v.channel_id,
                    v.published_at,
                    v.tracking_phase,
                    a.status as artist_status
                FROM videos v
                JOIN artists a ON v.channel_id = a.channel_id
                WHERE v.tracking_phase != 'stopped'
                ORDER BY v.published_at DESC
            """))
            return [dict(row._mapping) for row in result.fetchall()]
