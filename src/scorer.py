"""
src/scorer.py — Scorer v2 final

Critères de scoring (total 100 pts) :
    1. SPR — Score de Performance Relative     20 pts
    2. Indice de Viralité Organique            20 pts
    3. Vélocité 24h                            20 pts
    4. Vélocité 7j                             15 pts
    5. Régularité des publications             10 pts
    6. Qualité de chaîne                       10 pts
    7. Présence web + contact                   5 pts

Détection de fraude :
    - Fake View Detector : engagement < 0.1% sur > 100K vues → malus -20pts
    - Vidéo suspecte flaggée en base pour revue manuelle

Segmentation :
    >= 80  →  high_potential
    >= 60  →  standard
    >= 40  →  emerging
    <  40  →  low_priority
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

from sqlalchemy import text

from config import MIN_VIEWS, MIN_SUBSCRIBERS, SCORE_SEGMENTS
from src.database import get_db, save_alert

logger = logging.getLogger(__name__)

# ── Constantes de scoring ──────────────────────────────────────────────
FAKE_VIEW_MIN_VIEWS      = 100_000
FAKE_VIEW_MAX_ENGAGEMENT = 0.001
FAKE_VIEW_SCORE_PENALTY  = 20
# BREAKOUT_THRESHOLD supprime — lu dynamiquement depuis SettingsManager
# pour que la valeur en base (0.50) soit effectivement utilisee

def _get_breakout_threshold() -> float:
    """Lit le seuil de breakout depuis la base via SettingsManager.
    Fallback a 0.50 si absent ou illisible.
    """
    try:
        from src.settings_manager import SettingsManager
        return float(SettingsManager().get("tracking.breakout_threshold"))
    except Exception:
        return 0.50


@dataclass
class ScoringResult:
    channel_id    : str
    channel_name  : str
    total_score   : float
    segment       : str
    is_qualified  : bool
    breakdown     : dict = field(default_factory=dict)
    is_suspicious : bool = False
    disqualification_reason: str | None = None


class ArtistScorer:
    """
    Calcule le score de qualification des artistes avec
    détection de viralité organique et de fraude.
    """

    def score_all_discovered(self) -> list[ScoringResult]:
        """Score tous les artistes au statut 'discovered'."""
        artists = self._get_artists_to_score()

        if not artists:
            logger.info("Aucun artiste à scorer")
            return []

        logger.info(f"Scoring de {len(artists)} artiste(s)...")
        results = []

        for artist in artists:
            result = self.score_artist(artist)
            self._persist(result)
            results.append(result)

        results.sort(key=lambda r: r.total_score, reverse=True)

        qualified  = sum(1 for r in results if r.is_qualified)
        suspicious = sum(1 for r in results if r.is_suspicious)
        logger.info(
            f"  Qualifiés  : {qualified}/{len(results)}\n"
            f"  Suspects   : {suspicious}/{len(results)}"
        )
        return results

    def score_artist(self, artist: dict) -> ScoringResult:
        """
        Calcule le score complet d'un artiste.
        Peut être appelé sans base de données (utile pour les tests).

        artist doit contenir :
            channel_id, artist_name, subscriber_count,
            email, instagram, website,
            videos    : [{view_count, like_count, comment_count, published_at}]
            snapshots : [{view_count, like_count, comment_count,
                          subscriber_count, snapped_at}]
        """
        videos    = artist.get("videos", [])
        snapshots = artist.get("snapshots", [])

        # ── Disqualification immédiate ─────────────────────────────────
        if artist.get("subscriber_count", 0) < MIN_SUBSCRIBERS:
            return self._disqualify(artist, f"Moins de {MIN_SUBSCRIBERS} abonnés")

        max_views = max((v.get("view_count", 0) for v in videos), default=0)
        if max_views < MIN_VIEWS:
            return self._disqualify(
                artist, f"Moins de {MIN_VIEWS} vues sur la meilleure vidéo"
            )

        # ── Détection de fraude ────────────────────────────────────────
        is_suspicious, fraud_details = self._detect_fake_views(videos)
        if is_suspicious:
            logger.warning(
                f"[{artist.get('artist_name')}] Vues suspectes : {fraud_details}"
            )

        # ── Métriques intermédiaires ───────────────────────────────────
        spr          = self._compute_spr(videos, artist.get("subscriber_count", 1))
        engagement   = self._compute_engagement(videos)
        velocity_24h = self._compute_velocity_24h(snapshots)
        velocity_7d  = self._compute_velocity_7d(snapshots)
        breakout     = (
            velocity_24h > _get_breakout_threshold()
            and self._is_older_than(videos, 7)
        )

        # ── 7 critères ────────────────────────────────────────────────
        breakdown = {
            "spr"         : self._score_spr(spr),
            "engagement"  : self._score_engagement(engagement),
            "velocity_24h": self._score_velocity_24h(velocity_24h),
            "velocity_7d" : self._score_velocity_7d(velocity_7d),
            "regularity"  : self._score_regularity(videos),
            "channel"     : self._score_channel(artist.get("subscriber_count", 0)),
            "web_contact" : self._score_web_contact(artist),
        }

        total = round(sum(breakdown.values()), 2)

        # ── Malus fraude ───────────────────────────────────────────────
        if is_suspicious:
            total = max(0.0, total - FAKE_VIEW_SCORE_PENALTY)
            breakdown["fraud_penalty"] = -FAKE_VIEW_SCORE_PENALTY

        total   = min(total, 100.0)
        segment = self._get_segment(total)

        if breakout:
            self._flag_breakout(artist, velocity_24h)

        return ScoringResult(
            channel_id   = artist["channel_id"],
            channel_name = artist.get("artist_name", ""),
            total_score  = total,
            segment      = segment,
            is_qualified = total >= SCORE_SEGMENTS["emerging"],
            breakdown    = breakdown,
            is_suspicious= is_suspicious,
        )

    # ──────────────────────────────────────────────────────────────────
    # MÉTRIQUES INTERMÉDIAIRES
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _compute_spr(videos: list, subscriber_count: int) -> float:
        """SPR = max_views / subscriber_count"""
        if not videos or subscriber_count <= 0:
            return 0.0
        max_views = max(v.get("view_count", 0) for v in videos)
        return max_views / subscriber_count

    @staticmethod
    def _compute_engagement(videos: list) -> float:
        """Engagement = (likes + comments) / vues"""
        if not videos:
            return 0.0
        ratios = []
        for v in videos:
            views = v.get("view_count", 0)
            if views > 0:
                interactions = v.get("like_count", 0) + v.get("comment_count", 0)
                ratios.append(interactions / views)
        return sum(ratios) / len(ratios) if ratios else 0.0

    @staticmethod
    def _compute_velocity_24h(snapshots: list) -> float:
        """Vitesse = (vues_T - vues_T-24h) / vues_T-24h"""
        if len(snapshots) < 2:
            return 0.0
        sorted_s  = sorted(snapshots, key=lambda s: s.get("snapped_at", ""))
        old_views = sorted_s[0].get("view_count", 0)
        new_views = sorted_s[-1].get("view_count", 0)
        if old_views <= 0:
            return 0.0
        return (new_views - old_views) / old_views

    @staticmethod
    def _compute_velocity_7d(snapshots: list) -> float:
        """Vélocité sur 7 jours depuis les snapshots."""
        if len(snapshots) < 2:
            return 0.0
        sorted_s   = sorted(snapshots, key=lambda s: s.get("snapped_at", ""))
        first_views = sorted_s[0].get("view_count", 0)
        last_views  = sorted_s[-1].get("view_count", 0)
        if first_views <= 0:
            return 0.0
        return (last_views - first_views) / first_views

    @staticmethod
    def _detect_fake_views(videos: list) -> tuple[bool, dict]:
        """Détecte les vues artificielles par faible engagement."""
        for v in videos:
            views = v.get("view_count", 0)
            if views < FAKE_VIEW_MIN_VIEWS:
                continue
            likes    = v.get("like_count", 0)
            comments = v.get("comment_count", 0)
            engagement = (likes + comments) / views
            if engagement < FAKE_VIEW_MAX_ENGAGEMENT:
                return True, {
                    "video_id"  : v.get("video_id"),
                    "views"     : views,
                    "engagement": round(engagement, 6),
                }
        return False, {}

    @staticmethod
    def _is_older_than(videos: list, days: int) -> bool:
        """Vérifie si la vidéo la plus récente est plus ancienne que N jours."""
        if not videos:
            return False
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        for v in videos:
            pub = v.get("published_at")
            if pub:
                try:
                    if isinstance(pub, str):
                        pub = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    if pub > cutoff:
                        return False
                except ValueError:
                    pass
        return True

    # ──────────────────────────────────────────────────────────────────
    # CRITÈRES DE SCORING
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _score_spr(spr: float) -> float:
        """SPR — max 20 pts"""
        if spr <= 0:        return 0.0
        if spr < 0.10:      return 2.0
        if spr < 0.50:      return 6.0
        if spr < 1.00:      return 10.0
        if spr < 2.00:      return 15.0
        if spr < 5.00:      return 18.0
        return 20.0

    @staticmethod
    def _score_engagement(engagement: float) -> float:
        """Indice de Viralité Organique — max 20 pts"""
        if engagement <= 0:     return 0.0
        if engagement < 0.01:   return 3.0
        if engagement < 0.02:   return 7.0
        if engagement < 0.04:   return 11.0
        if engagement < 0.07:   return 15.0
        if engagement < 0.10:   return 18.0
        return 20.0

    @staticmethod
    def _score_velocity_24h(velocity: float) -> float:
        """Accélération 24h — max 20 pts"""
        if velocity <= 0:       return 0.0
        if velocity < 0.05:     return 4.0
        if velocity < 0.10:     return 8.0
        if velocity < 0.20:     return 12.0
        if velocity < 0.50:     return 16.0
        return 20.0

    @staticmethod
    def _score_velocity_7d(velocity: float) -> float:
        """Vélocité 7 jours — max 15 pts"""
        if velocity <= 0:       return 0.0
        if velocity < 0.10:     return 3.0
        if velocity < 0.25:     return 6.0
        if velocity < 0.50:     return 9.0
        if velocity < 1.00:     return 12.0
        return 15.0

    @staticmethod
    def _score_regularity(videos: list) -> float:
        """Régularité des publications — max 10 pts"""
        if len(videos) < 2:
            return 0.0
        dated = []
        for v in videos:
            pub = v.get("published_at")
            if pub:
                try:
                    if isinstance(pub, str):
                        pub = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    dated.append(pub)
                except ValueError:
                    continue
        if len(dated) < 2:
            return 0.0
        dated.sort()
        intervals = [(dated[i+1] - dated[i]).days for i in range(len(dated)-1)]
        avg_days  = sum(intervals) / len(intervals)

        if avg_days <= 14:  return 10.0
        if avg_days <= 30:  return 8.0
        if avg_days <= 60:  return 5.0
        if avg_days <= 90:  return 2.0
        return 0.0

    @staticmethod
    def _score_channel(subscriber_count: int) -> float:
        """Qualité de chaîne — max 10 pts"""
        if subscriber_count < 1_000:    return 0.0
        if subscriber_count < 5_000:    return 3.0
        if subscriber_count < 20_000:   return 6.0
        if subscriber_count < 100_000:  return 8.0
        return 10.0

    @staticmethod
    def _score_web_contact(artist: dict) -> float:
        """Présence web + contact — max 5 pts"""
        score = 0.0
        email = artist.get("email", "")
        if email and "@" in email:      score += 2.5
        if artist.get("instagram"):     score += 1.5
        if artist.get("website"):       score += 1.0
        return score

    # ──────────────────────────────────────────────────────────────────
    # UTILITAIRES
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _get_segment(score: float) -> str:
        if score >= SCORE_SEGMENTS["high_potential"]: return "high_potential"
        if score >= SCORE_SEGMENTS["standard"]:       return "standard"
        if score >= SCORE_SEGMENTS["emerging"]:       return "emerging"
        return "low_priority"

    @staticmethod
    def _disqualify(artist: dict, reason: str) -> ScoringResult:
        return ScoringResult(
            channel_id              = artist["channel_id"],
            channel_name            = artist.get("artist_name", ""),
            total_score             = 0.0,
            segment                 = "low_priority",
            is_qualified            = False,
            breakdown               = {},
            disqualification_reason = reason,
        )

    def _flag_breakout(self, artist: dict, velocity_24h: float):
        """Enregistre une alerte breakout en base."""
        try:
            save_alert(
                video_id   = artist.get("best_video_id", "unknown"),
                channel_id = artist["channel_id"],
                alert_type = "breakout",
                details    = {
                    "artist_name" : artist.get("artist_name"),
                    "velocity_24h": round(velocity_24h, 4),
                    "threshold"   : _get_breakout_threshold(),
                },
            )
            logger.info(
                f"Breakout : {artist.get('artist_name')} "
                f"(+{velocity_24h*100:.1f}% en 24h)"
            )
        except Exception as e:
            logger.warning(f"Impossible d'enregistrer l'alerte breakout : {e}")

    def _get_artists_to_score(self) -> list[dict]:
        with get_db() as conn:
            result = conn.execute(text("""
                SELECT channel_id, artist_name, subscriber_count,
                       email, instagram, website
                FROM artists WHERE status = 'discovered'
            """))
            artists = [dict(row._mapping) for row in result.fetchall()]

        for artist in artists:
            artist["videos"]    = self._get_videos(artist["channel_id"])
            artist["snapshots"] = self._get_snapshots(artist["channel_id"])
            if artist["videos"]:
                best = max(artist["videos"], key=lambda v: v.get("view_count", 0))
                artist["best_video_id"] = best.get("video_id", "unknown")

        return artists

    def _get_videos(self, channel_id: str) -> list[dict]:
        with get_db() as conn:
            result = conn.execute(text("""
                SELECT video_id, view_count, like_count,
                       comment_count, published_at
                FROM videos
                WHERE channel_id = :channel_id
                ORDER BY published_at DESC LIMIT 20
            """), {"channel_id": channel_id})
            return [dict(row._mapping) for row in result.fetchall()]

    def _get_snapshots(self, channel_id: str) -> list[dict]:
        # Calcul de la date de coupure en Python — évite le bug de
        # SQLAlchemy avec INTERVAL ':param days' qui n'est pas interpolé
        from datetime import datetime, timezone, timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        with get_db() as conn:
            result = conn.execute(text("""
                SELECT vs.view_count, vs.like_count,
                       vs.comment_count, vs.subscriber_count,
                       vs.snapped_at
                FROM view_snapshots vs
                JOIN videos v ON vs.video_id = v.video_id
                WHERE v.channel_id = :channel_id
                  AND vs.snapped_at >= :cutoff
                ORDER BY vs.snapped_at ASC
            """), {"channel_id": channel_id, "cutoff": cutoff})
            return [dict(row._mapping) for row in result.fetchall()]

    def _persist(self, result: ScoringResult):
        new_status = "qualified" if result.is_qualified else "rejected"
        with get_db() as conn:
            conn.execute(text("""
                UPDATE artists SET status = :status, updated_at = NOW()
                WHERE channel_id = :channel_id
            """), {"status": new_status, "channel_id": result.channel_id})

            conn.execute(text("""
                INSERT INTO scores (channel_id, score, segment, criteria_breakdown)
                VALUES (:channel_id, :score, :segment, :breakdown)
            """), {
                "channel_id": result.channel_id,
                "score"     : result.total_score,
                "segment"   : result.segment,
                "breakdown" : json.dumps(result.breakdown),
            })

            if result.is_suspicious:
                conn.execute(text("""
                    UPDATE videos SET is_suspicious = TRUE
                    WHERE channel_id = :channel_id
                """), {"channel_id": result.channel_id})