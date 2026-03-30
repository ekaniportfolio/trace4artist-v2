"""
src/database.py — Connexion PostgreSQL via SQLAlchemy

On utilise SQLAlchemy Core (pas l'ORM) — on garde le contrôle
du SQL tout en bénéficiant du pool de connexions et de la
compatibilité multi-bases (utile pour les tests avec SQLite).
"""

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from contextlib import contextmanager

from config import DATABASE_URL


# ── Engine principal ───────────────────────────────────────────────────
# pool_pre_ping=True : vérifie que la connexion est vivante avant usage
# (indispensable pour les connexions longue durée avec un scheduler)
# Déterminer si on utilise Supabase pooler (port 6543)
# Le pooler Supabase nécessite SSL et ne supporte pas les transactions
_is_pooler = ":6543" in (DATABASE_URL or "")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args={
        "sslmode": "require",
    } if _is_pooler else {},
)


@contextmanager
def get_db():
    """
    Context manager pour les transactions.

    Utilisation :
        with get_db() as conn:
            conn.execute(text("SELECT 1"))

    Commit automatique si pas d'exception.
    Rollback automatique si exception — la BDD reste cohérente.
    """
    with engine.connect() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def check_connection() -> bool:
    """
    Vérifie que la base de données est accessible.
    Utilisé au démarrage pour un feedback clair.
    """
    try:
        with get_db() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"❌ Connexion PostgreSQL impossible : {e}")
        return False


def get_quota_used_today() -> int:
    """Quota YouTube consommé aujourd'hui."""
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT COALESCE(SUM(units_used), 0)
            FROM quota_log
            WHERE date = CURRENT_DATE
        """))
        return result.scalar() or 0


def log_quota_usage(endpoint: str, units: int):
    """Enregistre une consommation de quota."""
    with get_db() as conn:
        conn.execute(text("""
            INSERT INTO quota_log (endpoint, units_used)
            VALUES (:endpoint, :units)
        """), {"endpoint": endpoint, "units": units})


def save_artist(artist_data: dict) -> bool:
    """
    Insère ou met à jour un artiste.
    Retourne True si c'est un nouvel artiste.
    """
    with get_db() as conn:
        result = conn.execute(text("""
            INSERT INTO artists (
                channel_id, artist_name, country, description,
                subscriber_count, total_views, video_count,
                email, website, instagram
            )
            VALUES (
                :channel_id, :artist_name, :country, :description,
                :subscriber_count, :total_views, :video_count,
                :email, :website, :instagram
            )
            ON CONFLICT (channel_id) DO UPDATE SET
                artist_name      = EXCLUDED.artist_name,
                subscriber_count = EXCLUDED.subscriber_count,
                total_views      = EXCLUDED.total_views,
                video_count      = EXCLUDED.video_count,
                email            = COALESCE(EXCLUDED.email, artists.email),
                website          = COALESCE(EXCLUDED.website, artists.website),
                instagram        = COALESCE(EXCLUDED.instagram, artists.instagram),
                updated_at       = NOW()
            RETURNING (xmax = 0) AS is_new
        """), artist_data)
        row = result.fetchone()
        return bool(row[0]) if row else False


def save_video(video_data: dict):
    """Insère une vidéo (ignore si elle existe déjà)."""
    with get_db() as conn:
        conn.execute(text("""
            INSERT INTO videos (
                video_id, channel_id, title,
                view_count, like_count, comment_count,
                published_at, duration
            )
            VALUES (
                :video_id, :channel_id, :title,
                :view_count, :like_count, :comment_count,
                :published_at, :duration
            )
            ON CONFLICT (video_id) DO UPDATE SET
                view_count    = EXCLUDED.view_count,
                like_count    = EXCLUDED.like_count,
                comment_count = EXCLUDED.comment_count
        """), video_data)


def save_view_snapshot(video_id: str, view_count: int):
    """
    Enregistre un snapshot des vues pour le calcul de vélocité.
    Appelé à chaque scan pour chaque vidéo.
    """
    with get_db() as conn:
        conn.execute(text("""
            INSERT INTO view_snapshots (video_id, view_count)
            VALUES (:video_id, :view_count)
        """), {"video_id": video_id, "view_count": view_count})


def get_view_velocity(video_id: str, window_days: int = 7) -> float:
    """
    Calcule la vélocité de croissance des vues sur N jours.
    Vélocité = (vues actuelles - vues il y a N jours) / vues il y a N jours

    Retourne 0.0 si pas assez de données historiques.
    """
    with get_db() as conn:
        result = conn.execute(text("""
            WITH snapshots AS (
                SELECT view_count, snapped_at,
                       ROW_NUMBER() OVER (ORDER BY snapped_at DESC) as rn_recent,
                       ROW_NUMBER() OVER (ORDER BY snapped_at ASC)  as rn_old
                FROM view_snapshots
                WHERE video_id  = :video_id
                  AND snapped_at >= NOW() - INTERVAL ':days days'
            )
            SELECT
                MAX(CASE WHEN rn_recent = 1 THEN view_count END) as current_views,
                MIN(CASE WHEN rn_old    = 1 THEN view_count END) as old_views
            FROM snapshots
        """), {"video_id": video_id, "days": window_days})

        row = result.fetchone()
        if not row or not row[0] or not row[1] or row[1] == 0:
            return 0.0

        return (row[0] - row[1]) / row[1]


def get_all_artists(status: str = None) -> list:
    """Récupère les artistes avec filtre optionnel par statut."""
    with get_db() as conn:
        if status:
            result = conn.execute(text("""
                SELECT * FROM artists
                WHERE status = :status
                ORDER BY updated_at DESC
            """), {"status": status})
        else:
            result = conn.execute(text("""
                SELECT * FROM artists ORDER BY updated_at DESC
            """))
        return [dict(row._mapping) for row in result.fetchall()]


def get_last_scan_date() -> str | None:
    """
    Retourne la date de fin du dernier scan réussi.
    Utilisé par le scheduler pour la recherche incrémentale :
    on ne cherche que les vidéos publiées après ce point.
    """
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT completed_at FROM scan_logs
            WHERE status = 'completed'
            ORDER BY completed_at DESC
            LIMIT 1
        """))
        row = result.fetchone()
        if row and row[0]:
            return row[0].isoformat()
        return None


# ──────────────────────────────────────────────────────────────────────
# FONCTIONS AJOUTÉES — Migration 003
# ──────────────────────────────────────────────────────────────────────

def save_view_snapshot_enriched(
    video_id        : str,
    view_count      : int,
    like_count      : int = 0,
    comment_count   : int = 0,
    subscriber_count: int = 0,
):
    """
    Version enrichie de save_view_snapshot.
    Stocke les métriques complètes au moment du snapshot
    pour permettre un scoring historique précis.

    Remplace save_view_snapshot() à partir de la Migration 003.
    """
    with get_db() as conn:
        conn.execute(text("""
            INSERT INTO view_snapshots
                (video_id, view_count, like_count,
                 comment_count, subscriber_count)
            VALUES
                (:video_id, :view_count, :like_count,
                 :comment_count, :subscriber_count)
        """), {
            "video_id"        : video_id,
            "view_count"      : view_count,
            "like_count"      : like_count,
            "comment_count"   : comment_count,
            "subscriber_count": subscriber_count,
        })


def get_snapshots_for_velocity(
    video_id    : str,
    window_hours: int = 24,
    limit       : int = 10,
) -> list[dict]:
    """
    Récupère les derniers snapshots d'une vidéo pour
    le calcul de vélocité sur une fenêtre de temps donnée.

    Returns:
        Liste de snapshots ordonnés du plus ancien au plus récent.
    """
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT
                view_count, like_count, comment_count,
                subscriber_count, snapped_at
            FROM view_snapshots
            WHERE video_id  = :video_id
              AND snapped_at >= NOW() - INTERVAL ':hours hours'
            ORDER BY snapped_at ASC
            LIMIT :limit
        """), {"video_id": video_id, "hours": window_hours, "limit": limit})
        return [dict(row._mapping) for row in result.fetchall()]


def get_videos_by_tracking_phase(phase: str) -> list[dict]:
    """
    Récupère toutes les vidéos d'une phase de tracking donnée.
    Utilisé par le scheduler tiered pour savoir quoi monitorer.
    """
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT
                v.video_id, v.channel_id, v.view_count,
                v.like_count, v.comment_count,
                v.published_at, v.tracking_phase,
                a.subscriber_count
            FROM videos v
            JOIN artists a ON v.channel_id = a.channel_id
            WHERE v.tracking_phase = :phase
            ORDER BY v.published_at DESC
        """), {"phase": phase})
        return [dict(row._mapping) for row in result.fetchall()]


def update_tracking_phase(
    video_id  : str,
    new_phase : str,
    old_phase : str,
):
    """
    Met à jour la phase de tracking d'une vidéo et
    enregistre une alerte 'phase_change' (Option B).
    """
    import json
    with get_db() as conn:
        conn.execute(text("""
            UPDATE videos SET
                tracking_phase  = :new_phase,
                phase_changed_at = NOW()
            WHERE video_id = :video_id
        """), {"video_id": video_id, "new_phase": new_phase})

        # Log du changement de phase (Option B)
        conn.execute(text("""
            INSERT INTO video_alerts
                (video_id, channel_id, alert_type, details)
            SELECT
                :video_id,
                channel_id,
                'phase_change',
                :details
            FROM videos WHERE video_id = :video_id
        """), {
            "video_id": video_id,
            "details" : json.dumps({
                "from": old_phase,
                "to"  : new_phase,
            }),
        })


def save_alert(
    video_id  : str,
    channel_id: str,
    alert_type: str,
    details   : dict,
):
    """Enregistre une alerte (breakout, fake_views, etc.)."""
    import json
    with get_db() as conn:
        conn.execute(text("""
            INSERT INTO video_alerts
                (video_id, channel_id, alert_type, details)
            VALUES
                (:video_id, :channel_id, :alert_type, :details)
        """), {
            "video_id"  : video_id,
            "channel_id": channel_id,
            "alert_type": alert_type,
            "details"   : json.dumps(details),
        })