"""
src/api.py — API interne Trace4Artist

Sert de pont entre le pipeline de scraping et le monde extérieur :
    - Aujourd'hui : panneau de contrôle (Postman, browser)
    - Demain      : backend du dashboard React

Documentation auto-générée accessible sur :
    http://localhost:8000/docs     (Swagger UI)
    http://localhost:8000/redoc    (ReDoc)

Lancement :
    uvicorn src.api:app --reload --port 8000
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text

from config import API_HOST, API_PORT
from src.database import get_db
from src.settings_manager import SettingsManager

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# APP FASTAPI
# ──────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Trace4Artist API",
    description = "API interne du pipeline de prospection musicale africaine",
    version     = "2.0.0",
)

# CORS — permet au dashboard React de consommer l'API
app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],   # À restreindre en production
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)


# ──────────────────────────────────────────────────────────────────────────────
# MODÈLES PYDANTIC — Validation des données entrantes/sortantes
# ──────────────────────────────────────────────────────────────────────────────

class SettingUpdate(BaseModel):
    value: str


class ScanTriggerRequest(BaseModel):
    regions   : Optional[list[str]] = None   # None = tous les pays
    max_results: Optional[int]      = None   # None = valeur settings


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — ARTISTES
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/artists", tags=["Artistes"])
def list_artists(
    status  : Optional[str] = Query(None, description="Filter: discovered|qualified|rejected|activated"),
    segment : Optional[str] = Query(None, description="Filter: high_potential|standard|emerging|low_priority"),
    country : Optional[str] = Query(None, description="Code pays ISO (ex: CM)"),
    limit   : int           = Query(50,   ge=1, le=200),
    offset  : int           = Query(0,    ge=0),
):
    """
    Liste les artistes avec filtres optionnels et pagination.
    Triés par score décroissant.
    """
    conditions = []
    params     = {"limit": limit, "offset": offset}

    if status:
        conditions.append("a.status = :status")
        params["status"] = status
    if country:
        conditions.append("a.country = :country")
        params["country"] = country.upper()

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Filtre sur le segment nécessite une jointure avec scores
    segment_join  = ""
    segment_where = ""
    if segment:
        segment_join  = """
            LEFT JOIN LATERAL (
                SELECT segment FROM scores
                WHERE channel_id = a.channel_id
                ORDER BY calculated_at DESC LIMIT 1
            ) s ON true
        """
        segment_where = f"{'AND' if where else 'WHERE'} s.segment = :segment"
        params["segment"] = segment

    with get_db() as conn:
        # Compter le total pour la pagination
        count_result = conn.execute(text(f"""
            SELECT COUNT(*) FROM artists a
            {segment_join}
            {where} {segment_where}
        """), params)
        total = count_result.scalar()

        # Récupérer les artistes
        result = conn.execute(text(f"""
            SELECT
                a.channel_id, a.artist_name, a.country,
                a.subscriber_count, a.total_views,
                a.email, a.website, a.instagram,
                a.status, a.hubspot_contact_id,
                a.created_at, a.updated_at,
                s2.score, s2.segment
            FROM artists a
            {segment_join}
            LEFT JOIN LATERAL (
                SELECT score, segment FROM scores
                WHERE channel_id = a.channel_id
                ORDER BY calculated_at DESC LIMIT 1
            ) s2 ON true
            {where} {segment_where}
            ORDER BY s2.score DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """), params)

        artists = [dict(row._mapping) for row in result.fetchall()]

    return {
        "total"  : total,
        "limit"  : limit,
        "offset" : offset,
        "items"  : artists,
    }


@app.get("/artists/top", tags=["Artistes"])
def get_top_artists(limit: int = Query(10, ge=1, le=50)):
    """
    Retourne les N meilleurs artistes qualifiés par segment.
    Endpoint principal pour le dashboard React.
    """
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT
                a.channel_id, a.artist_name, a.country,
                a.subscriber_count, a.email, a.website,
                a.enrichment_data,
                s.score, s.segment, s.criteria_breakdown,
                s.calculated_at
            FROM artists a
            JOIN LATERAL (
                SELECT score, segment, criteria_breakdown, calculated_at
                FROM scores
                WHERE channel_id = a.channel_id
                ORDER BY calculated_at DESC LIMIT 1
            ) s ON true
            WHERE a.status = 'qualified'
            ORDER BY s.score DESC
            LIMIT :limit
        """), {"limit": limit})
        return [dict(row._mapping) for row in result.fetchall()]


@app.get("/artists/{channel_id}", tags=["Artistes"])
def get_artist(channel_id: str):
    """Détail complet d'un artiste avec historique des scores."""
    with get_db() as conn:
        # Artiste
        result = conn.execute(text("""
            SELECT * FROM artists WHERE channel_id = :channel_id
        """), {"channel_id": channel_id})
        artist = result.fetchone()

        if not artist:
            raise HTTPException(status_code=404, detail="Artiste non trouvé")

        artist_dict = dict(artist._mapping)

        # Historique des scores
        scores_result = conn.execute(text("""
            SELECT score, segment, criteria_breakdown, calculated_at
            FROM scores
            WHERE channel_id = :channel_id
            ORDER BY calculated_at DESC
            LIMIT 10
        """), {"channel_id": channel_id})
        artist_dict["score_history"] = [
            dict(r._mapping) for r in scores_result.fetchall()
        ]

        # Vidéos
        videos_result = conn.execute(text("""
            SELECT video_id, title, view_count, like_count,
                   comment_count, published_at, tracking_phase
            FROM videos
            WHERE channel_id = :channel_id
            ORDER BY published_at DESC
            LIMIT 20
        """), {"channel_id": channel_id})
        artist_dict["videos"] = [
            dict(r._mapping) for r in videos_result.fetchall()
        ]

    return artist_dict


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — SCAN & MONITORING
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/scan/status", tags=["Scan"])
def get_scan_status():
    """État du dernier scan et quota YouTube du jour."""
    with get_db() as conn:
        # Dernier scan
        result = conn.execute(text("""
            SELECT scan_type, status, videos_found,
                   quota_used, started_at, completed_at, error_message
            FROM scan_logs
            ORDER BY started_at DESC
            LIMIT 1
        """))
        last_scan = result.fetchone()

        # Quota du jour
        quota_result = conn.execute(text("""
            SELECT COALESCE(SUM(units_used), 0) as used
            FROM quota_log
            WHERE date = CURRENT_DATE
        """))
        quota_used = quota_result.scalar() or 0

        # Stats phases de tracking
        phases_result = conn.execute(text("""
            SELECT tracking_phase, COUNT(*) as count
            FROM videos
            GROUP BY tracking_phase
        """))
        phases = {row[0]: row[1] for row in phases_result.fetchall()}

    return {
        "last_scan"   : dict(last_scan._mapping) if last_scan else None,
        "quota_today" : {
            "used"     : quota_used,
            "remaining": 10_000 - quota_used,
            "limit"    : 10_000,
        },
        "tracking_phases": phases,
    }


@app.post("/scan/trigger", tags=["Scan"])
def trigger_scan(request: ScanTriggerRequest = None):
    """
    Déclenche un scan de détection manuel via Celery.
    Utile pour tester ou lancer un scan hors planning.
    """
    from src.worker import fetch_video_details
    from src.scheduler import DetectionJob

    try:
        job = DetectionJob()

        # Override des paramètres si fournis
        if request and request.regions:
            job.settings = SettingsManager()

        # Lancer en arrière-plan via Celery
        # (ne bloque pas la réponse API)
        from celery import current_app
        current_app.send_task("tasks.fetch_video_details",
                              kwargs={"video_ids": [], "channel_ids": [],
                                      "region": "manual"})

        return {
            "status" : "triggered",
            "message": "Scan déclenché en arrière-plan",
            "regions": request.regions if request else "all",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scan/logs", tags=["Scan"])
def get_scan_logs(limit: int = Query(20, ge=1, le=100)):
    """Historique des scans avec statuts et métriques."""
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT scan_type, status, videos_found, artists_created,
                   quota_used, started_at, completed_at, error_message
            FROM scan_logs
            ORDER BY started_at DESC
            LIMIT :limit
        """), {"limit": limit})
        return [dict(row._mapping) for row in result.fetchall()]


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — SETTINGS
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/settings", tags=["Settings"])
def get_all_settings():
    """Retourne tous les paramètres configurables avec leurs valeurs."""
    sm = SettingsManager()
    return sm.get_all()


@app.patch("/settings/{key}", tags=["Settings"])
def update_setting(key: str, body: SettingUpdate):
    """
    Modifie un paramètre à chaud — sans redémarrage.

    Exemples :
        PATCH /settings/scan.regions       {"value": "CM,NG,CI"}
        PATCH /settings/scan.interval_hours {"value": "3"}
        PATCH /settings/scan.lookback_days  {"value": "30"}
    """
    sm = SettingsManager()
    try:
        result = sm.set(key, body.value)
        return {"status": "updated", **result}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — DASHBOARD STATS
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/stats/dashboard", tags=["Stats"])
def get_dashboard_stats():
    """
    KPIs principaux pour le tableau de bord.
    Endpoint appelé toutes les 30s par le dashboard React.
    """
    with get_db() as conn:
        # Compteurs artistes
        artists_result = conn.execute(text("""
            SELECT
                COUNT(*)                                        as total,
                SUM(CASE WHEN status='qualified'  THEN 1 END)  as qualified,
                SUM(CASE WHEN status='rejected'   THEN 1 END)  as rejected,
                SUM(CASE WHEN status='discovered' THEN 1 END)  as pending,
                SUM(CASE WHEN status='activated'  THEN 1 END)  as activated,
                SUM(CASE WHEN email IS NOT NULL   THEN 1 END)  as with_email
            FROM artists
        """))
        artists = dict(artists_result.fetchone()._mapping)

        # Répartition par segment
        segments_result = conn.execute(text("""
            SELECT s.segment, COUNT(DISTINCT s.channel_id) as count
            FROM scores s
            JOIN (
                SELECT channel_id, MAX(calculated_at) as latest
                FROM scores GROUP BY channel_id
            ) latest ON s.channel_id = latest.channel_id
                    AND s.calculated_at = latest.latest
            GROUP BY s.segment
        """))
        segments = {row[0]: row[1] for row in segments_result.fetchall()}

        # Quota du jour
        quota_result = conn.execute(text("""
            SELECT COALESCE(SUM(units_used), 0) FROM quota_log
            WHERE date = CURRENT_DATE
        """))
        quota_used = quota_result.scalar() or 0

        # Dernier scan
        scan_result = conn.execute(text("""
            SELECT status, completed_at, videos_found
            FROM scan_logs ORDER BY started_at DESC LIMIT 1
        """))
        last_scan = scan_result.fetchone()

        # Alertes non traitées
        alerts_result = conn.execute(text("""
            SELECT alert_type, COUNT(*) as count
            FROM video_alerts
            WHERE is_processed = FALSE
            GROUP BY alert_type
        """))
        alerts = {row[0]: row[1] for row in alerts_result.fetchall()}

    return {
        "artists"  : artists,
        "segments" : segments,
        "quota"    : {
            "used"     : quota_used,
            "remaining": 10_000 - quota_used,
            "limit"    : 10_000,
            "percent"  : round(quota_used / 10_000 * 100, 1),
        },
        "last_scan": dict(last_scan._mapping) if last_scan else None,
        "alerts"   : alerts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/stats/quota", tags=["Stats"])
def get_quota_history(days: int = Query(7, ge=1, le=30)):
    """Historique de consommation du quota YouTube sur N jours."""
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT
                date,
                SUM(units_used)                              as total_units,
                SUM(CASE WHEN endpoint='search.list'   THEN units_used END) as search_units,
                SUM(CASE WHEN endpoint='videos.list'   THEN units_used END) as videos_units,
                SUM(CASE WHEN endpoint='channels.list' THEN units_used END) as channels_units
            FROM quota_log
            WHERE date >= CURRENT_DATE - INTERVAL ':days days'
            GROUP BY date
            ORDER BY date DESC
        """), {"days": days})
        return [dict(row._mapping) for row in result.fetchall()]


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — ALERTES
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/alerts", tags=["Alertes"])
def get_alerts(
    alert_type  : Optional[str] = Query(None),
    processed   : bool          = Query(False),
    limit       : int           = Query(20, ge=1, le=100),
):
    """Breakouts, anomalies et changements de phase récents."""
    conditions = ["is_processed = :processed"]
    params     = {"processed": processed, "limit": limit}

    if alert_type:
        conditions.append("alert_type = :alert_type")
        params["alert_type"] = alert_type

    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        result = conn.execute(text(f"""
            SELECT
                va.id, va.video_id, va.channel_id,
                va.alert_type, va.details,
                va.is_processed, va.detected_at,
                a.artist_name
            FROM video_alerts va
            LEFT JOIN artists a ON va.channel_id = a.channel_id
            {where}
            ORDER BY va.detected_at DESC
            LIMIT :limit
        """), params)
        return [dict(row._mapping) for row in result.fetchall()]


@app.patch("/alerts/{alert_id}/process", tags=["Alertes"])
def mark_alert_processed(alert_id: int):
    """Marque une alerte comme traitée."""
    with get_db() as conn:
        result = conn.execute(text("""
            UPDATE video_alerts SET is_processed = TRUE
            WHERE id = :id RETURNING id
        """), {"id": alert_id})
        if not result.fetchone():
            raise HTTPException(status_code=404, detail="Alerte non trouvée")
    return {"status": "processed", "id": alert_id}


# ──────────────────────────────────────────────────────────────────────────────
# HEALTH CHECK
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Système"])
def health_check():
    """Vérifie que l'API et la base de données sont opérationnelles."""
    try:
        with get_db() as conn:
            conn.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {e}"

    return {
        "status"    : "ok" if db_status == "ok" else "degraded",
        "database"  : db_status,
        "timestamp" : datetime.now(timezone.utc).isoformat(),
    }
