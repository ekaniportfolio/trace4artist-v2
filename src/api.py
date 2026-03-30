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

from config import API_HOST, API_PORT, YOUTUBE_API_KEY, HUBSPOT_API_KEY, SPOTIFY_CLIENT_ID, GOOGLE_SEARCH_API_KEY
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


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — VIDÉOS RÉCENTES (Page 2)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/videos/recent", tags=["Vidéos"])
def get_recent_videos(
    limit  : int           = Query(20, ge=1, le=100),
    phase  : Optional[str] = Query(None, description="intensive|growth|passive"),
):
    """
    Dernières vidéos ajoutées avec leurs métriques de performance.
    Utilisé par le panneau 'Vidéos Récentes' du dashboard.
    """
    conditions = []
    params     = {"limit": limit}

    if phase:
        conditions.append("v.tracking_phase = :phase")
        params["phase"] = phase

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    with get_db() as conn:
        result = conn.execute(text(f"""
            SELECT
                v.video_id,
                v.title,
                v.view_count,
                v.like_count,
                v.comment_count,
                v.published_at,
                v.tracking_phase,
                v.is_suspicious,
                a.artist_name,
                a.country,
                a.channel_id,
                -- SPR calculé à la volée
                ROUND(
                    v.view_count::numeric
                    / NULLIF(a.subscriber_count, 0), 2
                ) as spr,
                -- Dernier snapshot
                vs.view_count as latest_snapshot_views,
                vs.snapped_at as snapshot_at
            FROM videos v
            JOIN artists a ON v.channel_id = a.channel_id
            LEFT JOIN LATERAL (
                SELECT view_count, snapped_at
                FROM view_snapshots
                WHERE video_id = v.video_id
                ORDER BY snapped_at DESC
                LIMIT 1
            ) vs ON true
            {where}
            ORDER BY v.detected_at DESC
            LIMIT :limit
        """), params)
        return [dict(row._mapping) for row in result.fetchall()]


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — CONTRÔLE DU BOT (Page 6)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/bot/status", tags=["Bot"])
def get_bot_status():
    """
    État actuel du bot : en marche, en erreur, ou arrêté.
    Basé sur les derniers scan_logs et la présence de scans récents.
    """
    with get_db() as conn:
        # Dernier scan
        result = conn.execute(text("""
            SELECT
                status,
                scan_type,
                started_at,
                completed_at,
                error_message,
                videos_found,
                quota_used
            FROM scan_logs
            ORDER BY started_at DESC
            LIMIT 1
        """))
        last_scan = result.fetchone()

        # Scans des dernières 24h
        activity = conn.execute(text("""
            SELECT COUNT(*) as count,
                   SUM(videos_found) as total_videos
            FROM scan_logs
            WHERE started_at >= NOW() - INTERVAL '24 hours'
              AND status = 'completed'
        """))
        activity_row = dict(activity.fetchone()._mapping)

    # Déterminer l'état du bot
    if not last_scan:
        bot_status = "never_started"
    elif last_scan[0] == "running":
        bot_status = "running"
    elif last_scan[0] == "failed":
        bot_status = "error"
    else:
        # Vérifier si le dernier scan date de moins de 12h
        from datetime import timedelta
        if last_scan[3]:  # completed_at
            age = datetime.now(timezone.utc) - last_scan[3].replace(
                tzinfo=timezone.utc
            )
            bot_status = "active" if age < timedelta(hours=12) else "idle"
        else:
            bot_status = "idle"

    return {
        "status"    : bot_status,
        # active | running | idle | error | never_started
        "last_scan" : dict(last_scan._mapping) if last_scan else None,
        "activity_24h": activity_row,
    }


@app.get("/bot/schedule", tags=["Bot"])
def get_bot_schedule():
    """
    Prochains déclenchements planifiés avec leur statut.
    Utilisé par le panneau de configuration des scans (Page 6).
    """
    settings = SettingsManager()

    detection_hour     = int(settings.get("tracking.detection_hour"))
    intensive_interval = int(settings.get("tracking.intensive_interval"))

    now = datetime.now(timezone.utc)

    # Calculer la prochaine heure de détection
    next_detection = now.replace(
        hour=detection_hour, minute=0, second=0, microsecond=0
    )
    if next_detection <= now:
        from datetime import timedelta
        next_detection += timedelta(days=1)

    # Calculer le prochain monitoring intensif
    hours_since_midnight = now.hour + now.minute / 60
    next_intensive_hour  = (
        (int(hours_since_midnight / intensive_interval) + 1)
        * intensive_interval
    ) % 24
    next_intensive = now.replace(
        hour=int(next_intensive_hour), minute=0,
        second=0, microsecond=0
    )

    with get_db() as conn:
        # Derniers scans par type
        result = conn.execute(text("""
            SELECT scan_type, status, started_at, completed_at, videos_found
            FROM scan_logs
            WHERE started_at >= NOW() - INTERVAL '7 days'
            ORDER BY started_at DESC
            LIMIT 10
        """))
        recent_scans = [dict(r._mapping) for r in result.fetchall()]

    return {
        "schedule": [
            {
                "job"        : "detect_new_videos",
                "description": "Détection quotidienne",
                "frequency"  : f"Quotidien à {detection_hour}h UTC",
                "next_run"   : next_detection.isoformat(),
            },
            {
                "job"        : "monitor_intensive",
                "description": "Monitoring intensif (vidéos < 7j)",
                "frequency"  : f"Toutes les {intensive_interval}h",
                "next_run"   : next_intensive.isoformat(),
            },
            {
                "job"        : "monitor_growth",
                "description": "Monitoring croissance (7-90j)",
                "frequency"  : "Hebdomadaire (lundi 01h UTC)",
                "next_run"   : None,   # Calculé dynamiquement
            },
            {
                "job"        : "monitor_passive",
                "description": "Monitoring passif (90-180j)",
                "frequency"  : "Mensuel (1er du mois 02h UTC)",
                "next_run"   : None,
            },
        ],
        "recent_scans": recent_scans,
        "settings"    : {
            "detection_hour"    : detection_hour,
            "intensive_interval": intensive_interval,
            "regions"           : settings.get_regions(),
        },
    }


@app.post("/bot/stop", tags=["Bot"])
def stop_bot():
    """
    Marque le bot comme arrêté en base.
    L'arrêt réel du scheduler se fait via Cloud Run (scale to 0).
    """
    with get_db() as conn:
        conn.execute(text("""
            INSERT INTO scan_logs (scan_type, status, error_message)
            VALUES ('manual', 'failed', 'Bot arrêté manuellement via API')
        """))
    return {
        "status" : "stop_requested",
        "message": "Arrêt enregistré. Pour arrêter complètement le scheduler, "
                   "scale le service Cloud Run à 0 instances.",
    }


@app.post("/bot/start", tags=["Bot"])
def start_bot():
    """
    Déclenche un scan de détection immédiat via Celery.
    """
    try:
        from src.worker import score_pending_artists
        score_pending_artists.apply_async(countdown=5)
        return {
            "status" : "started",
            "message": "Scan déclenché — résultats dans quelques minutes",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — STATS HEBDOMADAIRES (Pages 2 + 4)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/stats/weekly", tags=["Stats"])
def get_weekly_stats():
    """
    Agrégats par jour pour la semaine en cours et la précédente.
    Utilisé par les graphes de tendances du dashboard.
    """
    with get_db() as conn:
        # Nouvelles vidéos par jour (semaine en cours)
        videos_result = conn.execute(text("""
            SELECT
                DATE(detected_at)            as day,
                COUNT(*)                     as new_videos,
                COUNT(DISTINCT channel_id)   as new_artists
            FROM videos
            WHERE detected_at >= DATE_TRUNC('week', NOW())
            GROUP BY DATE(detected_at)
            ORDER BY day
        """))
        videos_by_day = [dict(r._mapping) for r in videos_result.fetchall()]

        # Comparaison semaine en cours vs semaine précédente
        comparison = conn.execute(text("""
            SELECT
                CASE
                    WHEN detected_at >= DATE_TRUNC('week', NOW())
                    THEN 'current_week'
                    ELSE 'previous_week'
                END as period,
                COUNT(DISTINCT channel_id) as artists,
                COUNT(*)                   as videos
            FROM videos
            WHERE detected_at >= DATE_TRUNC('week', NOW()) - INTERVAL '7 days'
            GROUP BY period
        """))
        comparison_data = {
            r[0]: {"artists": r[1], "videos": r[2]}
            for r in comparison.fetchall()
        }

        # Quotas par jour cette semaine
        quota_result = conn.execute(text("""
            SELECT
                date,
                SUM(units_used) as units,
                COUNT(*)        as api_calls
            FROM quota_log
            WHERE date >= DATE_TRUNC('week', NOW())::date
            GROUP BY date
            ORDER BY date
        """))
        quota_by_day = [dict(r._mapping) for r in quota_result.fetchall()]

        # Artistes qualifiés cette semaine
        qualified_result = conn.execute(text("""
            SELECT
                DATE(s.calculated_at)   as day,
                COUNT(*)                as qualified,
                AVG(s.score)            as avg_score
            FROM scores s
            WHERE s.calculated_at >= DATE_TRUNC('week', NOW())
              AND s.segment != 'low_priority'
            GROUP BY DATE(s.calculated_at)
            ORDER BY day
        """))
        qualified_by_day = [
            dict(r._mapping) for r in qualified_result.fetchall()
        ]

    # Calculer le taux de progression
    current  = comparison_data.get("current_week", {})
    previous = comparison_data.get("previous_week", {})

    def growth_rate(curr, prev):
        if not prev or prev == 0:
            return None
        return round((curr - prev) / prev * 100, 1)

    return {
        "videos_by_day"   : videos_by_day,
        "quota_by_day"    : quota_by_day,
        "qualified_by_day": qualified_by_day,
        "comparison"      : {
            "current_week" : current,
            "previous_week": previous,
            "growth_rate"  : {
                "artists": growth_rate(
                    current.get("artists", 0),
                    previous.get("artists", 0)
                ),
                "videos": growth_rate(
                    current.get("videos", 0),
                    previous.get("videos", 0)
                ),
            },
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — STATS COMMERCIALES DEPUIS HUBSPOT (Page 4)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/stats/commercial", tags=["Stats"])
def get_commercial_stats():
    """
    KPIs commerciaux récupérés depuis HubSpot CRM.
    contacted / replied / signed viennent de HubSpot,
    pas de notre base (évite la duplication de données).
    """
    import config as _config
    hubspot_key = _config.HUBSPOT_API_KEY

    if not hubspot_key:
        return {
            "status" : "not_configured",
            "message": "HubSpot non configuré",
            "data"   : None,
        }

    try:
        from hubspot import HubSpot
        hs = HubSpot(access_token=hubspot_key)

        # Compter les contacts par lifecycle stage
        # Les stages correspondent à nos segments Trace4Artist
        stages = ["lead", "marketingqualifiedlead", "salesqualifiedlead",
                  "opportunity", "customer"]

        counts = {}
        for stage in stages:
            result = hs.crm.contacts.search_api.do_search(
                public_object_search_request={
                    "filterGroups": [{
                        "filters": [{
                            "propertyName": "lifecyclestage",
                            "operator"    : "EQ",
                            "value"       : stage,
                        }]
                    }],
                    "limit": 1,
                }
            )
            counts[stage] = result.total

        # Artistes scannés depuis notre base
        with get_db() as conn:
            result = conn.execute(text("""
                SELECT
                    COUNT(*)                                       as total_scanned,
                    SUM(CASE WHEN status='qualified' THEN 1 END)   as qualified,
                    SUM(CASE WHEN status='activated' THEN 1 END)   as activated
                FROM artists
            """))
            our_data = dict(result.fetchone()._mapping)

        total_scanned = our_data.get("total_scanned", 0)
        activated     = our_data.get("activated", 0)
        contacted     = counts.get("lead", 0)
        replied       = counts.get("marketingqualifiedlead", 0)
        signed        = counts.get("customer", 0)

        def pct(value, total):
            if not total:
                return 0
            return round(value / total * 100, 1)

        return {
            "status": "ok",
            "data"  : {
                "scanned"  : {"count": total_scanned, "pct": 100},
                "qualified": {
                    "count": our_data.get("qualified", 0),
                    "pct"  : pct(our_data.get("qualified", 0), total_scanned),
                },
                "contacted": {
                    "count": contacted,
                    "pct"  : pct(contacted, total_scanned),
                },
                "replied"  : {
                    "count": replied,
                    "pct"  : pct(replied, total_scanned),
                },
                "signed"   : {
                    "count": signed,
                    "pct"  : pct(signed, total_scanned),
                },
            },
        }

    except Exception as e:
        logger.error(f"HubSpot stats error : {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Erreur lors de la récupération des données HubSpot : {e}"
        )


@app.get("/stats/commercial/weekly", tags=["Stats"])
def get_commercial_weekly():
    """
    Tendances commerciales hebdomadaires depuis HubSpot.
    Évolution quotidienne des contacts, réponses et contrats.
    """
    import config as _config
    hubspot_key = _config.HUBSPOT_API_KEY

    if not hubspot_key:
        return {"status": "not_configured", "data": []}

    try:
        from hubspot import HubSpot
        from datetime import timedelta

        hs  = HubSpot(access_token=hubspot_key)
        now = datetime.now(timezone.utc)

        weekly_data = []
        for i in range(6, -1, -1):
            day       = now - timedelta(days=i)
            day_start = day.replace(hour=0,  minute=0,  second=0)
            day_end   = day.replace(hour=23, minute=59, second=59)

            # Contacts créés ce jour
            created = hs.crm.contacts.search_api.do_search(
                public_object_search_request={
                    "filterGroups": [{
                        "filters": [
                            {
                                "propertyName": "createdate",
                                "operator"    : "BETWEEN",
                                "value"       : str(int(day_start.timestamp() * 1000)),
                                "highValue"   : str(int(day_end.timestamp() * 1000)),
                            },
                            {
                                "propertyName": "source_platform",
                                "operator"    : "EQ",
                                "value"       : "YouTube",
                            },
                        ]
                    }],
                    "limit": 1,
                }
            )

            weekly_data.append({
                "day"      : day.strftime("%Y-%m-%d"),
                "contacted": created.total,
                "replied"  : 0,   # À enrichir si HubSpot expose cet event
                "signed"   : 0,   # À enrichir si HubSpot expose cet event
            })

        return {"status": "ok", "data": weekly_data}

    except Exception as e:
        logger.error(f"HubSpot weekly stats error : {e}")
        raise HTTPException(status_code=502, detail=str(e))


# ──────────────────────────────────────────────────────────────────────────────
# ROUTE — API HEALTH CHECK (santé des APIs externes)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/stats/api-health", tags=["Stats"])
def get_api_health():
    """
    Santé des APIs externes utilisées par le système.
    Utilisé par le panneau 'Santé des APIs' du dashboard (Page 2).
    """
    import config as _config
    YOUTUBE_API_KEY      = _config.YOUTUBE_API_KEY
    HUBSPOT_API_KEY      = _config.HUBSPOT_API_KEY
    SPOTIFY_CLIENT_ID    = _config.SPOTIFY_CLIENT_ID
    GOOGLE_SEARCH_API_KEY= _config.GOOGLE_SEARCH_API_KEY

    results = {}

    # YouTube — vérifier via quota_log
    with get_db() as conn:
        quota = conn.execute(text("""
            SELECT
                COUNT(*)        as calls_today,
                SUM(units_used) as units_today
            FROM quota_log
            WHERE date = CURRENT_DATE
        """))
        yt_data = dict(quota.fetchone()._mapping)

    results["youtube"] = {
        "configured"  : bool(YOUTUBE_API_KEY),
        "calls_today" : yt_data.get("calls_today", 0),
        "units_today" : yt_data.get("units_today", 0),
        "quota_limit" : 10_000,
        "status"      : "ok" if YOUTUBE_API_KEY else "not_configured",
    }

    # HubSpot
    results["hubspot"] = {
        "configured": bool(HUBSPOT_API_KEY),
        "status"    : "ok" if HUBSPOT_API_KEY else "not_configured",
    }

    # Spotify
    results["spotify"] = {
        "configured": bool(SPOTIFY_CLIENT_ID),
        "status"    : "ok" if SPOTIFY_CLIENT_ID else "not_configured",
    }

    # Google Search
    results["google_search"] = {
        "configured": bool(GOOGLE_SEARCH_API_KEY),
        "status"    : "ok" if GOOGLE_SEARCH_API_KEY else "not_configured",
    }

    overall = "ok" if all(
        v["configured"] for v in results.values()
    ) else "partial"

    return {
        "overall": overall,
        "apis"   : results,
    }