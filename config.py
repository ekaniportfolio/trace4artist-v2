"""
config.py — Configuration centralisée Trace4Artist v2
"""

import os
from dotenv import load_dotenv

load_dotenv()


# ── YouTube API ────────────────────────────────────────────────────────
YOUTUBE_API_KEY          = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION      = "v3"

TARGET_REGIONS           = ["NG", "CI", "KE", "GH", "ZA", "CD", "SN", "CM"]
MUSIC_CATEGORY_ID        = "10"
MAX_RESULTS_PER_SEARCH   = 50
SEARCH_KEYWORDS          = '"official video" OR "clip officiel" OR "music video"'
SEARCH_ORDER             = "date"

# Première recherche : 1 an en arrière (configurable)
INITIAL_LOOKBACK_DAYS    = 365

DAILY_QUOTA_LIMIT        = 10_000
QUOTA_COST               = {
    "search.list"  : 100,
    "videos.list"  : 1,
    "channels.list": 1,
}
REQUEST_DELAY_SECONDS    = 0.5


# ── Base de données PostgreSQL ─────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://t4a_user:t4a_password@localhost:5432/trace4artist"
)


# ── Redis ──────────────────────────────────────────────────────────────
REDIS_URL        = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_CACHE_TTL  = 6 * 3600    # 6 heures en secondes


# ── Celery ─────────────────────────────────────────────────────────────
CELERY_BROKER_URL    = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL


# ── Scheduler ─────────────────────────────────────────────────────────
SCAN_INTERVAL_HOURS  = 6       # Fréquence des scans automatiques


# ── Spotify API ────────────────────────────────────────────────────────
SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")


# ── Google Custom Search ───────────────────────────────────────────────
GOOGLE_SEARCH_API_KEY = os.getenv("GOOGLE_SEARCH_API_KEY", "")
GOOGLE_SEARCH_CX      = os.getenv("GOOGLE_SEARCH_CX", "")
GOOGLE_SEARCH_DAILY_LIMIT = 100   # Quota gratuit


# ── HubSpot CRM ───────────────────────────────────────────────────────
HUBSPOT_API_KEY       = os.getenv("HUBSPOT_API_KEY", "")


# ── Scoring v2 ─────────────────────────────────────────────────────────
MIN_VIEWS             = 5_000   # Ajusté selon les données réelles du marché africain
MIN_SUBSCRIBERS       = 500    # Ajusté selon les données réelles du marché africain
VELOCITY_WINDOW_DAYS  = 7       # Fenêtre de calcul de vélocité

SCORE_SEGMENTS = {
    "high_potential": 80,   # Score >= 80
    "standard"      : 60,   # Score >= 60
    "emerging"      : 40,   # Score >= 40
    "low_priority"  : 0,    # Score < 40
}

ACTIVATION_SCORE_THRESHOLD = 70


# ── FastAPI ────────────────────────────────────────────────────────────
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))


# ── Validation ─────────────────────────────────────────────────────────
def validate_config():
    errors = []

    if not YOUTUBE_API_KEY:
        errors.append("YOUTUBE_API_KEY manquante")

    if not DATABASE_URL:
        errors.append("DATABASE_URL manquante")

    if errors:
        raise ValueError(
            "❌ Configuration invalide :\n" +
            "\n".join(f"   - {e}" for e in errors)
        )

    print("✅ Configuration valide")
    print(f"   → Base de données : {DATABASE_URL.split('@')[-1]}")
    print(f"   → Redis           : {REDIS_URL}")
    print(f"   → Régions cibles  : {', '.join(TARGET_REGIONS)}")