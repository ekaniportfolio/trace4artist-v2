"""
src/settings_manager.py — Gestion des paramètres dynamiques

Les paramètres sont lus depuis PostgreSQL à chaque appel.
config.py sert uniquement de fallback si la table est vide
ou inaccessible.

Utilisation :
    settings = SettingsManager()
    regions  = settings.get_regions()      # ["CM", "NG", ...]
    hours    = settings.get_scan_interval() # 6
    
    settings.set("scan.interval_hours", "3")  # Changement à chaud
"""

import logging
from sqlalchemy import text

from config import (
    TARGET_REGIONS,
    INITIAL_LOOKBACK_DAYS,
    SCAN_INTERVAL_HOURS,
    MAX_RESULTS_PER_SEARCH,
    SEARCH_KEYWORDS,
)
from src.database import get_db

logger = logging.getLogger(__name__)

# Valeurs par défaut issues de config.py
# Utilisées si PostgreSQL est inaccessible ou si la clé n'existe pas
DEFAULTS = {
    # Paramètres de scan
    "scan.lookback_days" : str(INITIAL_LOOKBACK_DAYS),
    "scan.interval_hours": str(SCAN_INTERVAL_HOURS),
    "scan.regions"       : ",".join(TARGET_REGIONS),
    "scan.max_results"   : str(MAX_RESULTS_PER_SEARCH),
    "scan.keywords"      : SEARCH_KEYWORDS,

    # Paramètres de tracking tiered (migration 004)
    "tracking.detection_hour"    : "0",
    "tracking.intensive_interval": "6",
    "tracking.intensive_max_days": "7",
    "tracking.growth_max_days"   : "90",
    "tracking.passive_max_days"  : "180",
    "tracking.keep_qualified"    : "true",
    "tracking.breakout_threshold": "0.50",  # Seuil mis a jour (etait 0.20)

    # Paramètres d'enrichissement (migration 005 + 006)
    "enrichment.enabled"              : "true",

    # Auth
    "auth.jwt_expire_hours"       : "24",
    "enrichment.spotify_enabled"      : "true",
    "enrichment.spotify_min_popularity": "10",
}


class SettingsManager:
    """
    Lit et écrit les paramètres de scan dans PostgreSQL.
    Chaque lecture va en base — les changements via API
    sont immédiatement pris en compte au prochain scan.
    """

    def get(self, key: str) -> str:
        """
        Lit un paramètre depuis PostgreSQL.
        Retourne la valeur par défaut si la clé est absente
        ou si la base est inaccessible.
        """
        try:
            with get_db() as conn:
                result = conn.execute(
                    text("SELECT value FROM settings WHERE key = :key"),
                    {"key": key},
                )
                row = result.fetchone()
                if row:
                    return row[0]
        except Exception as e:
            logger.warning(f"SettingsManager.get({key}) — DB error : {e}")

        # Fallback sur les valeurs par défaut
        default = DEFAULTS.get(key)
        if default is None:
            raise KeyError(f"Paramètre inconnu : '{key}'")
        return default

    def set(self, key: str, value: str) -> dict:
        """
        Met à jour un paramètre dans PostgreSQL.
        Le changement est effectif immédiatement pour
        tous les composants qui lisent depuis la base.

        Retourne le paramètre mis à jour.
        Lève KeyError si la clé n'est pas reconnue.
        """
        if key not in DEFAULTS:
            raise KeyError(f"Paramètre inconnu : '{key}'")

        # Validation selon le type attendu
        self._validate(key, value)

        with get_db() as conn:
            conn.execute(text("""
                INSERT INTO settings (key, value, updated_at)
                VALUES (:key, :value, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value      = EXCLUDED.value,
                    updated_at = NOW()
            """), {"key": key, "value": value})

        logger.info(f"Setting mis à jour : {key} = {value!r}")
        return {"key": key, "value": value}

    def get_all(self) -> list[dict]:
        """
        Retourne tous les paramètres avec leurs valeurs actuelles.
        Utilisé par GET /settings dans l'API.
        """
        try:
            with get_db() as conn:
                result = conn.execute(text("""
                    SELECT key, value, description, updated_at
                    FROM settings
                    ORDER BY key
                """))
                return [dict(row._mapping) for row in result.fetchall()]
        except Exception as e:
            logger.warning(f"SettingsManager.get_all() — DB error : {e}")
            # Retourner les defaults si la BDD est inaccessible
            return [
                {"key": k, "value": v, "description": None, "updated_at": None}
                for k, v in DEFAULTS.items()
            ]

    # ──────────────────────────────────────────────────────────────────
    # ACCESSEURS TYPÉS
    # Chaque méthode caste la valeur TEXT au bon type Python.
    # Le scheduler et les workers utilisent ces méthodes,
    # jamais les constantes de config.py directement.
    # ──────────────────────────────────────────────────────────────────

    def get_lookback_days(self) -> int:
        return int(self.get("scan.lookback_days"))

    def get_scan_interval(self) -> int:
        return int(self.get("scan.interval_hours"))

    def get_regions(self) -> list[str]:
        raw = self.get("scan.regions")
        return [r.strip().upper() for r in raw.split(",") if r.strip()]

    def get_max_results(self) -> int:
        return int(self.get("scan.max_results"))

    def get_keywords(self) -> str:
        return self.get("scan.keywords")

    # ──────────────────────────────────────────────────────────────────
    # VALIDATION
    # ──────────────────────────────────────────────────────────────────

    def _validate(self, key: str, value: str):
        """
        Valide la valeur avant de l'écrire en base.
        Lève ValueError avec un message clair si invalide.
        """
        if key in (
            "scan.lookback_days", "scan.interval_hours", "scan.max_results",
            "tracking.intensive_interval", "tracking.intensive_max_days",
            "tracking.growth_max_days", "tracking.passive_max_days",
            "tracking.detection_hour",
        ):
            try:
                int_val = int(value)
            except ValueError:
                raise ValueError(f"'{key}' doit être un entier, reçu : {value!r}")

            if key == "scan.lookback_days" and not (1 <= int_val <= 1825):
                raise ValueError(
                    f"scan.lookback_days doit être entre 1 et 1825 jours "
                    f"(5 ans max), reçu : {int_val}"
                )
            if key == "scan.interval_hours" and not (1 <= int_val <= 24):
                raise ValueError(
                    f"scan.interval_hours doit être entre 1h et 24h, "
                    f"reçu : {int_val}"
                )
            if key == "scan.max_results" and not (1 <= int_val <= 50):
                raise ValueError(
                    f"scan.max_results doit être entre 1 et 50 "
                    f"(limite YouTube), reçu : {int_val}"
                )

        if key == "scan.regions":
            regions = [r.strip() for r in value.split(",") if r.strip()]
            if not regions:
                raise ValueError("scan.regions ne peut pas être vide")
            if len(regions) > 20:
                raise ValueError(
                    f"scan.regions : maximum 20 pays, reçu : {len(regions)}"
                )

        if key == "scan.keywords" and not value.strip():
            raise ValueError("scan.keywords ne peut pas être vide")