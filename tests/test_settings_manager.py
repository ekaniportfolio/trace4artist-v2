"""
tests/test_settings_manager.py — Tests du gestionnaire de paramètres

On teste ici :
- La lecture avec fallback sur les defaults si la BDD est inaccessible
- La validation de chaque paramètre (bornes, types, format)
- La mise à jour à chaud
- Les accesseurs typés (get_regions retourne une liste, etc.)
"""

import pytest
from unittest.mock import patch, MagicMock

from src.settings_manager import SettingsManager, DEFAULTS


@pytest.fixture
def settings():
    return SettingsManager()


# ──────────────────────────────────────────────────────────────────────
# TESTS : LECTURE ET FALLBACK
# ──────────────────────────────────────────────────────────────────────

class TestGetWithFallback:

    def test_returns_db_value_when_available(self, settings):
        """La valeur en base prime sur le default."""
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: "30"

        with patch("src.settings_manager.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.return_value.fetchone.return_value = ("30",)

            result = settings.get("scan.lookback_days")

        assert result == "30"

    def test_falls_back_to_default_when_db_unavailable(self, settings):
        """Si la BDD est down, on retourne le default de config.py."""
        with patch("src.settings_manager.get_db",
                   side_effect=Exception("DB down")):
            result = settings.get("scan.lookback_days")

        assert result == DEFAULTS["scan.lookback_days"]

    def test_falls_back_to_default_when_key_not_in_db(self, settings):
        """Clé absente de la BDD → default."""
        with patch("src.settings_manager.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.return_value.fetchone.return_value = None

            result = settings.get("scan.interval_hours")

        assert result == DEFAULTS["scan.interval_hours"]

    def test_raises_for_unknown_key(self, settings):
        """Une clé inconnue doit lever une KeyError claire."""
        with patch("src.settings_manager.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.return_value.fetchone.return_value = None

            with pytest.raises(KeyError, match="inconnu"):
                settings.get("scan.inexistant")


# ──────────────────────────────────────────────────────────────────────
# TESTS : ACCESSEURS TYPÉS
# ──────────────────────────────────────────────────────────────────────

class TestTypedAccessors:

    def test_get_regions_returns_list(self, settings):
        """get_regions() doit retourner une liste de strings."""
        with patch.object(settings, "get", return_value="CM,NG,CI"):
            result = settings.get_regions()

        assert result == ["CM", "NG", "CI"]

    def test_get_regions_strips_spaces(self, settings):
        """Les espaces autour des codes pays doivent être supprimés."""
        with patch.object(settings, "get", return_value=" CM , NG , CI "):
            result = settings.get_regions()

        assert result == ["CM", "NG", "CI"]

    def test_get_regions_uppercases(self, settings):
        """Les codes pays doivent être en majuscules."""
        with patch.object(settings, "get", return_value="cm,ng"):
            result = settings.get_regions()

        assert result == ["CM", "NG"]

    def test_get_scan_interval_returns_int(self, settings):
        with patch.object(settings, "get", return_value="6"):
            assert settings.get_scan_interval() == 6

    def test_get_lookback_days_returns_int(self, settings):
        with patch.object(settings, "get", return_value="365"):
            assert settings.get_lookback_days() == 365

    def test_get_max_results_returns_int(self, settings):
        with patch.object(settings, "get", return_value="50"):
            assert settings.get_max_results() == 50


# ──────────────────────────────────────────────────────────────────────
# TESTS : VALIDATION
# ──────────────────────────────────────────────────────────────────────

class TestValidation:

    def test_lookback_days_must_be_integer(self, settings):
        with pytest.raises(ValueError, match="entier"):
            settings._validate("scan.lookback_days", "pas_un_nombre")

    def test_lookback_days_max_5_years(self, settings):
        with pytest.raises(ValueError, match="1825"):
            settings._validate("scan.lookback_days", "2000")

    def test_lookback_days_min_1(self, settings):
        with pytest.raises(ValueError, match="1"):
            settings._validate("scan.lookback_days", "0")

    def test_interval_hours_must_be_1_to_24(self, settings):
        with pytest.raises(ValueError, match="24h"):
            settings._validate("scan.interval_hours", "25")

    def test_max_results_must_be_1_to_50(self, settings):
        """YouTube accepte max 50 résultats par requête."""
        with pytest.raises(ValueError, match="50"):
            settings._validate("scan.max_results", "51")

    def test_regions_cannot_be_empty(self, settings):
        with pytest.raises(ValueError, match="vide"):
            settings._validate("scan.regions", "")

    def test_regions_max_20_countries(self, settings):
        too_many = ",".join([f"C{i}" for i in range(21)])
        with pytest.raises(ValueError, match="20"):
            settings._validate("scan.regions", too_many)

    def test_keywords_cannot_be_empty(self, settings):
        with pytest.raises(ValueError, match="vide"):
            settings._validate("scan.keywords", "   ")

    def test_valid_values_pass_validation(self, settings):
        """Les valeurs dans les bornes ne doivent pas lever d'exception."""
        settings._validate("scan.lookback_days",  "30")
        settings._validate("scan.interval_hours", "6")
        settings._validate("scan.max_results",    "50")
        settings._validate("scan.regions",        "CM,NG,CI")
        settings._validate("scan.keywords",        "official video")