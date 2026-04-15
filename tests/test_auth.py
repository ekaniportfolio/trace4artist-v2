"""
tests/test_auth.py — Tests du module d'authentification JWT

Couvre :
    - Hachage et vérification des mots de passe (bcrypt)
    - Création et décodage des tokens JWT
    - Dépendances FastAPI (get_current_user, require_admin...)
    - Routes /auth/login, /auth/me
    - Routes /users (CRUD admin)
    - Protection des routes d'action
"""

import pytest
from unittest.mock import patch, MagicMock
from starlette.testclient import TestClient

from src.auth import (
    hash_password, verify_password,
    create_access_token, decode_token,
    TokenData,
)
from src.api import app

client = TestClient(app)


# ──────────────────────────────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture
def admin_token():
    return create_access_token(user_id=1, username="admin", role="admin")


@pytest.fixture
def manager_token():
    return create_access_token(user_id=2, username="manager", role="manager")


@pytest.fixture
def technician_token():
    return create_access_token(user_id=3, username="tech", role="technician")


@pytest.fixture
def sample_user():
    return {
        "id"           : 1,
        "username"     : "admin",
        "email"        : "admin@trace4artist.com",
        "full_name"    : "Administrateur",
        "role"         : "admin",
        "password_hash": hash_password("Admin@T4A2025!"),
        "is_active"    : True,
        "created_at"   : "2024-01-01T00:00:00Z",
        "last_login"   : None,
    }


# ──────────────────────────────────────────────────────────────────────
# TESTS : MOT DE PASSE
# ──────────────────────────────────────────────────────────────────────

class TestPassword:

    def test_hash_is_not_plaintext(self):
        hashed = hash_password("monmotdepasse")
        assert hashed != "monmotdepasse"
        assert hashed.startswith("$2b$")  # Format bcrypt

    def test_verify_correct_password(self):
        hashed = hash_password("monmotdepasse")
        assert verify_password("monmotdepasse", hashed) is True

    def test_reject_wrong_password(self):
        hashed = hash_password("monmotdepasse")
        assert verify_password("mauvais", hashed) is False

    def test_two_hashes_of_same_password_differ(self):
        """bcrypt utilise un sel aléatoire — deux hashes sont toujours différents."""
        h1 = hash_password("motdepasse")
        h2 = hash_password("motdepasse")
        assert h1 != h2
        assert verify_password("motdepasse", h1)
        assert verify_password("motdepasse", h2)


# ──────────────────────────────────────────────────────────────────────
# TESTS : JWT TOKEN
# ──────────────────────────────────────────────────────────────────────

class TestJWT:

    def test_create_and_decode_token(self):
        token = create_access_token(user_id=1, username="admin", role="admin")
        data  = decode_token(token)
        assert data.user_id  == 1
        assert data.username == "admin"
        assert data.role     == "admin"

    def test_invalid_token_raises_401(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc:
            decode_token("token.invalide.ici")
        assert exc.value.status_code == 401

    def test_token_contains_role(self):
        for role in ("admin", "manager", "technician"):
            token = create_access_token(1, "user", role)
            data  = decode_token(token)
            assert data.role == role


# ──────────────────────────────────────────────────────────────────────
# TESTS : ROUTE /auth/login
# ──────────────────────────────────────────────────────────────────────

class TestLogin:

    def test_login_success(self, sample_user):
        with patch("src.api.get_user_by_email", return_value=sample_user), \
             patch("src.api.update_last_login"):
            response = client.post("/auth/login", json={
                "email"   : "admin@trace4artist.com",
                "password": "Admin@T4A2025!",
            })

        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert data["user"]["role"] == "admin"

    def test_login_wrong_password(self, sample_user):
        with patch("src.api.get_user_by_email", return_value=sample_user):
            response = client.post("/auth/login", json={
                "email"   : "admin@trace4artist.com",
                "password": "mauvais",
            })

        assert response.status_code == 401

    def test_login_unknown_email(self):
        with patch("src.api.get_user_by_email", return_value=None):
            response = client.post("/auth/login", json={
                "email"   : "inconnu@test.com",
                "password": "test",
            })

        assert response.status_code == 401

    def test_login_inactive_user(self, sample_user):
        inactive = {**sample_user, "is_active": False}
        with patch("src.api.get_user_by_email", return_value=inactive), \
             patch("src.api.verify_password", return_value=True):
            response = client.post("/auth/login", json={
                "email"   : "admin@trace4artist.com",
                "password": "Admin@T4A2025!",
            })

        assert response.status_code == 403


# ──────────────────────────────────────────────────────────────────────
# TESTS : ROUTE /auth/me
# ──────────────────────────────────────────────────────────────────────

class TestGetMe:

    def test_get_me_with_valid_token(self, admin_token, sample_user):
        with patch("src.auth.get_db") as mock_db, \
             patch("src.api.get_user_by_id", return_value=sample_user):
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = (1, True)
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            response = client.get(
                "/auth/me",
                headers={"Authorization": f"Bearer {admin_token}"},
            )

        assert response.status_code == 200
        assert response.json()["username"] == "admin"

    def test_get_me_without_token(self):
        response = client.get("/auth/me")
        assert response.status_code == 401

    def test_get_me_with_invalid_token(self):
        response = client.get(
            "/auth/me",
            headers={"Authorization": "Bearer token.invalide"},
        )
        assert response.status_code == 401


# ──────────────────────────────────────────────────────────────────────
# TESTS : GESTION DES UTILISATEURS (admin only)
# ──────────────────────────────────────────────────────────────────────

class TestUsersRoutes:

    def _auth_headers(self, token):
        return {"Authorization": f"Bearer {token}"}

    def _mock_db(self, active=True, user_id=1):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (user_id, active)
        mock_conn.__enter__ = lambda s: mock_conn
        mock_conn.__exit__  = MagicMock(return_value=False)
        return mock_conn

    def test_list_users_admin_ok(self, admin_token):
        with patch("src.auth.get_db") as mock_db, \
             patch("src.api.get_all_users", return_value=[]):
            mock_db.return_value = self._mock_db()
            response = client.get(
                "/users",
                headers=self._auth_headers(admin_token),
            )
        assert response.status_code == 200

    def test_list_users_manager_forbidden(self, manager_token):
        with patch("src.auth.get_db") as mock_db:
            mock_db.return_value = self._mock_db(user_id=2)
            response = client.get(
                "/users",
                headers=self._auth_headers(manager_token),
            )
        assert response.status_code == 403

    def test_list_users_no_token(self):
        response = client.get("/users")
        assert response.status_code == 401

    def test_create_user_admin_ok(self, admin_token):
        new_user = {
            "id": 5, "username": "newuser", "email": "new@test.com",
            "full_name": "New User", "role": "technician",
            "is_active": True, "created_at": "2024-01-01T00:00:00Z",
        }
        with patch("src.auth.get_db") as mock_db, \
             patch("src.api.create_user", return_value=new_user):
            mock_db.return_value = self._mock_db()
            response = client.post(
                "/users",
                headers=self._auth_headers(admin_token),
                json={
                    "username" : "newuser",
                    "email"    : "new@test.com",
                    "full_name": "New User",
                    "password" : "Pass@123",
                    "role"     : "technician",
                },
            )
        assert response.status_code == 201

    def test_create_user_duplicate_raises_422(self, admin_token):
        with patch("src.auth.get_db") as mock_db, \
             patch("src.api.create_user",
                   side_effect=ValueError("Email ou username déjà utilisé")):
            mock_db.return_value = self._mock_db()
            response = client.post(
                "/users",
                headers=self._auth_headers(admin_token),
                json={
                    "username" : "admin",
                    "email"    : "admin@trace4artist.com",
                    "full_name": "Admin",
                    "password" : "test",
                    "role"     : "admin",
                },
            )
        assert response.status_code == 422

    def test_delete_own_account_forbidden(self, admin_token):
        """Un admin ne peut pas supprimer son propre compte (id=1)."""
        with patch("src.auth.get_db") as mock_db:
            mock_db.return_value = self._mock_db(user_id=1)
            response = client.delete(
                "/users/1",
                headers=self._auth_headers(admin_token),
            )
        assert response.status_code == 422

    def test_delete_other_user_ok(self, admin_token):
        with patch("src.auth.get_db") as mock_db, \
             patch("src.api.delete_user", return_value=True):
            mock_db.return_value = self._mock_db(user_id=1)
            response = client.delete(
                "/users/99",
                headers=self._auth_headers(admin_token),
            )
        assert response.status_code == 200


# ──────────────────────────────────────────────────────────────────────
# TESTS : PROTECTION DES ROUTES D'ACTION
# ──────────────────────────────────────────────────────────────────────

class TestProtectedRoutes:

    def _mock_db(self, user_id=3, active=True):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (user_id, active)
        mock_conn.__enter__ = lambda s: mock_conn
        mock_conn.__exit__  = MagicMock(return_value=False)
        return mock_conn

    def test_patch_settings_requires_auth(self):
        """PATCH /settings sans token → 401."""
        response = client.patch(
            "/settings/scan.lookback_days",
            json={"value": "30"},
        )
        assert response.status_code == 401

    def test_patch_settings_manager_forbidden(self, manager_token):
        """PATCH /settings avec rôle manager → 403."""
        with patch("src.auth.get_db") as mock_db:
            mock_db.return_value = self._mock_db(user_id=2)
            response = client.patch(
                "/settings/scan.lookback_days",
                headers={"Authorization": f"Bearer {manager_token}"},
                json={"value": "30"},
            )
        assert response.status_code == 403

    def test_patch_settings_technician_ok(self, technician_token):
        """PATCH /settings avec rôle technician → autorisé."""
        with patch("src.auth.get_db") as mock_db, \
             patch("src.api.SettingsManager") as MockSM:
            mock_db.return_value = self._mock_db(user_id=3)
            MockSM.return_value.set.return_value = {
                "key": "scan.lookback_days", "value": "30"
            }
            response = client.patch(
                "/settings/scan.lookback_days",
                headers={"Authorization": f"Bearer {technician_token}"},
                json={"value": "30"},
            )
        assert response.status_code == 200

    def test_public_routes_still_accessible(self):
        """Les routes publiques restent accessibles sans token."""
        with patch("src.api.get_db") as mock_db:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchall.return_value = []
            mock_conn.execute.return_value.fetchone.return_value = \
                MagicMock(_mapping={
                    "total": 0, "qualified": 0, "rejected": 0,
                    "pending": 0, "activated": 0, "with_email": 0,
                })
            mock_conn.execute.return_value.scalar.return_value = 0
            mock_conn.__enter__ = lambda s: mock_conn
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_db.return_value = mock_conn

            # Ces routes sont publiques — pas de token requis
            assert client.get("/health").status_code         == 200
            assert client.get("/stats/dashboard").status_code == 200
