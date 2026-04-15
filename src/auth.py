"""
src/auth.py — Authentification JWT + gestion des utilisateurs

Responsabilités :
    1. Hachage et vérification des mots de passe (bcrypt)
    2. Création et vérification des tokens JWT
    3. Dépendances FastAPI pour protéger les routes
    4. Initialisation du compte admin au premier démarrage

Sécurité :
    - Mots de passe hachés avec bcrypt (cost factor 12)
    - JWT signé avec SECRET_KEY (HMAC-SHA256)
    - Tokens valides 24h (configurable via settings)
    - SECRET_KEY doit être dans les secrets GCP en production
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from sqlalchemy import text

from src.database import get_db

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────
SECRET_KEY         = os.getenv("JWT_SECRET_KEY", "trace4artist-dev-secret-change-in-prod")
ALGORITHM          = "HS256"
TOKEN_EXPIRE_HOURS = 24

# Schéma OAuth2 — extrait le token du header Authorization: Bearer <token>
oauth2_scheme   = OAuth2PasswordBearer(tokenUrl="/auth/login")
oauth2_optional = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


# ── Modèles Pydantic ────────────────────────────────────────────────────

class TokenData(BaseModel):
    user_id : int
    username: str
    role    : str


class UserInDB(BaseModel):
    id           : int
    username     : str
    email        : str
    full_name    : str
    role         : str
    is_active    : bool
    created_at   : datetime
    last_login   : Optional[datetime] = None


# ── Utilitaires mots de passe ───────────────────────────────────────────
# Compatibilité Python 3.14 : passlib[bcrypt] lève ValueError sur bytes.
# On utilise bcrypt directement si disponible, sinon fallback SHA256 (dev).

def hash_password(password: str) -> str:
    """
    Hache un mot de passe avec bcrypt (72 bytes max).
    Utilise le module bcrypt directement pour éviter les incompatibilités
    de passlib avec Python 3.12+.
    """
    pw_bytes = password.encode("utf-8")[:72]
    try:
        import bcrypt
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(pw_bytes, salt).decode("utf-8")
    except ImportError:
        pass
    # Fallback dev : SHA256 (jamais en production)
    import hashlib, secrets as _secrets
    salt   = _secrets.token_hex(16)
    hashed = hashlib.sha256(pw_bytes + salt.encode()).hexdigest()
    return f"sha256${salt}${hashed}"


def verify_password(plain: str, hashed: str) -> bool:
    """
    Vérifie un mot de passe contre son hash.
    Compatible avec bcrypt direct et le fallback SHA256.
    """
    pw_bytes = plain.encode("utf-8")[:72]
    if hashed.startswith("sha256$"):
        import hashlib
        _, salt, stored = hashed.split("$")
        return hashlib.sha256(pw_bytes + salt.encode()).hexdigest() == stored
    try:
        import bcrypt
        return bcrypt.checkpw(pw_bytes, hashed.encode("utf-8"))
    except ImportError:
        return False


# ── Utilitaires JWT ────────────────────────────────────────────────────

def create_access_token(user_id: int, username: str, role: str) -> str:
    """
    Crée un token JWT signé avec les informations de l'utilisateur.
    Expire après TOKEN_EXPIRE_HOURS heures.
    """
    expire = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)
    payload = {
        "sub"     : str(user_id),
        "username": username,
        "role"    : role,
        "exp"     : expire,
        "iat"     : datetime.now(timezone.utc),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> TokenData:
    """
    Décode et vérifie un token JWT.
    Lève HTTPException 401 si invalide ou expiré.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id  = int(payload.get("sub", 0))
        username = payload.get("username", "")
        role     = payload.get("role", "")
        if not user_id or not username:
            raise ValueError("Token invalide")
        return TokenData(user_id=user_id, username=username, role=role)
    except JWTError as e:
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED,
            detail      = "Token invalide ou expiré",
            headers     = {"WWW-Authenticate": "Bearer"},
        ) from e


# ── Dépendances FastAPI ────────────────────────────────────────────────

def get_current_user(token: str = Depends(oauth2_scheme)) -> TokenData:
    """
    Dépendance FastAPI — extrait et vérifie le token JWT.
    Met à jour last_login de l'utilisateur.
    Usage : route_handler(user: TokenData = Depends(get_current_user))
    """
    token_data = decode_token(token)

    # Vérifier que l'utilisateur existe et est actif
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT id, is_active FROM users WHERE id = :id
        """), {"id": token_data.user_id})
        row = result.fetchone()

    if not row or not row[1]:
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED,
            detail      = "Utilisateur inactif ou supprimé",
        )
    return token_data


def require_admin(user: TokenData = Depends(get_current_user)) -> TokenData:
    """Dépendance — route accessible uniquement aux admins."""
    if user.role != "admin":
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN,
            detail      = "Accès réservé aux administrateurs",
        )
    return user


def require_admin_or_technician(
    user: TokenData = Depends(get_current_user)
) -> TokenData:
    """Dépendance — route accessible aux admins et techniciens."""
    if user.role not in ("admin", "technician"):
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN,
            detail      = "Accès réservé aux administrateurs et techniciens",
        )
    return user


def require_admin_or_manager(
    user: TokenData = Depends(get_current_user)
) -> TokenData:
    """Dépendance — route accessible aux admins et managers."""
    if user.role not in ("admin", "manager"):
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN,
            detail      = "Accès réservé aux administrateurs et managers",
        )
    return user


# ── CRUD utilisateurs ──────────────────────────────────────────────────

def get_user_by_email(email: str) -> Optional[dict]:
    """Récupère un utilisateur par email (pour le login)."""
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT id, username, email, full_name, role,
                   password_hash, is_active, created_at, last_login
            FROM users WHERE email = :email
        """), {"email": email})
        row = result.fetchone()
        return dict(row._mapping) if row else None


def get_user_by_id(user_id: int) -> Optional[dict]:
    """Récupère un utilisateur par ID."""
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT id, username, email, full_name, role,
                   is_active, created_at, last_login
            FROM users WHERE id = :id
        """), {"id": user_id})
        row = result.fetchone()
        return dict(row._mapping) if row else None


def get_all_users() -> list[dict]:
    """Liste tous les utilisateurs (sans les mots de passe)."""
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT id, username, email, full_name, role,
                   is_active, created_at, last_login
            FROM users
            ORDER BY created_at DESC
        """))
        return [dict(row._mapping) for row in result.fetchall()]


def create_user(
    username : str,
    email    : str,
    full_name: str,
    password : str,
    role     : str = "technician",
) -> dict:
    """
    Crée un nouvel utilisateur.
    Lève ValueError si email ou username déjà pris.
    """
    if role not in ("admin", "manager", "technician"):
        raise ValueError(f"Rôle invalide : {role}")

    password_hash = hash_password(password)
    with get_db() as conn:
        try:
            result = conn.execute(text("""
                INSERT INTO users
                    (username, email, full_name, role, password_hash)
                VALUES
                    (:username, :email, :full_name, :role, :password_hash)
                RETURNING id, username, email, full_name, role,
                          is_active, created_at
            """), {
                "username"     : username,
                "email"        : email,
                "full_name"    : full_name,
                "role"         : role,
                "password_hash": password_hash,
            })
            return dict(result.fetchone()._mapping)
        except Exception as e:
            if "unique" in str(e).lower():
                raise ValueError("Email ou username déjà utilisé") from e
            raise


def update_user(
    user_id  : int,
    full_name: Optional[str] = None,
    role     : Optional[str] = None,
    is_active: Optional[bool] = None,
    password : Optional[str] = None,
) -> Optional[dict]:
    """Met à jour un utilisateur. Retourne None si introuvable."""
    updates = {"updated_at": "NOW()"}
    params  = {"user_id": user_id}

    if full_name is not None:
        updates["full_name"] = ":full_name"
        params["full_name"]  = full_name
    if role is not None:
        if role not in ("admin", "manager", "technician"):
            raise ValueError(f"Rôle invalide : {role}")
        updates["role"]  = ":role"
        params["role"]   = role
    if is_active is not None:
        updates["is_active"] = ":is_active"
        params["is_active"]  = is_active
    if password is not None:
        updates["password_hash"] = ":password_hash"
        params["password_hash"]  = hash_password(password)

    if len(updates) == 1:  # Seulement updated_at
        return get_user_by_id(user_id)

    set_clause = ", ".join(
        f"{k} = {v}" if v == "NOW()" else f"{k} = {v}"
        for k, v in updates.items()
    )
    with get_db() as conn:
        result = conn.execute(text(f"""
            UPDATE users SET {set_clause}, updated_at = NOW()
            WHERE id = :user_id
            RETURNING id, username, email, full_name, role,
                      is_active, created_at, last_login
        """), params)
        row = result.fetchone()
        return dict(row._mapping) if row else None


def delete_user(user_id: int) -> bool:
    """Supprime un utilisateur. Retourne False si introuvable."""
    with get_db() as conn:
        result = conn.execute(text("""
            DELETE FROM users WHERE id = :id RETURNING id
        """), {"id": user_id})
        return result.fetchone() is not None


def update_last_login(user_id: int):
    """Met à jour la date de dernière connexion."""
    with get_db() as conn:
        conn.execute(text("""
            UPDATE users SET last_login = NOW() WHERE id = :id
        """), {"id": user_id})


# ── Initialisation admin au 1er démarrage ──────────────────────────────

def init_default_admin():
    """
    Crée le compte admin par défaut si aucun utilisateur n'existe.
    Appelé au démarrage de l'API et du scheduler.

    Credentials par défaut :
        email    : admin@trace4artist.com
        password : Admin@T4A2025!
        → À changer immédiatement après le premier login
    """
    with get_db() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
        if count and count > 0:
            return  # Des utilisateurs existent déjà

    default_password = os.getenv("DEFAULT_ADMIN_PASSWORD", "Admin@T4A2025!")
    default_email    = os.getenv("DEFAULT_ADMIN_EMAIL", "admin@trace4artist.com")

    try:
        create_user(
            username  = "admin",
            email     = default_email,
            full_name = "Administrateur",
            password  = default_password,
            role      = "admin",
        )
        logger.info(
            f"Compte admin créé : {default_email} / {default_password}\n"
            "⚠️  Changez ce mot de passe immédiatement après le premier login !"
        )
    except ValueError:
        pass  # Admin déjà créé (race condition)