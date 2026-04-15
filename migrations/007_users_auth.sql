-- migrations/007_users_auth.sql
-- Table des utilisateurs + authentification JWT

CREATE TABLE IF NOT EXISTS users (
    id           SERIAL PRIMARY KEY,
    username     TEXT NOT NULL UNIQUE,
    email        TEXT NOT NULL UNIQUE,
    full_name    TEXT NOT NULL,
    role         TEXT NOT NULL DEFAULT 'technician',
    -- Valeurs : 'admin' | 'manager' | 'technician'
    password_hash TEXT NOT NULL,
    is_active    BOOLEAN DEFAULT TRUE,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    last_login   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_users_email    ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_role     ON users(role);

-- Paramètres JWT dans la table settings
INSERT INTO settings (key, value, description) VALUES
    ('auth.jwt_expire_hours', '24',
     'Durée de vie du token JWT en heures'),
    ('auth.default_admin_email', 'admin@trace4artist.com',
     'Email du compte admin créé au premier démarrage'),
    ('auth.default_admin_username', 'admin',
     'Username du compte admin créé au premier démarrage')
ON CONFLICT (key) DO NOTHING;
