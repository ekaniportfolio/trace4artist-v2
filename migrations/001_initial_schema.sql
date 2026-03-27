-- migrations/001_initial_schema.sql
-- Schéma initial de la base de données Trace4Artist v2
--
-- Ce fichier est exécuté automatiquement par PostgreSQL
-- au premier démarrage du conteneur Docker.
-- Pour les migrations suivantes : 002_..., 003_... etc.

-- ── Extension UUID ─────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- ── Table : artists ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS artists (
    id                  SERIAL PRIMARY KEY,
    channel_id          TEXT UNIQUE NOT NULL,
    artist_name         TEXT,
    country             TEXT,
    description         TEXT,
    subscriber_count    INTEGER DEFAULT 0,
    total_views         BIGINT  DEFAULT 0,
    video_count         INTEGER DEFAULT 0,
    email               TEXT,
    website             TEXT,
    instagram           TEXT,
    hubspot_contact_id  TEXT,              -- ID du contact dans HubSpot
    status              TEXT DEFAULT 'discovered',
    -- discovered | qualified | rejected | activated
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_artists_status     ON artists(status);
CREATE INDEX IF NOT EXISTS idx_artists_country    ON artists(country);
CREATE INDEX IF NOT EXISTS idx_artists_channel_id ON artists(channel_id);


-- ── Table : videos ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS videos (
    id              SERIAL PRIMARY KEY,
    video_id        TEXT UNIQUE NOT NULL,
    channel_id      TEXT NOT NULL REFERENCES artists(channel_id) ON DELETE CASCADE,
    title           TEXT,
    view_count      BIGINT  DEFAULT 0,
    like_count      INTEGER DEFAULT 0,
    comment_count   INTEGER DEFAULT 0,
    published_at    TIMESTAMPTZ,
    duration        TEXT,
    detected_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_videos_channel_id  ON videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_videos_published_at ON videos(published_at);


-- ── Table : view_snapshots ─────────────────────────────────────────────
-- Historique des vues pour calculer la vélocité (croissance sur 7j)
-- C'est le critère nouveau à 20% dans le scoring v2
CREATE TABLE IF NOT EXISTS view_snapshots (
    id          SERIAL PRIMARY KEY,
    video_id    TEXT NOT NULL REFERENCES videos(video_id) ON DELETE CASCADE,
    view_count  BIGINT NOT NULL,
    snapped_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshots_video_id  ON view_snapshots(video_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_snapped_at ON view_snapshots(snapped_at);


-- ── Table : scores ─────────────────────────────────────────────────────
-- Historique des scores (on garde chaque calcul, pas seulement le dernier)
CREATE TABLE IF NOT EXISTS scores (
    id                  SERIAL PRIMARY KEY,
    channel_id          TEXT NOT NULL REFERENCES artists(channel_id) ON DELETE CASCADE,
    score               NUMERIC(5,2) NOT NULL,
    segment             TEXT NOT NULL,
    -- high_potential | standard | emerging | low_priority
    criteria_breakdown  JSONB,             -- Détail de chaque critère
    calculated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scores_channel_id    ON scores(channel_id);
CREATE INDEX IF NOT EXISTS idx_scores_calculated_at ON scores(calculated_at);


-- ── Table : scan_logs ─────────────────────────────────────────────────
-- Trace chaque exécution du scheduler
CREATE TABLE IF NOT EXISTS scan_logs (
    id              SERIAL PRIMARY KEY,
    scan_type       TEXT NOT NULL,         -- full | incremental
    region          TEXT,                  -- NULL = tous les pays
    videos_found    INTEGER DEFAULT 0,
    artists_created INTEGER DEFAULT 0,
    quota_used      INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'running',-- running | completed | failed
    error_message   TEXT,
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);


-- ── Table : quota_log ─────────────────────────────────────────────────
-- Suivi précis du quota YouTube par jour et par endpoint
CREATE TABLE IF NOT EXISTS quota_log (
    id          SERIAL PRIMARY KEY,
    date        DATE NOT NULL DEFAULT CURRENT_DATE,
    endpoint    TEXT NOT NULL,
    units_used  INTEGER NOT NULL,
    logged_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quota_log_date ON quota_log(date);
