-- migrations/003_enriched_snapshots.sql
--
-- 1. Enrichit view_snapshots avec les métriques au moment du snapshot
--    (nécessaire pour le scoring historique précis)
-- 2. Ajoute video_alerts pour tracker les breakouts détectés
-- 3. Ajoute des colonnes de tracking phase sur videos

-- ── 1. Enrichissement de view_snapshots ───────────────────────────────
-- On ajoute les colonnes manquantes avec DEFAULT 0
-- pour ne pas casser les snapshots existants
ALTER TABLE view_snapshots
    ADD COLUMN IF NOT EXISTS like_count            INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS comment_count         INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS subscriber_count      INTEGER DEFAULT 0;

-- Index pour accélérer les calculs de vélocité
-- (requêtes fréquentes : derniers N snapshots d'une vidéo)
CREATE INDEX IF NOT EXISTS idx_snapshots_video_snapped
    ON view_snapshots(video_id, snapped_at DESC);


-- ── 2. Table video_alerts — breakouts et anomalies ────────────────────
CREATE TABLE IF NOT EXISTS video_alerts (
    id              SERIAL PRIMARY KEY,
    video_id        TEXT NOT NULL REFERENCES videos(video_id) ON DELETE CASCADE,
    channel_id      TEXT NOT NULL,
    alert_type      TEXT NOT NULL,
    -- 'breakout'        : croissance > 20% en 24h sur vidéo > 7j
    -- 'fake_views'      : engagement anormalement bas
    -- 'phase_change'    : changement de phase de tracking (Option B)
    details         JSONB,             -- Données contextuelles de l'alerte
    is_processed    BOOLEAN DEFAULT FALSE,
    detected_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_video_id   ON video_alerts(video_id);
CREATE INDEX IF NOT EXISTS idx_alerts_type       ON video_alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_processed  ON video_alerts(is_processed);


-- ── 3. Phase de tracking sur videos ───────────────────────────────────
ALTER TABLE videos
    ADD COLUMN IF NOT EXISTS tracking_phase     TEXT DEFAULT 'intensive',
    -- 'intensive'  : < 7 jours  → snapshot toutes les 6h
    -- 'growth'     : 7-90 jours → snapshot hebdomadaire
    -- 'passive'    : > 90 jours → snapshot mensuel
    -- 'stopped'    : > 180 jours et non qualifié → plus de tracking
    ADD COLUMN IF NOT EXISTS phase_changed_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS consecutive_growth_days INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS is_suspicious      BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_videos_tracking_phase
    ON videos(tracking_phase);


-- ── 4. Nouveaux paramètres de settings ────────────────────────────────
INSERT INTO settings (key, value, description) VALUES
    ('tracking.intensive_hours',  '6',
     'Fréquence de snapshot pour vidéos < 7 jours (en heures)'),
    ('tracking.intensive_days',   '7',
     'Durée de la phase intensive (en jours)'),
    ('tracking.growth_days',      '90',
     'Durée de la phase de croissance (en jours)'),
    ('tracking.passive_days',     '180',
     'Durée de la phase passive avant arrêt (en jours)'),
    ('tracking.breakout_threshold', '0.20',
     'Seuil de croissance 24h pour déclencher une alerte breakout (0.20 = 20%)')
ON CONFLICT (key) DO NOTHING;
