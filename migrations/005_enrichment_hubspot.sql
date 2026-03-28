-- migrations/005_enrichment_hubspot.sql
-- Colonnes pour l'enrichissement Google Search et HubSpot

-- Données enrichies par Google Search (label, Spotify, presse...)
ALTER TABLE artists
    ADD COLUMN IF NOT EXISTS enrichment_data   JSONB,
    ADD COLUMN IF NOT EXISTS hubspot_contact_id TEXT;

-- Index pour retrouver rapidement les artistes non encore
-- synchronisés avec HubSpot
CREATE INDEX IF NOT EXISTS idx_artists_hubspot_id
    ON artists(hubspot_contact_id)
    WHERE hubspot_contact_id IS NOT NULL;

-- Paramètre : activer/désactiver l'enrichissement Google
INSERT INTO settings (key, value, description) VALUES
    ('enrichment.enabled', 'true',
     'Activer l enrichissement Google Search après le scoring')
ON CONFLICT (key) DO NOTHING;
