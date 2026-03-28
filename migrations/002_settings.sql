-- migrations/002_settings.sql
-- Table de configuration dynamique
-- Permet de modifier les paramètres de scan à chaud via l'API
-- sans redémarrer le système.
--
-- Chaque paramètre a :
--   - une clé unique
--   - une valeur stockée en TEXT (castée au bon type par SettingsManager)
--   - une description lisible par les humains
--   - une date de dernière modification

CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    description TEXT,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Valeurs par défaut — identiques à config.py
-- Modifiables via API sans redémarrage
INSERT INTO settings (key, value, description) VALUES
    ('scan.lookback_days',   '365',
     'Période de lookback du premier scan (en jours)'),
    ('scan.interval_hours',  '6',
     'Intervalle entre deux scans automatiques (en heures)'),
    ('scan.regions',         'NG,CI,KE,GH,ZA,CD,SN,CM',
     'Pays cibles séparés par des virgules (codes ISO)'),
    ('scan.max_results',     '50',
     'Nombre maximum de résultats par recherche YouTube'),
    ('scan.keywords',        '"official video" OR "clip officiel" OR "music video"',
     'Mots-clés YouTube pour filtrer les clips musicaux')
ON CONFLICT (key) DO NOTHING;
-- ON CONFLICT DO NOTHING : ne pas écraser les valeurs
-- déjà modifiées par l'utilisateur si on relance la migration