-- migrations/006_spotify_settings.sql
-- Paramètres pour l'enrichissement Spotify

INSERT INTO settings (key, value, description) VALUES
    ('enrichment.spotify_enabled', 'true',
     'Activer l enrichissement via l API Spotify'),
    ('enrichment.spotify_min_popularity', '10',
     'Popularité Spotify minimum pour considérer un match (0-100)')
ON CONFLICT (key) DO NOTHING;
