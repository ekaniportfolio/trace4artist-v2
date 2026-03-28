-- migrations/004_tiered_tracking.sql
--
-- Paramètres configurables du Tiered Tracking via SettingsManager.
-- Tous modifiables à chaud via l'API sans redémarrage.

INSERT INTO settings (key, value, description) VALUES

    -- Fréquences de scan
    ('tracking.detection_hour',     '0',
     'Heure UTC du scan de détection quotidien (0-23)'),

    ('tracking.intensive_interval', '6',
     'Intervalle en heures entre deux scans intensifs (vidéos < 7j)'),

    -- Seuils de transition entre phases
    ('tracking.intensive_max_days', '7',
     'Âge max (jours) pour la phase intensive'),

    ('tracking.growth_max_days',    '90',
     'Âge max (jours) pour la phase de croissance'),

    ('tracking.passive_max_days',   '180',
     'Âge max (jours) pour la phase passive avant arrêt'),

    -- Règle spéciale : continuer à tracker les artistes qualifiés
    ('tracking.keep_qualified',     'true',
     'Continuer le tracking mensuel des artistes qualifiés au-delà de 180j')

ON CONFLICT (key) DO NOTHING;
