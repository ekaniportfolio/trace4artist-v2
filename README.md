# 🎵 Trace4Artist v2

> Pipeline automatisé de prospection et qualification d'artistes musicaux africains émergents

[![CI/CD](https://github.com/ekaniportfolio/trace4artist-v2/actions/workflows/deploy.yml/badge.svg)](https://github.com/ekaniportfolio/trace4artist-v2/actions/workflows/deploy.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Redis](https://img.shields.io/badge/Redis-7-red)

---

## 🎯 Objectif

Trace4Artist automatise entièrement la chaîne de prospection commerciale pour identifier les talents musicaux africains émergents sur YouTube, les qualifier selon leur potentiel viral, et les injecter dans HubSpot CRM pour activation commerciale.

```
YouTube API          Google Search + Spotify
    ↓                        ↓
Détection des clips  Enrichissement des profils
    ↓                        ↓
         Scoring IA (0-100)
              ↓
         HubSpot CRM
              ↓
      Équipe commerciale
```

---

## ✨ Fonctionnalités

### 🔍 Détection intelligente
- Scan de **8 pays africains** (CM, NG, CI, KE, GH, ZA, CD, SN)
- Filtrage par catégorie Musique YouTube (clips officiels uniquement)
- Recherche incrémentale — ne traite que les nouvelles vidéos

### 📊 Scoring avancé (7 critères)
| Critère | Poids | Description |
|---|---|---|
| SPR — Score de Performance Relative | 20 pts | Vues / Abonnés — détecte la viralité externe |
| Indice de Viralité Organique | 20 pts | (Likes + Commentaires) / Vues |
| Vélocité 24h | 20 pts | Croissance des vues sur 24h |
| Vélocité 7 jours | 15 pts | Tendance sur la semaine |
| Régularité des publications | 10 pts | Fréquence de publication |
| Qualité de chaîne | 10 pts | Taille de l'audience établie |
| Présence web + contact | 5 pts | Email, Instagram, site officiel |

### 🎯 Tiered Tracking (économie de quota)
```
Phase intensive  < 7 jours   → snapshot toutes les 6h
Phase croissance 7-90 jours  → snapshot hebdomadaire
Phase passive    90-180 jours → snapshot mensuel
Phase arrêtée    > 180 jours  → fin du tracking
```

### 🛡️ Détection de fraude
- Fake View Detector : engagement < 0.1% sur +100K vues → malus -20pts
- Breakout Detector : croissance > 20% en 24h → alerte immédiate

### 🔗 Intégrations
- **YouTube Data API v3** — détection et métriques
- **Spotify API** — label, popularité, confirmation d'identité
- **Google Custom Search** — presse, contacts, présence web
- **HubSpot CRM** — synchronisation avec 10 propriétés custom

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────┐
│                  APScheduler                        │
│  Job 1: Détection (quotidien)                       │
│  Job 2: Monitoring intensif (toutes les 6h)         │
│  Job 3: Monitoring croissance (hebdo)               │
│  Job 4: Monitoring passif (mensuel)                 │
└──────────────────┬──────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────┐
│              Celery + Redis                         │
│         Queue de tâches asynchrones                 │
└──────────────────┬──────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────┐
│              PostgreSQL                             │
│  artists · videos · scores · view_snapshots         │
│  scan_logs · quota_log · settings · video_alerts   │
└──────────────────┬──────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────┐
│           FastAPI — API interne                     │
│  /artists · /scan · /settings · /stats · /alerts   │
└──────────────────┬──────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────┐
│           Dashboard React (futur)                   │
└─────────────────────────────────────────────────────┘
```

---

## 🚀 Stack technique

| Composant | Technologie |
|---|---|
| Backend | Python 3.11 |
| API | FastAPI + Uvicorn |
| Base de données | PostgreSQL 16 (Supabase) |
| Cache & Queue | Redis 7 + Celery |
| Scheduler | APScheduler |
| CRM | HubSpot API |
| Musique | Spotify API |
| Recherche | Google Custom Search |
| Déploiement | Google Cloud Run |
| CI/CD | GitHub Actions |

---

## 📁 Structure du projet

```
trace4artist-v2/
├── src/
│   ├── api.py            # API FastAPI (tous les endpoints)
│   ├── scheduler.py      # Tiered Tracking Scheduler (4 jobs)
│   ├── worker.py         # Tâches Celery asynchrones
│   ├── youtube_client.py # Client YouTube + cache Redis
│   ├── searcher.py       # Recherche et parsing YouTube
│   ├── scorer.py         # Scoring 7 critères + détection fraude
│   ├── enricher.py       # Google Search + Spotify
│   ├── hubspot_client.py # Sync HubSpot CRM
│   ├── phase_manager.py  # Gestion phases de tracking
│   ├── settings_manager.py # Paramètres dynamiques (hot-reload)
│   └── database.py       # Connexions PostgreSQL (SQLAlchemy)
├── migrations/
│   ├── 001_initial_schema.sql
│   ├── 002_settings.sql
│   ├── 003_enriched_snapshots.sql
│   ├── 004_tiered_tracking.sql
│   ├── 005_enrichment_hubspot.sql
│   └── 006_spotify_settings.sql
├── tests/                # 109 tests unitaires
├── Dockerfile
├── docker-compose.yml    # Dev local (PostgreSQL + Redis)
└── .github/workflows/    # CI/CD GitHub Actions
```

---

## 🛠️ Installation locale

### Prérequis
- Python 3.11+
- Docker Desktop
- Git

### Démarrage rapide

```bash
# 1. Cloner le repo
git clone https://github.com/ekaniportfolio/trace4artist-v2.git
cd trace4artist-v2

# 2. Configurer les variables d'environnement
cp .env.example .env
# Remplir les clés API dans .env

# 3. Lancer PostgreSQL + Redis
docker-compose up -d

# 4. Installer les dépendances Python
pip install -r requirements.txt

# 5. Vérifier l'installation
python main.py
```

### Lancer les services

```bash
# API FastAPI (http://localhost:8000/docs)
uvicorn src.api:app --reload --port 8000

# Scheduler (dans un second terminal)
python -m src.scheduler

# Celery Worker (dans un troisième terminal)
celery -A src.worker worker --loglevel=info
```

### Lancer les tests

```bash
pytest
# 109 tests — tous verts ✅
```

---

## 🌐 API — Endpoints principaux

| Méthode | Endpoint | Description |
|---|---|---|
| GET | `/artists` | Liste des artistes avec filtres |
| GET | `/artists/top` | Top artistes qualifiés |
| GET | `/artists/{id}` | Détail d'un artiste |
| GET | `/scan/status` | État du dernier scan |
| POST | `/scan/trigger` | Déclencher un scan manuel |
| GET | `/settings` | Paramètres du système |
| PATCH | `/settings/{key}` | Modifier un paramètre à chaud |
| GET | `/stats/dashboard` | KPIs pour le dashboard |
| GET | `/stats/quota` | Historique consommation quota |
| GET | `/alerts` | Breakouts et anomalies |
| GET | `/health` | Santé de l'API |

Documentation interactive : `https://ton-api.run.app/docs`

---

## ⚙️ Paramètres configurables à chaud

Tous ces paramètres sont modifiables via `PATCH /settings/{key}` sans redémarrage :

| Paramètre | Défaut | Description |
|---|---|---|
| `scan.lookback_days` | 365 | Période du premier scan |
| `scan.interval_hours` | 6 | Fréquence des scans |
| `scan.regions` | CM,NG,CI,... | Pays cibles |
| `scan.max_results` | 50 | Résultats par recherche |
| `scan.keywords` | "official video"... | Mots-clés YouTube |
| `tracking.intensive_max_days` | 7 | Durée phase intensive |
| `tracking.growth_max_days` | 90 | Durée phase croissance |
| `tracking.breakout_threshold` | 0.20 | Seuil alerte breakout |

---

## 🚢 Déploiement

Le projet est déployé sur **Google Cloud Run** via GitHub Actions.

Chaque push sur `master` déclenche automatiquement :
1. ✅ Les 109 tests unitaires
2. 🐳 Build de l'image Docker
3. 📦 Push sur Artifact Registry
4. 🚀 Déploiement des 3 services Cloud Run

```
trace4artist-api        → API FastAPI
trace4artist-scheduler  → Scheduler APScheduler
trace4artist-worker     → Celery Workers
```

---

## 📄 Licence

Projet propriétaire — Trace4Artist © 2025
