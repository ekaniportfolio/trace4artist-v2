# Dockerfile — Trace4Artist v2
#
# Un seul image, trois modes de démarrage :
#   MODE=api        → uvicorn (API FastAPI)
#   MODE=scheduler  → APScheduler (détection + orchestration)
#   MODE=worker     → Celery worker (traitement des tâches)
#
# Build : docker build -t trace4artist .
# Run   : docker run -e MODE=api -p 8000:8000 trace4artist

# ── Image de base ──────────────────────────────────────────────────────
# python:3.11-slim : Python 3.11 sur Debian minimal
# On utilise 3.11 en production (plus stable que 3.14 pour les dépendances)
FROM python:3.11-slim

# ── Variables d'environnement ──────────────────────────────────────────
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1        \
    PORT=8080                 \
    MODE=api

# PYTHONDONTWRITEBYTECODE : pas de fichiers .pyc
# PYTHONUNBUFFERED        : logs visibles immédiatement dans Cloud Run

# ── Dépendances système ────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
# gcc et libpq-dev : nécessaires pour compiler psycopg2

# ── Répertoire de travail ──────────────────────────────────────────────
WORKDIR /app

# ── Dépendances Python ─────────────────────────────────────────────────
# On copie d'abord requirements.txt seul pour profiter du cache Docker :
# si le code change mais pas les dépendances, cette couche n'est pas reconstruite
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Code source ────────────────────────────────────────────────────────
COPY . .

# ── Script de démarrage ────────────────────────────────────────────────
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# ── Port exposé ────────────────────────────────────────────────────────
EXPOSE $PORT

# ── Point d'entrée ─────────────────────────────────────────────────────
ENTRYPOINT ["/docker-entrypoint.sh"]