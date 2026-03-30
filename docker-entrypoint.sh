#!/bin/sh
# docker-entrypoint.sh — Détermine ce qui démarre selon MODE

set -e

echo "Trace4Artist — démarrage en mode : $MODE"

case "$MODE" in
    api)
        echo "Démarrage de l'API FastAPI..."
        exec uvicorn src.api:app \
            --host 0.0.0.0 \
            --port "${PORT:-8000}" \
            --workers 2 \
            --log-level info
        ;;

    scheduler)
        echo "Démarrage du Scheduler APScheduler..."
        exec python -m src.scheduler
        ;;

    worker)
        echo "Démarrage du Celery Worker..."
        exec celery -A src.worker worker \
            --loglevel=info \
            --concurrency=4 \
            --queues=celery
        ;;

    *)
        echo "MODE inconnu : $MODE"
        echo "Valeurs valides : api | scheduler | worker"
        exit 1
        ;;
esac
