#!/bin/sh
# docker-entrypoint.sh

set -e

echo "Trace4Artist — démarrage en mode : $MODE"

case "$MODE" in
    api)
        echo "Démarrage de l'API FastAPI..."
        exec uvicorn src.api:app \
            --host 0.0.0.0 \
            --port "${PORT:-8080}" \
            --workers 2 \
            --log-level info
        ;;

    scheduler)
        echo "Démarrage du Scheduler + Health Server..."
        exec python -m src.scheduler
        ;;

    worker)
        echo "Démarrage du Worker Celery + Health Server..."
        exec python -c "from src.worker import start_worker; start_worker()"
        ;;

    *)
        echo "MODE inconnu : $MODE"
        echo "Valeurs valides : api | scheduler | worker"
        exit 1
        ;;
esac