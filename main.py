"""
main.py — Point d'entrée Trace4Artist v2
"""

from config import validate_config
from src.database import check_connection


def main():
    print("=" * 55)
    print("  🎵 Trace4Artist v2 — African Music Scanner")
    print("=" * 55)

    # Validation de la config
    validate_config()

    # Vérification de la connexion à la base
    print("\n🔌 Connexion à PostgreSQL...", end=" ")
    if not check_connection():
        print("\n💡 Lance d'abord : docker-compose up -d")
        return
    print("✅")

    print("\n✅ Système prêt.")
    print("   → Lance le scheduler : python -m src.scheduler")
    print("   → Lance l'API        : python -m src.api")
    print("   → Lance les workers  : celery -A src.worker worker")


if __name__ == "__main__":
    main()
