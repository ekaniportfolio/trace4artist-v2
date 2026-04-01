"""
src/health_server.py — Serveur HTTP minimal pour le health check Cloud Run

Le scheduler et le worker ne sont pas des serveurs HTTP.
Mais Cloud Run vérifie que le conteneur répond sur le port défini.
Ce mini-serveur répond uniquement sur /health pour satisfaire ce check.

Il tourne dans un thread séparé pour ne pas bloquer le scheduler/worker.

Un thread de keepalive envoie une requête /health toutes les 30s
pour signaler à Cloud Run que le conteneur est actif.
"""
 
import threading
import time
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
 
logger = logging.getLogger(__name__)
 
 
class HealthHandler(BaseHTTPRequestHandler):
 
    def do_GET(self):
        if self.path in ("/health", "/"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()
 
    def log_message(self, format, *args):
        pass   # Silence les logs HTTP
 
 
def _keepalive_loop(port: int, interval: int = 30):
    """
    Envoie une requête GET /health toutes les N secondes.
    Prouve à Cloud Run que le conteneur est actif
    même sans trafic entrant.
    """
    import urllib.request
    time.sleep(10)  # Attendre que le serveur soit prêt
 
    while True:
        try:
            urllib.request.urlopen(
                f"http://localhost:{port}/health",
                timeout=5,
            )
        except Exception:
            pass  # Silencieux — le serveur redémarre peut-être
        time.sleep(interval)
 
 
def start_health_server(port: int = 8080):
    """
    Démarre le serveur health check + le thread keepalive.
    Les deux sont des threads daemon — ils s'arrêtent
    automatiquement quand le process principal s'arrête.
    """
    # Serveur HTTP
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server_thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )
    server_thread.start()
 
    # Keepalive
    keepalive_thread = threading.Thread(
        target=_keepalive_loop,
        args=(port,),
        daemon=True,
    )
    keepalive_thread.start()
 
    logger.info(f"Health server démarré sur port {port} (keepalive: 30s)")
    return server
 