"""
src/health_server.py — Serveur HTTP minimal pour le health check Cloud Run

Le scheduler et le worker ne sont pas des serveurs HTTP.
Mais Cloud Run vérifie que le conteneur répond sur le port défini.
Ce mini-serveur répond uniquement sur /health pour satisfaire ce check.

Il tourne dans un thread séparé pour ne pas bloquer le scheduler/worker.
"""

import threading
from http.server import HTTPServer, BaseHTTPRequestHandler


class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass   # Silence les logs HTTP inutiles


def start_health_server(port: int = 8080):
    """
    Démarre le serveur health check dans un thread daemon.
    Thread daemon = il s'arrête automatiquement quand le process principal s'arrête.
    """
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
