"""Route locali che in produzione risolve nginx (root = viewer/)."""
from __future__ import annotations

import os
import threading
import urllib.error
import urllib.request

import server


def test_local_server_serves_log_page_stylesheet_and_api_auth() -> None:
    previous = os.getcwd()
    os.chdir(server._APP_DIR)
    handler = server._make_handler("test-token", server._APP_DIR / "data", {}, [])
    httpd = server.http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    try:
        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None

        opener = urllib.request.build_opener(_NoRedirect)
        try:
            opener.open(base + "/")
            raise AssertionError("GET / deve reindirizzare al viewer")
        except urllib.error.HTTPError as exc:
            assert exc.code == 302
            assert exc.headers.get("Location") == "/viewer/index.html"

        head = urllib.request.Request(base + "/", method="HEAD")
        try:
            opener.open(head)
            raise AssertionError("HEAD / non deve elencare la root del repository")
        except urllib.error.HTTPError as exc:
            assert exc.code == 302

        with urllib.request.urlopen(base + "/log") as response:
            body = response.read().decode("utf-8")
            assert response.status == 200
            assert "Stato Log" in body
            assert 'href="/style.css"' in body

        with urllib.request.urlopen(base + "/style.css") as response:
            css = response.read().decode("utf-8")
            assert response.status == 200
            assert "--" in css or "{" in css

        with urllib.request.urlopen(base + "/assets/trader-icon.svg") as response:
            icon = response.read()
            assert response.status == 200
            assert b"<svg" in icon

        request = urllib.request.Request(base + "/api/db/products")
        try:
            urllib.request.urlopen(request)
            raise AssertionError("la API senza bearer deve rispondere 401")
        except urllib.error.HTTPError as exc:
            assert exc.code == 401

        request = urllib.request.Request(
            base + "/api/logs/status",
            headers={"Authorization": "Bearer test-token"},
        )
        with urllib.request.urlopen(request) as response:
            assert response.status == 200
    finally:
        httpd.shutdown()
        httpd.server_close()
        os.chdir(previous)
