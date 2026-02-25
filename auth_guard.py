from __future__ import annotations

from flask import jsonify, redirect, request, session, url_for


PUBLIC_PATHS = {"/login", "/api/health", "/readyz", "/metrics"}
PUBLIC_PREFIXES = ("/static/",)


def register_auth_guard(app, config):
    @app.before_request
    def require_auth():
        if not config.has_auth():
            return None

        path = request.path
        if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
            return None

        api_key = request.headers.get("X-Api-Key") or request.args.get("apikey")
        if api_key and api_key == config.API_KEY:
            return None

        if session.get("authenticated"):
            return None

        if request.path.startswith("/api/"):
            return jsonify({"error": "Unauthorized"}), 401
        return redirect(url_for("web_routes.login"))

    return require_auth
