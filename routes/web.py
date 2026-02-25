from __future__ import annotations

import os

from flask import Blueprint, current_app, jsonify, redirect, request, send_file, session, url_for


def create_blueprint(ctx):
    bp = Blueprint("web_routes", __name__)
    config = ctx["config"]

    @bp.route("/login", methods=["GET", "POST"])
    def login():
        if not config.has_auth():
            return redirect("/")

        if request.method == "GET":
            return send_file("templates/login.html")

        data = request.form if request.form else (request.json or {})
        username = data.get("username", "")
        password = data.get("password", "")

        if username == config.AUTH_USERNAME and config.verify_password(password, config.AUTH_PASSWORD):
            session["authenticated"] = True
            if request.is_json:
                return jsonify({"success": True})
            return redirect("/")

        error = "Invalid username or password"
        if request.is_json:
            return jsonify({"success": False, "error": error}), 401
        return redirect(url_for("web_routes.login", error="1"))

    @bp.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("web_routes.login"))

    @bp.route("/")
    def index():
        if os.path.exists(os.path.join(current_app.static_folder, "index.html")):
            return current_app.send_static_file("index.html")
        return send_file("templates/index.html")

    return bp
