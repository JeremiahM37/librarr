from __future__ import annotations

import json

from flask import Blueprint, jsonify, request


def create_blueprint(ctx):
    bp = Blueprint("settings_routes", __name__)
    config = ctx["config"]
    qb = ctx["qb"]
    logger = ctx["logger"]

    @bp.route("/api/settings")
    def api_get_settings():
        return jsonify(config.get_all_settings())

    @bp.route("/api/settings", methods=["POST"])
    def api_save_settings():
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400
        try:
            data = dict(data)
            if "target_routing_rules" in data:
                raw_rules = (data.get("target_routing_rules") or "").strip() or "{}"
                try:
                    parsed = json.loads(raw_rules)
                    if not isinstance(parsed, dict):
                        return jsonify({"success": False, "error": "target_routing_rules must be a JSON object"}), 400
                    data["target_routing_rules"] = json.dumps(parsed)
                except Exception as e:
                    return jsonify({"success": False, "error": f"Invalid target_routing_rules JSON: {e}"}), 400
            masked = getattr(config, "MASKED_SECRET", "••••••••")
            for key in ("prowlarr_api_key", "qb_pass", "abs_token", "kavita_api_key", "api_key"):
                if data.get(key) == masked:
                    del data[key]
            if "auth_password" in data:
                pw = data["auth_password"]
                if pw and pw != masked:
                    data["auth_password"] = config.hash_password(pw)
                else:
                    del data["auth_password"]
            config.save_settings(data)
            ctx["flask_app"].secret_key = config.SECRET_KEY
            qb.authenticated = False
            return jsonify({"success": True})
        except Exception as e:
            logger.error("Failed to save settings: %s", e)
            return jsonify({"success": False, "error": str(e)}), 500

    @bp.route("/api/validate/config")
    def api_validate_config():
        include_network = request.args.get("network", "0").lower() in ("1", "true", "yes")
        return jsonify(ctx["runtime_config_validation"](run_network_tests=include_network))

    @bp.route("/api/test/all", methods=["POST"])
    def api_test_all():
        return jsonify(ctx["runtime_config_validation"](run_network_tests=True))

    @bp.route("/api/test/prowlarr", methods=["POST"])
    def api_test_prowlarr():
        data = request.json or {}
        url = data.get("url", "").rstrip("/")
        api_key = data.get("api_key", "")
        return jsonify(ctx["test_prowlarr_connection"](url, api_key))

    @bp.route("/api/test/qbittorrent", methods=["POST"])
    def api_test_qbittorrent():
        data = request.json or {}
        url = data.get("url", "").rstrip("/")
        user = data.get("user", "admin")
        password = data.get("pass", "")
        return jsonify(ctx["test_qbittorrent_connection"](url, user, password))

    @bp.route("/api/test/audiobookshelf", methods=["POST"])
    def api_test_audiobookshelf():
        data = request.json or {}
        url = data.get("url", "").rstrip("/")
        token = data.get("token", "")
        return jsonify(ctx["test_audiobookshelf_connection"](url, token))

    @bp.route("/api/test/kavita", methods=["POST"])
    def api_test_kavita():
        data = request.json or {}
        url = data.get("url", "").rstrip("/")
        api_key = data.get("api_key", "")
        return jsonify(ctx["test_kavita_connection"](url, api_key))

    return bp
