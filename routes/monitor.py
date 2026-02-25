from __future__ import annotations

import time as _time

from flask import Blueprint, jsonify


def create_blueprint(ctx):
    bp = Blueprint("monitor_routes", __name__)

    @bp.route("/api/monitor/status")
    def api_monitor_status():
        monitor = ctx["get_monitor"]()
        if monitor is None:
            return jsonify({"enabled": False, "error": "Monitor not initialized"})
        return jsonify(monitor.get_status())

    @bp.route("/api/monitor/analyze", methods=["POST"])
    def api_monitor_analyze():
        monitor = ctx["get_monitor"]()
        if monitor is None:
            return jsonify({"success": False, "error": "Monitor not initialized"})
        monitor.trigger_manual()
        _time.sleep(0.1)
        return jsonify({"success": True, **monitor.get_status()})

    @bp.route("/api/monitor/actions/<action_id>/approve", methods=["POST"])
    def api_monitor_approve(action_id):
        monitor = ctx["get_monitor"]()
        if monitor is None:
            return jsonify({"success": False, "error": "Monitor not initialized"})
        success, message = monitor.execute_approved(action_id)
        return jsonify({"success": success, "message": message})

    @bp.route("/api/monitor/actions/<action_id>/dismiss", methods=["POST"])
    def api_monitor_dismiss(action_id):
        monitor = ctx["get_monitor"]()
        if monitor is None:
            return jsonify({"success": False, "error": "Monitor not initialized"})
        dismissed = monitor.action_queue.dismiss(action_id)
        return jsonify({"success": dismissed})

    return bp
