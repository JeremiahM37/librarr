"""Connectivity and runtime diagnostics helpers."""
from __future__ import annotations

import json
import os

import requests


def path_check(name, path, create=False):
    check = {"name": name, "path": path or "", "exists": False, "writable": False, "ok": False}
    if not path:
        check["error"] = "not configured"
        return check
    if os.path.exists(path):
        check["exists"] = True
        check["writable"] = os.access(path, os.W_OK)
        check["ok"] = check["writable"]
        if not check["ok"]:
            check["error"] = "path not writable"
        return check
    if create:
        try:
            os.makedirs(path, exist_ok=True)
            check["exists"] = True
            check["writable"] = os.access(path, os.W_OK)
            check["ok"] = check["writable"]
            if not check["ok"]:
                check["error"] = "created but not writable"
            return check
        except Exception as e:
            check["error"] = str(e)
            return check
    check["error"] = "path does not exist"
    return check


def test_prowlarr_connection(url, api_key, requests_module=requests):
    if not url or not api_key:
        return {"success": False, "error": "URL and API key required", "error_class": "missing_config"}
    try:
        resp = requests_module.get(f"{url.rstrip('/')}/api/v1/indexer", headers={"X-Api-Key": api_key}, timeout=10)
        if resp.status_code == 200:
            indexers = resp.json()
            return {"success": True, "message": f"Connected ({len(indexers)} indexers)", "indexer_count": len(indexers)}
        if resp.status_code == 401:
            return {"success": False, "error": "Invalid API key", "error_class": "auth_failed"}
        return {"success": False, "error": f"HTTP {resp.status_code}", "error_class": f"http_{resp.status_code}"}
    except requests_module.Timeout:
        return {"success": False, "error": "Timed out connecting to Prowlarr", "error_class": "timeout"}
    except requests_module.ConnectionError:
        return {"success": False, "error": "Connection refused — is Prowlarr running?", "error_class": "unreachable"}
    except Exception as e:
        return {"success": False, "error": str(e), "error_class": "request_error"}


def test_audiobookshelf_connection(url, token, requests_module=requests):
    if not url or not token:
        return {"success": False, "error": "URL and API token required", "error_class": "missing_config"}
    try:
        resp = requests_module.get(f"{url.rstrip('/')}/api/libraries", headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if resp.status_code == 200:
            libraries = resp.json().get("libraries", [])
            return {
                "success": True,
                "message": f"Connected ({len(libraries)} libraries)",
                "libraries": [{"id": l["id"], "name": l["name"]} for l in libraries],
            }
        if resp.status_code == 401:
            return {"success": False, "error": "Invalid API token", "error_class": "auth_failed"}
        return {"success": False, "error": f"HTTP {resp.status_code}", "error_class": f"http_{resp.status_code}"}
    except requests_module.Timeout:
        return {"success": False, "error": "Timed out connecting to Audiobookshelf", "error_class": "timeout"}
    except requests_module.ConnectionError:
        return {"success": False, "error": "Connection refused — is Audiobookshelf running?", "error_class": "unreachable"}
    except Exception as e:
        return {"success": False, "error": str(e), "error_class": "request_error"}


def test_kavita_connection(url, api_key, requests_module=requests):
    if not url or not api_key:
        return {"success": False, "error": "URL and API key required", "error_class": "missing_config"}
    try:
        resp = requests_module.post(
            f"{url.rstrip('/')}/api/Plugin/authenticate",
            params={"apiKey": api_key, "pluginName": "Librarr"},
            timeout=10,
        )
        if resp.status_code == 200:
            token = resp.json().get("token", "")
            if not token:
                return {"success": False, "error": "Auth returned no token", "error_class": "protocol_error"}
            lib_resp = requests_module.get(f"{url.rstrip('/')}/api/Library", headers={"Authorization": f"Bearer {token}"}, timeout=10)
            libraries = [{"id": l["id"], "name": l["name"]} for l in lib_resp.json()] if lib_resp.status_code == 200 else []
            return {"success": True, "message": f"Connected ({len(libraries)} libraries)", "libraries": libraries}
        if resp.status_code == 401:
            return {"success": False, "error": "Invalid API key", "error_class": "auth_failed"}
        return {"success": False, "error": f"HTTP {resp.status_code}", "error_class": f"http_{resp.status_code}"}
    except requests_module.Timeout:
        return {"success": False, "error": "Timed out connecting to Kavita", "error_class": "timeout"}
    except requests_module.ConnectionError:
        return {"success": False, "error": "Connection refused — is Kavita running?", "error_class": "unreachable"}
    except Exception as e:
        return {"success": False, "error": str(e), "error_class": "request_error"}


def runtime_config_validation(config_module, qb, *, run_network_tests=False, requests_module=requests):
    rules_raw = config_module.TARGET_ROUTING_RULES or "{}"
    routing_rule_error = None
    try:
        parsed_rules = json.loads(rules_raw)
        if not isinstance(parsed_rules, dict):
            routing_rule_error = "target_routing_rules must be a JSON object"
    except Exception as e:
        parsed_rules = {}
        routing_rule_error = f"Invalid target_routing_rules JSON: {e}"

    checks = {
        "paths": [],
        "services": {},
        "qb": qb.diagnose() if run_network_tests else None,
        "routing_rules": {"valid": routing_rule_error is None, "error": routing_rule_error, "rules": parsed_rules if routing_rule_error is None else None},
    }
    checks["paths"].append(path_check("incoming_dir", config_module.INCOMING_DIR, create=True))
    if config_module.FILE_ORG_ENABLED:
        checks["paths"].append(path_check("ebook_organized_dir", config_module.EBOOK_ORGANIZED_DIR, create=True))
        checks["paths"].append(path_check("audiobook_organized_dir", config_module.AUDIOBOOK_ORGANIZED_DIR, create=True))
    else:
        checks["paths"].append({"name": "file_org", "ok": True, "info": "disabled"})
    if config_module.KAVITA_LIBRARY_PATH:
        checks["paths"].append(path_check("kavita_library_path", config_module.KAVITA_LIBRARY_PATH, create=True))

    checks["services"]["prowlarr"] = (
        test_prowlarr_connection(config_module.PROWLARR_URL, config_module.PROWLARR_API_KEY, requests_module=requests_module)
        if config_module.has_prowlarr() else {"success": None, "info": "not configured"}
    ) if run_network_tests else {"success": None, "info": "skipped"}
    checks["services"]["audiobookshelf"] = (
        test_audiobookshelf_connection(config_module.ABS_URL, config_module.ABS_TOKEN, requests_module=requests_module)
        if config_module.has_audiobookshelf() else {"success": None, "info": "not configured"}
    ) if run_network_tests else {"success": None, "info": "skipped"}
    checks["services"]["kavita"] = (
        test_kavita_connection(config_module.KAVITA_URL, config_module.KAVITA_API_KEY, requests_module=requests_module)
        if config_module.has_kavita() else {"success": None, "info": "not configured"}
    ) if run_network_tests else {"success": None, "info": "skipped"}

    path_errors = [p for p in checks["paths"] if p.get("ok") is False]
    svc_failures = [v for v in checks["services"].values() if v.get("success") is False]
    qb_fail = checks["qb"] and checks["qb"].get("success") is False
    checks["success"] = not path_errors and not svc_failures and not qb_fail and checks["routing_rules"]["valid"]
    return checks
