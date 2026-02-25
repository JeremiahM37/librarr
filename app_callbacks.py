from __future__ import annotations


def make_blueprint_registrar(app, deps):
    registered = {"value": False}

    def _register_blueprints():
        if registered["value"]:
            return
        deps["blueprint_registry"].register_blueprints(app, deps["blueprint_deps"])
        registered["value"] = True

    return _register_blueprints


def make_abb_rotate_callback(provider_search, state):
    def _rotate_abb_domain():
        state["ABB_URL"] = provider_search.rotate_abb_domain()
        return state["ABB_URL"]

    return _rotate_abb_domain
