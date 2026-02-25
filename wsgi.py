"""WSGI entrypoint for production servers (Gunicorn)."""

from app_factory import app, initialize_runtime_once

initialize_runtime_once()
