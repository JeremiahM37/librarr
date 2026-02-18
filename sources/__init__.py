"""Librarr source plugin loader.

Auto-discovers Source subclasses in this directory on startup.
Drop a .py file here with a Source subclass and it will be loaded automatically.
"""
import importlib
import logging
import os

from .base import Source

logger = logging.getLogger("librarr")

_sources = {}  # name -> Source instance


def _make_config_getter(source_name):
    """Create a get_config function bound to a specific source name."""
    def get_config(key):
        import config
        env_key = f"SOURCE_{source_name.upper()}_{key.upper()}"
        json_key = f"source_{source_name}_{key}"
        return config._get(env_key, json_key, "")
    return get_config


def load_sources():
    """Auto-discover and load all source modules in this directory."""
    source_dir = os.path.dirname(__file__)
    for filename in sorted(os.listdir(source_dir)):
        if filename.startswith("_") or not filename.endswith(".py"):
            continue
        if filename == "base.py":
            continue
        module_name = filename[:-3]
        try:
            module = importlib.import_module(f".{module_name}", package="sources")
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (isinstance(attr, type) and issubclass(attr, Source)
                        and attr is not Source and attr.name):
                    instance = attr()
                    instance.get_config = _make_config_getter(instance.name)
                    _sources[instance.name] = instance
                    status = "enabled" if instance.enabled() else "disabled"
                    logger.info(f"Source loaded: {instance.label} [{instance.name}] — {status}")
        except Exception as e:
            logger.error(f"Failed to load source {module_name}: {e}")


def get_sources():
    """Return dict of all loaded sources (name -> Source)."""
    return _sources


def get_enabled_sources(tab="main"):
    """Return list of enabled Source instances for a given search tab."""
    return [s for s in _sources.values() if s.enabled() and s.search_tab == tab]


def get_source(name):
    """Get a Source instance by name, or None."""
    return _sources.get(name)


def get_source_metadata():
    """Return metadata dict for all sources (for UI rendering)."""
    return {
        name: {
            "label": s.label,
            "color": s.color,
            "download_type": s.download_type,
            "enabled": s.enabled(),
            "search_tab": s.search_tab,
            "config_fields": s.config_fields,
        }
        for name, s in _sources.items()
    }
