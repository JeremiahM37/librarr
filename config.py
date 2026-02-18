import json
import os
import threading

# =============================================================================
# Librarr Configuration
# Priority: environment variables > settings.json > defaults
# =============================================================================

SETTINGS_FILE = os.getenv("LIBRARR_SETTINGS_FILE", "/data/librarr/settings.json")

_lock = threading.Lock()
_file_settings = {}


def _load_file_settings():
    global _file_settings
    try:
        with open(SETTINGS_FILE, "r") as f:
            _file_settings = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        _file_settings = {}


def save_settings(new_settings):
    global _file_settings
    with _lock:
        _load_file_settings()
        _file_settings.update(new_settings)
        os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
        with open(SETTINGS_FILE, "w") as f:
            json.dump(_file_settings, f, indent=2)
        # Reload module-level vars
        _apply_settings()


def _get(env_key, json_key, default=""):
    """Get a config value: env var wins, then settings.json, then default."""
    env_val = os.getenv(env_key, "")
    if env_val:
        return env_val
    return _file_settings.get(json_key, default)


def _apply_settings():
    """Apply settings to module-level variables."""
    global PROWLARR_URL, PROWLARR_API_KEY
    global QB_URL, QB_USER, QB_PASS, QB_SAVE_PATH, QB_CATEGORY
    global ABS_URL, ABS_TOKEN, ABS_LIBRARY_ID, ABS_EBOOK_LIBRARY_ID, ABS_PUBLIC_URL
    global AUDIOBOOK_DIR, QB_AUDIOBOOK_SAVE_PATH, QB_AUDIOBOOK_CATEGORY
    global LNCRAWL_CONTAINER, CALIBRE_CONTAINER, CALIBRE_LIBRARY
    global CALIBRE_LIBRARY_CONTAINER, CALIBRE_DB, INCOMING_DIR
    global KAVITA_URL, KAVITA_API_KEY, KAVITA_LIBRARY_ID, KAVITA_LIBRARY_PATH
    global FILE_ORG_ENABLED, EBOOK_ORGANIZED_DIR, AUDIOBOOK_ORGANIZED_DIR
    global ENABLED_TARGETS

    # Prowlarr
    PROWLARR_URL = _get("PROWLARR_URL", "prowlarr_url")
    PROWLARR_API_KEY = _get("PROWLARR_API_KEY", "prowlarr_api_key")

    # qBittorrent
    QB_URL = _get("QB_URL", "qb_url")
    QB_USER = _get("QB_USER", "qb_user", "admin")
    QB_PASS = _get("QB_PASS", "qb_pass")
    QB_SAVE_PATH = _get("QB_SAVE_PATH", "qb_save_path", "/books-incoming/")
    QB_CATEGORY = _get("QB_CATEGORY", "qb_category", "books")

    # Audiobookshelf
    ABS_URL = _get("ABS_URL", "abs_url")
    ABS_TOKEN = _get("ABS_TOKEN", "abs_token")
    ABS_LIBRARY_ID = _get("ABS_LIBRARY_ID", "abs_library_id")
    ABS_EBOOK_LIBRARY_ID = _get("ABS_EBOOK_LIBRARY_ID", "abs_ebook_library_id")
    ABS_PUBLIC_URL = _get("ABS_PUBLIC_URL", "abs_public_url")

    # Audiobook downloads
    AUDIOBOOK_DIR = _get("AUDIOBOOK_DIR", "audiobook_dir", "/data/media/books/audiobooks")
    QB_AUDIOBOOK_SAVE_PATH = _get("QB_AUDIOBOOK_SAVE_PATH", "qb_audiobook_save_path", "/audiobooks-incoming/")
    QB_AUDIOBOOK_CATEGORY = _get("QB_AUDIOBOOK_CATEGORY", "qb_audiobook_category", "audiobooks")

    # lightnovel-crawler
    LNCRAWL_CONTAINER = _get("LNCRAWL_CONTAINER", "lncrawl_container")

    # Calibre-Web
    CALIBRE_CONTAINER = _get("CALIBRE_CONTAINER", "calibre_container")
    CALIBRE_LIBRARY = _get("CALIBRE_LIBRARY", "calibre_library", "/data/media/books/ebooks")
    CALIBRE_LIBRARY_CONTAINER = _get("CALIBRE_LIBRARY_CONTAINER", "calibre_library_container", "/books")
    CALIBRE_DB = os.path.join(CALIBRE_LIBRARY, "metadata.db")

    # Incoming directory
    INCOMING_DIR = _get("INCOMING_DIR", "incoming_dir", "/data/media/books/ebooks/incoming")

    # Kavita
    KAVITA_URL = _get("KAVITA_URL", "kavita_url")
    KAVITA_API_KEY = _get("KAVITA_API_KEY", "kavita_api_key")
    KAVITA_LIBRARY_ID = _get("KAVITA_LIBRARY_ID", "kavita_library_id", "")
    KAVITA_LIBRARY_PATH = _get("KAVITA_LIBRARY_PATH", "kavita_library_path", "")

    # File organization
    FILE_ORG_ENABLED = _get("FILE_ORG_ENABLED", "file_org_enabled", "true").lower() in ("true", "1", "yes")
    EBOOK_ORGANIZED_DIR = _get("EBOOK_ORGANIZED_DIR", "ebook_organized_dir", "/data/media/books/ebooks")
    AUDIOBOOK_ORGANIZED_DIR = _get("AUDIOBOOK_ORGANIZED_DIR", "audiobook_organized_dir", "/data/media/books/audiobooks")

    # Pipeline targets (comma-separated)
    ENABLED_TARGETS = _get("ENABLED_TARGETS", "enabled_targets", "calibre,audiobookshelf")


# Feature flags
def has_prowlarr():
    return bool(PROWLARR_URL and PROWLARR_API_KEY)

def has_qbittorrent():
    return bool(QB_URL)

def has_audiobookshelf():
    return bool(ABS_URL and ABS_TOKEN)

def has_calibre():
    return bool(CALIBRE_CONTAINER)

def has_lncrawl():
    return bool(LNCRAWL_CONTAINER)

def has_audiobooks():
    return bool(QB_URL)

def has_kavita():
    return bool(KAVITA_URL and KAVITA_API_KEY)

def get_enabled_target_names():
    """Return set of target names the user has enabled."""
    return set(t.strip() for t in ENABLED_TARGETS.split(",") if t.strip())

def get_all_settings():
    """Return current settings (for the settings UI), masking sensitive values."""
    return {
        "prowlarr_url": PROWLARR_URL,
        "prowlarr_api_key": PROWLARR_API_KEY,
        "qb_url": QB_URL,
        "qb_user": QB_USER,
        "qb_pass": QB_PASS,
        "qb_save_path": QB_SAVE_PATH,
        "qb_category": QB_CATEGORY,
        "qb_audiobook_save_path": QB_AUDIOBOOK_SAVE_PATH,
        "qb_audiobook_category": QB_AUDIOBOOK_CATEGORY,
        "abs_url": ABS_URL,
        "abs_token": ABS_TOKEN,
        "abs_library_id": ABS_LIBRARY_ID,
        "abs_ebook_library_id": ABS_EBOOK_LIBRARY_ID,
        "abs_public_url": ABS_PUBLIC_URL,
        "calibre_container": CALIBRE_CONTAINER,
        "calibre_library": CALIBRE_LIBRARY,
        "calibre_library_container": CALIBRE_LIBRARY_CONTAINER,
        "lncrawl_container": LNCRAWL_CONTAINER,
        "incoming_dir": INCOMING_DIR,
        "audiobook_dir": AUDIOBOOK_DIR,
        "kavita_url": KAVITA_URL,
        "kavita_api_key": KAVITA_API_KEY,
        "kavita_library_id": KAVITA_LIBRARY_ID,
        "kavita_library_path": KAVITA_LIBRARY_PATH,
        "file_org_enabled": FILE_ORG_ENABLED,
        "ebook_organized_dir": EBOOK_ORGANIZED_DIR,
        "audiobook_organized_dir": AUDIOBOOK_ORGANIZED_DIR,
        "enabled_targets": ENABLED_TARGETS,
    }


# Initialize on import
_load_file_settings()
_apply_settings()
