import os

# =============================================================================
# Librarr Configuration
# All settings are configured via environment variables.
# =============================================================================

# --- Prowlarr (optional: enables torrent search) ---
PROWLARR_URL = os.getenv("PROWLARR_URL", "")
PROWLARR_API_KEY = os.getenv("PROWLARR_API_KEY", "")

# --- qBittorrent (optional: enables torrent downloads) ---
QB_URL = os.getenv("QB_URL", "")
QB_USER = os.getenv("QB_USER", "admin")
QB_PASS = os.getenv("QB_PASS", "")
QB_SAVE_PATH = os.getenv("QB_SAVE_PATH", "/books-incoming/")
QB_CATEGORY = os.getenv("QB_CATEGORY", "books")

# --- Audiobookshelf (optional: enables library browsing + audiobook management) ---
ABS_URL = os.getenv("ABS_URL", "")
ABS_TOKEN = os.getenv("ABS_TOKEN", "")
ABS_LIBRARY_ID = os.getenv("ABS_LIBRARY_ID", "")
ABS_EBOOK_LIBRARY_ID = os.getenv("ABS_EBOOK_LIBRARY_ID", "")
ABS_PUBLIC_URL = os.getenv("ABS_PUBLIC_URL", "")

# --- Audiobook downloads ---
AUDIOBOOK_DIR = os.getenv("AUDIOBOOK_DIR", "/data/media/books/audiobooks")
QB_AUDIOBOOK_SAVE_PATH = os.getenv("QB_AUDIOBOOK_SAVE_PATH", "/audiobooks-incoming/")
QB_AUDIOBOOK_CATEGORY = os.getenv("QB_AUDIOBOOK_CATEGORY", "audiobooks")

# --- lightnovel-crawler (optional: enables web novel scraping to EPUB) ---
LNCRAWL_CONTAINER = os.getenv("LNCRAWL_CONTAINER", "")

# --- Calibre-Web (optional: enables ebook import via calibredb) ---
CALIBRE_CONTAINER = os.getenv("CALIBRE_CONTAINER", "")
CALIBRE_LIBRARY = os.getenv("CALIBRE_LIBRARY", "/data/media/books/ebooks")
CALIBRE_LIBRARY_CONTAINER = os.getenv("CALIBRE_LIBRARY_CONTAINER", "/books")
CALIBRE_DB = os.path.join(CALIBRE_LIBRARY, "metadata.db")

# --- Incoming directory for downloads ---
INCOMING_DIR = os.getenv("INCOMING_DIR", "/data/media/books/ebooks/incoming")


# =============================================================================
# Feature flags (derived from config above)
# =============================================================================
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
