"""Base class for Librarr search sources.

To create a new source, subclass Source and drop the file in this directory.
The app auto-discovers and loads all Source subclasses on startup.

Minimal example (direct download):

    from .base import Source

    class MySource(Source):
        name = "mysource"
        label = "My Source"
        color = "#2ecc71"

        def search(self, query):
            return [{"title": "Example Book", "my_id": "123"}]

        def download(self, result, job):
            job["detail"] = "Downloading..."
            # ... download logic ...
            job["status"] = "completed"
            return True

Torrent source (downloads via qBittorrent, no download() needed):

    from .base import Source

    class MyTorrentSource(Source):
        name = "mytorrent"
        label = "My Torrent Site"
        color = "#e17055"
        download_type = "torrent"

        def search(self, query):
            return [{
                "title": "Example Book",
                "seeders": 10,
                "size_human": "1.5 GB",
                "download_url": "https://...",
            }]
"""


class Source:
    # -- Required: override these in your subclass --
    name = ""       # Internal ID, e.g. "annas", "mam". Must be unique.
    label = ""      # Display name for UI badge, e.g. "Anna's Archive"
    color = "#888"  # CSS hex color for badge

    # -- Optional: override as needed --
    download_type = "direct"
    """How this source's results are downloaded:
    - "direct": Framework creates a background job, calls self.download(result, job).
    - "torrent": Results sent to qBittorrent. Results must include download_url,
                 magnet_url, or info_hash.
    - "custom": Same as "direct" but signals the source handles everything
                (e.g. spawning external tools like lncrawl).
    """

    config_fields = []
    """List of config fields this source needs. Each is a dict:
        {"key": "api_key", "label": "API Key", "type": "text", "required": True}
    Types: "text", "password", "url"
    If all required fields are empty, the source is auto-disabled.
    Config values are stored in settings.json under source_{name}_{key}
    and can be overridden by env var SOURCE_{NAME}_{KEY}.
    """

    search_tab = "main"
    """Which search tab this source appears in:
    - "main": The main ebook search tab.
    - "audiobook": The audiobook search tab.
    """

    def enabled(self):
        """Return True if this source is configured and ready to use.
        Default: True if all required config_fields have non-empty values.
        Override for custom logic (e.g. checking existing config vars).
        """
        for field in self.config_fields:
            if field.get("required") and not self.get_config(field["key"]):
                return False
        return True

    def get_config(self, key):
        """Get a config value for this source.
        Checks env var SOURCE_{NAME}_{KEY}, then settings.json source_{name}_{key}.
        Injected by the source loader — do not override.
        """
        return ""

    def search(self, query):
        """Search this source. Return a list of result dicts.

        Every result dict MUST include:
            "title" (str): The book/item title.

        The framework automatically adds "source": self.name to each result.

        For download_type="torrent", results SHOULD include:
            "download_url", "magnet_url", or "info_hash" (at least one)
            "seeders", "leechers", "size_human", "indexer" (for display)

        For download_type="direct"/"custom", include whatever your
        download() method needs to do its job.

        Common display fields (shown automatically if present):
            "author", "size_human", "indexer", "seeders", "site"
        """
        return []

    def download(self, result, job):
        """Download a search result. Called in a background thread.

        Only called for download_type="direct" or "custom".
        Torrent sources use qBittorrent automatically.

        Args:
            result: The full result dict from search(), as sent by the client.
            job: A dict-like PersistentJob object for tracking progress.
                 Update these keys as you go:
                   job["status"]: "downloading" -> "importing" -> "completed"
                   job["detail"]: Progress string shown in the UI
                   job["error"]: Error message (set on failure)

        Returns:
            True if download succeeded, False otherwise.
            If you set job["status"] = "completed" yourself, return True.
            If you return False without setting status, the framework sets "error".
        """
        return False
