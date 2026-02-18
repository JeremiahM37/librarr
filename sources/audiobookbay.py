"""AudioBookBay — audiobook torrent search (always available)."""
from .base import Source


class AudioBookBaySource(Source):
    name = "audiobook"
    label = "AudioBookBay"
    color = "#fd79a8"
    download_type = "torrent"
    search_tab = "audiobook"

    def enabled(self):
        return True  # Always available (search works without config, download needs qBit)

    def search(self, query):
        from app import search_audiobookbay
        return search_audiobookbay(query)
