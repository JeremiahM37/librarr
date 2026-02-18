"""Prowlarr — torrent indexer search (ebooks + audiobooks)."""
import config
from .base import Source


class ProwlarrSource(Source):
    name = "torrent"
    label = "Prowlarr"
    color = "#e17055"
    download_type = "torrent"
    search_tab = "main"

    def enabled(self):
        return config.has_prowlarr()

    def search(self, query):
        from app import search_prowlarr
        return search_prowlarr(query)


class ProwlarrAudiobookSource(Source):
    name = "prowlarr_audiobook"
    label = "Prowlarr"
    color = "#fd79a8"
    download_type = "torrent"
    search_tab = "audiobook"

    def enabled(self):
        return config.has_prowlarr()

    def search(self, query):
        from app import search_prowlarr_audiobooks
        return search_prowlarr_audiobooks(query)
