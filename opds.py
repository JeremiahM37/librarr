"""
opds.py — OPDS 1.2 catalog server for Librarr.

Allows e-readers (Kobo, PocketBook, Moon+ Reader, Librera, etc.) to browse
and download books directly from Librarr without any extra software.

Add to app.py __main__ block:
    import opds
    opds.init_app(app, library)

Endpoints:
    GET /opds/                  → root navigation catalog
    GET /opds/library           → full library acquisition feed (paginated)
    GET /opds/search?q=...      → live multi-source search as OPDS feed
    GET /opds/opensearch.xml    → OpenSearch description document
    GET /opds/download/<id>     → serve the actual ebook/audiobook file
"""

import os
from datetime import datetime, timezone
from xml.sax.saxutils import escape as _xml_escape

from flask import Blueprint, Response, request, send_file

opds_bp = Blueprint("opds", __name__)

_library = None       # LibraryDB instance injected via init_app
_search_fn = None     # callable(query, source_type) injected via init_app


# MIME type constants
_NAV = "application/atom+xml;profile=opds-catalog;kind=navigation"
_ACQ = "application/atom+xml;profile=opds-catalog;kind=acquisition"
_OS  = "application/opensearchdescription+xml"

_FORMAT_MIMES = {
    "epub": "application/epub+zip",
    "pdf":  "application/pdf",
    "mobi": "application/x-mobipocket-ebook",
    "azw3": "application/x-mobi8-ebook",
    "mp3":  "audio/mpeg",
    "m4b":  "audio/mp4",
    "ogg":  "audio/ogg",
    "flac": "audio/flac",
    "cbz":  "application/x-cbz",
    "cbr":  "application/x-cbr",
}

PAGE_SIZE = 50


def init_app(app, library, search_fn=None):
    """Register the OPDS blueprint and inject dependencies.

    Args:
        app: Flask application instance.
        library: LibraryDB instance for reading the local collection.
        search_fn: Optional callable(query) → list[result_dicts] for live search.
    """
    global _library, _search_fn
    _library = library
    _search_fn = search_fn
    app.register_blueprint(opds_bp)


# ── XML helpers ────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts(ts):
    if not ts:
        return _now()
    return datetime.fromtimestamp(float(ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _e(s):
    return _xml_escape(str(s or ""))


def _xml(content: str) -> Response:
    return Response(content, content_type="application/atom+xml; charset=utf-8")


def _feed_open(feed_id: str, title: str, kind: str, self_href: str,
               total: int = 0, page: int = 1) -> str:
    mime = _NAV if kind == "navigation" else _ACQ
    items_per_page = PAGE_SIZE
    start_index = (page - 1) * items_per_page + 1
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:opds="http://opds-spec.org/2010/catalog"
      xmlns:dc="http://purl.org/dc/terms/"
      xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
  <id>urn:librarr:{_e(feed_id)}</id>
  <title>{_e(title)}</title>
  <updated>{_now()}</updated>
  <author><name>Librarr</name></author>
  <link rel="self" href="{_e(self_href)}" type="{mime}"/>
  <link rel="start" href="/opds/" type="{_NAV}"/>
  <link rel="search" href="/opds/opensearch.xml" type="{_OS}"/>
  <opensearch:totalResults>{total}</opensearch:totalResults>
  <opensearch:itemsPerPage>{items_per_page}</opensearch:itemsPerPage>
  <opensearch:startIndex>{start_index}</opensearch:startIndex>
"""


def _feed_close() -> str:
    return "</feed>"


def _nav_entry(entry_id: str, title: str, content: str, href: str, mime: str = _ACQ) -> str:
    return f"""  <entry>
    <title>{_e(title)}</title>
    <id>urn:librarr:{_e(entry_id)}</id>
    <updated>{_now()}</updated>
    <content type="text">{_e(content)}</content>
    <link rel="subsection" href="{_e(href)}" type="{mime}"/>
  </entry>
"""


def _item_entry(item: dict) -> str:
    fmt = (item.get("file_format") or "").lower().lstrip(".")
    mime = _FORMAT_MIMES.get(fmt, "application/octet-stream")
    item_id = item["id"]
    updated = _ts(item.get("added_at"))
    title = _e(item.get("title") or "Unknown Title")
    author = _e(item.get("author") or "Unknown Author")

    author_xml = f"  <author><name>{author}</name></author>\n" if item.get("author") else ""
    cover_xml = f"""  <link rel="http://opds-spec.org/image" href="/api/cover/{item_id}" type="image/jpeg"/>
  <link rel="http://opds-spec.org/image/thumbnail" href="/api/cover/{item_id}" type="image/jpeg"/>
""" if item.get("file_path") else ""

    # Summary from metadata if available
    summary_xml = ""
    try:
        import json
        meta = json.loads(item.get("metadata") or "{}")
        if meta.get("description"):
            summary_xml = f"  <summary>{_e(meta['description'][:500])}</summary>\n"
    except Exception:
        pass

    return f"""  <entry>
    <title>{title}</title>
    <id>urn:librarr:item:{item_id}</id>
    <updated>{updated}</updated>
{author_xml}{cover_xml}{summary_xml}    <dc:format>{_e(mime)}</dc:format>
    <link rel="http://opds-spec.org/acquisition"
          href="/opds/download/{item_id}"
          type="{_e(mime)}"/>
  </entry>
"""


# ── Routes ─────────────────────────────────────────────────────────────────────

@opds_bp.route("/opds/")
@opds_bp.route("/opds")
def opds_root():
    """Root navigation catalog."""
    total_books = _library.count_items(media_type="ebook") if _library else 0
    total_audio = _library.count_items(media_type="audiobook") if _library else 0
    total = total_books + total_audio

    body = _feed_open("", "Librarr", "navigation", "/opds/", total=total)
    body += _nav_entry(
        "library",
        f"My Library ({total} items)",
        "Browse your downloaded books and audiobooks",
        "/opds/library",
    )
    body += _nav_entry(
        "library-ebooks",
        f"Ebooks ({total_books})",
        "Browse ebooks in your local library",
        "/opds/library?type=ebook",
    )
    body += _nav_entry(
        "library-audiobooks",
        f"Audiobooks ({total_audio})",
        "Browse audiobooks in your local library",
        "/opds/library?type=audiobook",
    )
    body += _nav_entry(
        "search",
        "Search",
        "Search all sources for new books, audiobooks, and web novels",
        "/opds/search?q={searchTerms}",
        mime=_ACQ,
    )
    body += _feed_close()
    return _xml(body)


@opds_bp.route("/opds/library")
def opds_library():
    """Paginated acquisition feed of the local library."""
    page = max(1, int(request.args.get("page", 1)))
    media_type = request.args.get("type") or None
    offset = (page - 1) * PAGE_SIZE

    if _library is None:
        items, total = [], 0
    else:
        items = _library.get_items(limit=PAGE_SIZE, offset=offset, media_type=media_type)
        total = _library.count_items(media_type=media_type)

    self_href = f"/opds/library?page={page}" + (f"&type={media_type}" if media_type else "")
    body = _feed_open("library", "My Library", "acquisition", self_href,
                      total=total, page=page)

    # Pagination links
    if page > 1:
        prev_href = f"/opds/library?page={page-1}" + (f"&type={media_type}" if media_type else "")
        body += f'  <link rel="previous" href="{_e(prev_href)}" type="{_ACQ}"/>\n'
    if offset + PAGE_SIZE < total:
        next_href = f"/opds/library?page={page+1}" + (f"&type={media_type}" if media_type else "")
        body += f'  <link rel="next" href="{_e(next_href)}" type="{_ACQ}"/>\n'

    for item in items:
        body += _item_entry(item)

    body += _feed_close()
    return _xml(body)


@opds_bp.route("/opds/search")
def opds_search():
    """Live multi-source search returned as an OPDS acquisition feed."""
    query = request.args.get("q", "").strip()
    if not query:
        return opds_root()

    results = []
    if _search_fn and query:
        try:
            raw = _search_fn(query)
            # Convert search result dicts to item-like dicts for _item_entry
            for r in raw[:50]:
                results.append({
                    "id": r.get("id") or r.get("guid") or "",
                    "title": r.get("title", ""),
                    "author": r.get("author") or r.get("artist") or "",
                    "file_format": r.get("format") or r.get("file_format") or "epub",
                    "added_at": None,
                    "file_path": r.get("download_url") or r.get("url") or "",
                    "metadata": None,
                })
        except Exception:
            pass

    body = _feed_open(f"search?q={_e(query)}", f'Search: {_e(query)}', "acquisition",
                      f"/opds/search?q={_e(query)}", total=len(results))
    for item in results:
        body += _item_entry(item)
    body += _feed_close()
    return _xml(body)


@opds_bp.route("/opds/opensearch.xml")
def opds_opensearch():
    """OpenSearch description document — tells clients how to search."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/">
  <ShortName>Librarr</ShortName>
  <Description>Search Librarr for books, audiobooks, and web novels</Description>
  <InputEncoding>UTF-8</InputEncoding>
  <OutputEncoding>UTF-8</OutputEncoding>
  <Url type="application/atom+xml;profile=opds-catalog;kind=acquisition"
       template="/opds/search?q={searchTerms}"/>
</OpenSearchDescription>"""
    return Response(xml, content_type="application/opensearchdescription+xml; charset=utf-8")


@opds_bp.route("/opds/download/<item_id>")
def opds_download(item_id):
    """Serve the actual ebook/audiobook file to the e-reader."""
    if _library is None:
        return ("Library unavailable", 503)

    items = _library.get_items(limit=1, offset=0)  # fallback
    # Get specific item — use find by id
    item = None
    try:
        items_all = _library.get_items(limit=10000, offset=0)
        for it in items_all:
            if str(it.get("id")) == str(item_id):
                item = it
                break
    except Exception:
        pass

    if not item:
        return ("Item not found", 404)

    file_path = item.get("file_path")
    if not file_path or not os.path.exists(file_path):
        return ("File not found on disk", 404)

    fmt = (item.get("file_format") or os.path.splitext(file_path)[1]).lower().lstrip(".")
    mime = _FORMAT_MIMES.get(fmt, "application/octet-stream")

    return send_file(
        file_path,
        mimetype=mime,
        as_attachment=True,
        download_name=os.path.basename(file_path),
    )
