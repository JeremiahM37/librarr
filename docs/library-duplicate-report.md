# Library duplicate report

These commands are read-only. Run them against a copy or the live database
after stopping nothing and without opening it for writes:

```sh
DB=/opt/docker/data/librarr/librarr.db

# Rows pointing at the same normalized destination path.
sqlite3 -header -column "$DB" <<'SQL'
SELECT lower(replace(replace(trim(file_path), char(92), '/'), '//', '/')) AS normalized_path,
       COUNT(*) AS count,
       group_concat(id) AS ids,
       group_concat(title, ' | ') AS titles
FROM library_items
GROUP BY normalized_path
HAVING COUNT(*) > 1
ORDER BY count DESC;
SQL

# Same torrent/source identifier. This is diagnostic only: one torrent may
# legitimately contain multiple files and must not be deduplicated by source_id alone.
sqlite3 -header -column "$DB" <<'SQL'
SELECT source_id, COUNT(*) AS count, group_concat(id) AS ids,
       group_concat(title, ' | ') AS titles
FROM library_items
WHERE source_id <> ''
GROUP BY source_id
HAVING COUNT(*) > 1
ORDER BY count DESC;
SQL

# After the content_hash migration is present, identify repeated content by
# media type and format. EPUB and MOBI remain separate groups.
sqlite3 -header -column "$DB" <<'SQL'
SELECT content_hash, media_type, file_format, COUNT(*) AS count,
       group_concat(id) AS ids, group_concat(file_path, ' | ') AS paths
FROM library_items
WHERE content_hash <> ''
GROUP BY content_hash, media_type, file_format
HAVING COUNT(*) > 1
ORDER BY count DESC;
SQL
```

Same title/author/format is only a review group; it is not proof of a
duplicate file:

```sh
sqlite3 -header -column "$DB" <<'SQL'
SELECT lower(trim(title)) AS title, lower(trim(author)) AS author,
       media_type, lower(trim(file_format)) AS format,
       COUNT(*) AS count, group_concat(id) AS ids
FROM library_items
GROUP BY title, author, media_type, format
HAVING COUNT(*) > 1
ORDER BY count DESC;
SQL
```

For files not yet represented by `content_hash`, a read-only filesystem scan
can be compared with the database paths:

```sh
find /mnt/media/books/ebooks -type f -print0 | xargs -0 sha256sum | sort
```

Do not delete or merge rows automatically. Review exact path/content matches
separately from same-title or same-source groups.
