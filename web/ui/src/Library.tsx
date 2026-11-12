import { useEffect, useMemo, useRef, useState } from "react";
import {
  api,
  ApiError,
  assetUrl,
  type LibraryItem,
  type LibraryResponse,
  type MediaType,
} from "./api";
import { useTranslation, type TranslationKey } from "./i18n";

type Category = "ebooks" | "audiobooks" | "manga";
const endpoints: Record<Category, string> = {
  ebooks: "/api/library",
  audiobooks: "/api/library/audiobooks",
  manga: "/api/library/manga",
};
const gradients = [
  "from-indigo-600 to-purple-700",
  "from-blue-600 to-cyan-700",
  "from-emerald-600 to-teal-700",
  "from-rose-600 to-pink-700",
  "from-amber-600 to-orange-700",
  "from-violet-600 to-fuchsia-700",
  "from-sky-600 to-blue-700",
  "from-lime-600 to-green-700",
];

function seriesBaseName(series = ""): string {
  return series
    .replace(/\s*#\s*[\d.]+.*$/, "")
    .replace(/\s*Book\s+[\d.]+.*$/i, "")
    .replace(/\s*Vol(ume)?\s*\.?\s*[\d.]+.*$/i, "")
    .replace(/\s*\([\d.]+\).*$/, "")
    .replace(/\s*,\s*$/, "")
    .trim();
}
function displayTitle(item: LibraryItem): string {
  return item.title || item.name || "Unknown";
}
function initials(title: string): string {
  return (
    title
      .split(/\s+/)
      .filter(Boolean)
      .slice(0, 2)
      .map((word) => word[0]?.toUpperCase())
      .join("") || "?"
  );
}
function size(value?: number | string): string {
  if (typeof value === "string") return value;
  if (!value) return "";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let n = value,
    i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
}

function Cover({
  item,
  index,
  sizing = "w-full h-48",
}: {
  item: LibraryItem;
  index: number;
  sizing?: string;
}): React.JSX.Element {
  const [failed, setFailed] = useState(false);
  const title = displayTitle(item);
  if (!item.cover_url || failed)
    return (
      <div
        data-cover-fallback
        className={`${sizing} bg-gradient-to-br ${gradients[index % gradients.length]} flex items-center justify-center text-3xl font-bold text-white/80`}
      >
        {initials(title)}
      </div>
    );
  return (
    <img
      src={assetUrl(item.cover_url)}
      alt=""
      className={`${sizing} object-cover`}
      loading="lazy"
      onError={() => setFailed(true)}
    />
  );
}

function Card({
  item,
  index,
  category,
  remove,
  removing,
  open,
}: {
  item: LibraryItem;
  index: number;
  category: Category;
  remove: (item: LibraryItem) => void;
  removing: boolean;
  open?: () => void;
}): React.JSX.Element {
  const { t } = useTranslation();
  const title = displayTitle(item);
  const format = formatOf(item);
  return (
    <article className="book-card bg-slate-900 rounded-xl overflow-hidden border border-slate-800 relative group">
      <Cover item={item} index={isLocal(item) ? item.id : index} />
      {item.id != null && !item.kavita_url && (
        <button
          data-action="deleteLibraryItem"
          title="Remove from library"
          aria-label="Remove from library"
          disabled={removing}
          onClick={() => remove(item)}
          className="absolute top-2 right-2 z-10 w-11 h-11 rounded-full bg-slate-950/80 text-white hover:bg-red-700 disabled:opacity-50"
        >
          ✕
        </button>
      )}
      <div className="p-3">
        <h3 className="text-sm font-semibold text-white">
          {open ? (
            <button
              type="button"
              data-action="openLibraryItem"
              className="text-left hover:underline after:absolute after:inset-0"
              onClick={open}
            >
              {title}
            </button>
          ) : (
            title
          )}
        </h3>
        {item.author && (
          <p className="text-xs text-slate-400">{item.author}</p>
        )}
        {item.series && (
          <p className="text-xs text-indigo-400">{item.series}</p>
        )}
        <div className="flex gap-2 text-xs text-slate-500">
          {Boolean(format || item.file_size || item.size) && (
            <>
              <span className="uppercase">{format}</span>
              <span>{size(item.file_size ?? item.size)}</span>
            </>
          )}
          {category === "audiobooks" && (
            <>
              <span>
                {item.duration_hours
                  ? `${item.duration_hours.toFixed(1)}h`
                  : ""}
              </span>
              {item.num_files ? (
                <span>{t("n_files", { n: item.num_files })}</span>
              ) : null}
            </>
          )}
          {category === "manga" && (
            <>
              {item.pages ? (
                <span>{t("n_pages", { n: item.pages })}</span>
              ) : null}
              <span>{item.library}</span>
            </>
          )}
        </div>
        {item.abs_url && (
          <a
            className="relative z-10"
            href={item.abs_url}
            target="_blank"
            rel="noreferrer"
          >
            {t("open_in_abs")}
          </a>
        )}
        {item.kavita_url && (
          <a
            className="relative z-10"
            href={item.kavita_url}
            target="_blank"
            rel="noreferrer"
          >
            {t("open_in_kavita")}
          </a>
        )}
      </div>
    </article>
  );
}

function isLocal(item: LibraryItem): item is LibraryItem & { id: number } {
  return typeof item.id === "number" && !item.abs_url && !item.kavita_url;
}
// The Librarr record behind a card: the row itself, or for a row listed from
// Audiobookshelf or Kavita, the record the server matched it to.
function localId(item: LibraryItem): number | undefined {
  return isLocal(item) ? item.id : item.local_id;
}
function formatOf(item: LibraryItem): string {
  return (
    item.file_format ||
    item.format ||
    item.file_path?.match(/\.([^./\\]+)$/)?.[1] ||
    ""
  );
}
const mediaLabels: Record<MediaType, TranslationKey> = {
  ebook: "tab_ebooks",
  audiobook: "tab_audiobooks",
  manga: "tab_manga",
};

export function ItemDetail({
  id,
  listing,
  onClose,
  remove,
  removing,
}: {
  id: number;
  // The Library card this was opened from, when it came from Audiobookshelf
  // or Kavita: its link is shown, and removing acts on it, as the card's ✕ does.
  listing?: LibraryItem;
  onClose: () => void;
  remove: (item: LibraryItem) => Promise<boolean>;
  removing: boolean;
}): React.JSX.Element {
  const { t } = useTranslation();
  const dialog = useRef<HTMLDialogElement>(null);
  const [item, setItem] = useState<LibraryItem | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    dialog.current?.showModal();
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    setItem(null);
    setError("");
    api<LibraryItem>(`/api/library/${id}`, { signal: controller.signal })
      .then(setItem)
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setError(
          e instanceof ApiError && e.status === 404
            ? t("item_not_found")
            : t("item_load_failed", {
                msg: e instanceof Error ? e.message : String(e),
              }),
        );
      });
    return () => controller.abort();
  }, [id]);
  const title = item ? displayTitle(item) : "";
  const format = item ? formatOf(item) : "";
  const added = item?.added_at ? new Date(item.added_at) : null;
  const facts: [TranslationKey, string][] = item
    ? [
        ["item_format", format.toUpperCase()],
        ["item_size", size(item.file_size ?? item.size)],
        ["item_type", item.media_type ? t(mediaLabels[item.media_type]) : ""],
        [
          "item_added",
          added && !Number.isNaN(added.getTime())
            ? added.toLocaleDateString()
            : "",
        ],
        ["item_source", item.source ?? ""],
      ]
    : [];
  return (
    <dialog
      ref={dialog}
      id="library-item-detail"
      aria-label={item ? t("item_details", { title }) : undefined}
      aria-busy={!item && !error}
      className="library-item-detail bg-slate-900 text-slate-200 border border-slate-700 rounded-xl p-0"
      onClose={onClose}
      onClick={(event) => {
        if (event.target === dialog.current) dialog.current?.close();
      }}
    >
      <div className="p-4 sm:p-6">
        <div className="flex justify-end">
          <button
            type="button"
            data-action="closeLibraryItem"
            aria-label={t("item_close")}
            className="w-11 h-11 rounded-full hover:bg-slate-800"
            onClick={() => dialog.current?.close()}
          >
            ✕
          </button>
        </div>
        {error && <p role="alert">{error}</p>}
        {!item && !error && <p>Loading…</p>}
        {item && (
          <div className="flex flex-col sm:flex-row gap-6">
            <div className="sm:w-48 shrink-0 rounded-lg overflow-hidden self-center sm:self-start">
              <Cover item={item} index={id} sizing="w-48 h-72" />
            </div>
            <div className="min-w-0 flex-1 space-y-4">
              <div>
                <h2 className="text-xl font-semibold text-white">{title}</h2>
                {item.author && <p className="text-slate-400">{item.author}</p>}
                {item.series && (
                  <p className="text-indigo-400">{item.series}</p>
                )}
              </div>
              <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
                {facts
                  .filter(([, value]) => value)
                  .map(([label, value]) => (
                    <div className="contents" key={label}>
                      <dt className="text-slate-500">{t(label)}</dt>
                      <dd>{value}</dd>
                    </div>
                  ))}
                {item.file_path && (
                  <div className="contents">
                    <dt className="text-slate-500">{t("item_file")}</dt>
                    <dd className="font-mono text-xs break-all">
                      {item.file_path}
                    </dd>
                  </div>
                )}
              </dl>
              {!!item.tags?.length && (
                <div>
                  <h3 className="text-sm text-slate-500">{t("item_tags")}</h3>
                  <ul className="flex flex-wrap gap-2 mt-1">
                    {item.tags.map((tag) => (
                      <li
                        key={tag.id}
                        className="px-2 py-1 rounded bg-slate-800 text-xs"
                        style={tag.color ? { borderLeft: `3px solid ${tag.color}` } : undefined}
                      >
                        {tag.name}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              <div className="flex flex-wrap gap-2">
                {/\.[^./\\]+$/.test(item.file_path ?? "") && (
                  <a
                    data-action="downloadLibraryFile"
                    className="px-4 py-2 rounded-lg bg-indigo-600 text-white"
                    href={assetUrl(`/api/library/${id}/file`)}
                    download
                  >
                    {t("item_download_file")}
                  </a>
                )}
                {listing?.abs_url && (
                  <a
                    className="px-4 py-2 rounded-lg bg-slate-800"
                    href={listing.abs_url}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {t("open_in_abs")}
                  </a>
                )}
                {listing?.kavita_url && (
                  <a
                    className="px-4 py-2 rounded-lg bg-slate-800"
                    href={listing.kavita_url}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {t("open_in_kavita")}
                  </a>
                )}
                {!listing?.kavita_url && (
                  <button
                    type="button"
                    data-action="removeLibraryItem"
                    className="px-4 py-2 rounded-lg bg-slate-800 hover:bg-red-700 disabled:opacity-50"
                    disabled={removing}
                    onClick={() =>
                      void remove(listing ?? item).then((removed) => {
                        if (removed) dialog.current?.close();
                      })
                    }
                  >
                    {t("item_remove")}
                  </button>
                )}
              </div>
            </div>
          </div>
        )}
      </div>
    </dialog>
  );
}

export function Library({
  setNotice,
}: {
  setNotice: (message: string) => void;
}): React.JSX.Element {
  const { t } = useTranslation();
  const [category, setCategory] = useState<Category>("ebooks"),
    [query, setQuery] = useState(""),
    [debounced, setDebounced] = useState(""),
    [page, setPage] = useState(1),
    [pages, setPages] = useState(1),
    [items, setItems] = useState<LibraryItem[]>([]),
    [loading, setLoading] = useState(false),
    [removing, setRemoving] = useState(false),
    [revision, setRevision] = useState(0),
    [detail, setDetail] = useState<{ id: number; listing?: LibraryItem } | null>(null);
  const requestVersion = useRef(0);
  useEffect(() => {
    const nextQuery = query.trim();
    if (nextQuery === debounced) return;
    const timer = window.setTimeout(() => {
      setPage(1);
      setDebounced(nextQuery);
    }, 400);
    return () => window.clearTimeout(timer);
  }, [query, debounced]);
  const load = async () => {
    const version = ++requestVersion.current;
    setLoading(true);
    try {
      const suffix = `?page=${page}${debounced ? `&q=${encodeURIComponent(debounced)}` : ""}`;
      const data = await api<LibraryResponse>(endpoints[category] + suffix);
      if (version !== requestVersion.current) return;
      const lastPage = Math.max(1, data.pages ?? 1);
      if (page > lastPage) {
        setPage(lastPage);
        return;
      }
      setItems(data.items ?? []);
      setPages(lastPage);
    } catch (error) {
      if (version !== requestVersion.current) return;
      setItems([]);
      setPages(1);
      setNotice(
        error instanceof Error ? error.message : t("failed_load_library"),
      );
    } finally {
      if (version === requestVersion.current) setLoading(false);
    }
  };
  useEffect(() => {
    void load();
    return () => {
      requestVersion.current++;
    };
  }, [category, page, debounced, revision]);
  const grouped = useMemo(() => {
    const groups = new Map<string, LibraryItem[]>(),
      standalone: LibraryItem[] = [];
    for (const item of items) {
      const base = seriesBaseName(item.series);
      if (!base) standalone.push(item);
      else groups.set(base, [...(groups.get(base) ?? []), item]);
    }
    return { groups: [...groups], standalone };
  }, [items]);
  const remove = async (item: LibraryItem): Promise<boolean> => {
    if (
      removing ||
      item.id == null ||
      item.kavita_url ||
      !window.confirm(`Remove "${displayTitle(item)}" from library?`)
    )
      return false;
    const media =
      item.media_type ??
      (category === "audiobooks"
        ? "audiobook"
        : category === "manga" ? "manga" : "ebook");
    const type = media === "ebook" ? "book" : media;
    const suffix = type === "manga" ? "?source=local" : "";
    setRemoving(true);
    try {
      await api(`/api/library/${type}/${encodeURIComponent(item.id)}${suffix}`, {
        method: "DELETE",
      });
      setNotice(`Removed "${displayTitle(item)}"`);
      setRevision((value) => value + 1);
      return true;
    } catch (error) {
      setNotice(
        `Failed to remove: ${error instanceof Error ? error.message : "unknown error"}`,
      );
      return false;
    } finally {
      setRemoving(false);
    }
  };
  const openerFor = (item: LibraryItem) => {
    const id = localId(item);
    if (id == null) return undefined;
    return () => setDetail({ id, listing: isLocal(item) ? undefined : item });
  };
  let index = 0;
  const render = (item: LibraryItem) => (
    <Card
      key={`${item.id ?? displayTitle(item)}:${index}`}
      item={item}
      index={index++}
      category={category}
      remove={(item) => void remove(item)}
      removing={removing}
      open={openerFor(item)}
    />
  );
  const start = Math.max(1, Math.min(page - 3, pages - 6)),
    end = Math.min(pages, start + 6),
    numbers = Array.from({ length: end - start + 1 }, (_, i) => start + i);
  return (
    <section id="tab-library" className="tab-content active">
      <div className="library-toolbar">
        <div className="library-categories" role="group" aria-label="Library category">
          {(["ebooks", "audiobooks", "manga"] as const).map((value) => (
            <button
              key={value}
              type="button"
              className="library-category"
              data-library-tab={value}
              aria-pressed={category === value}
              onClick={() => {
                if (category === value) return;
                setCategory(value);
                setItems([]);
                setPage(1);
              }}
            >
              {t(`tab_${value}`)}
            </button>
          ))}
        </div>
        <input
          id="library-search"
          aria-label={t("library_filter_placeholder")}
          placeholder={t("library_filter_placeholder")}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
      </div>
      {loading && <p aria-busy="true">Loading…</p>}{" "}
      {!loading && items.length === 0 && (
        <div id="library-empty">
          <h2>{t("library_empty")}</h2>
          <p>{t("library_empty_hint")}</p>
        </div>
      )}
      <div
        id="library-results"
        className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3"
      >
        {!loading && grouped.groups.map(([series, group]) => (
          <section data-series-group={series} className="contents" key={series}>
            {group.length > 1 && (
              <header className="col-span-full">
                <h3>{series}</h3>
                <span>{t("n_items", { n: group.length })}</span>
              </header>
            )}
            {group.map(render)}
          </section>
        ))}
        {!loading && grouped.standalone.length > 0 && grouped.groups.length > 0 && (
          <h3 className="col-span-full">{t("other")}</h3>
        )}
        {!loading && grouped.standalone.map(render)}
      </div>
      {detail && (
        <ItemDetail
          key={detail.id}
          id={detail.id}
          listing={detail.listing}
          onClose={() => setDetail(null)}
          remove={remove}
          removing={removing}
        />
      )}
      {pages > 1 && (
        <nav id="library-pagination" aria-label="Library pages">
          <button
            data-action="goLibraryPage"
            disabled={page <= 1}
            onClick={() => setPage(page - 1)}
          >
            {t("prev")}
          </button>
          {start > 1 && (
            <>
              <button onClick={() => setPage(1)}>1</button>
              {start > 2 && <span>…</span>}
            </>
          )}
          {numbers.map((number) => (
            <button
              key={number}
              aria-current={number === page ? "page" : undefined}
              onClick={() => setPage(number)}
            >
              {number}
            </button>
          ))}
          {end < pages && (
            <>
              {end < pages - 1 && <span>…</span>}
              <button onClick={() => setPage(pages)}>{pages}</button>
            </>
          )}
          <button
            data-action="goLibraryPage"
            disabled={page >= pages}
            onClick={() => setPage(page + 1)}
          >
            {t("next")}
          </button>
        </nav>
      )}
    </section>
  );
}
