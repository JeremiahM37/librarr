import { useEffect, useMemo, useRef, useState } from "react";
import { api, type LibraryItem, type LibraryResponse } from "./api";
import { useTranslation } from "./i18n";

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
}: {
  item: LibraryItem;
  index: number;
}): React.JSX.Element {
  const [failed, setFailed] = useState(false);
  const title = displayTitle(item);
  if (!item.cover_url || failed)
    return (
      <div
        data-cover-fallback
        className={`w-full h-48 bg-gradient-to-br ${gradients[index % gradients.length]} flex items-center justify-center text-3xl font-bold text-white/80`}
      >
        {initials(title)}
      </div>
    );
  return (
    <img
      src={item.cover_url}
      alt=""
      className="w-full h-48 object-cover"
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
}: {
  item: LibraryItem;
  index: number;
  category: Category;
  remove: (item: LibraryItem) => void;
  removing: boolean;
}): React.JSX.Element {
  const { t } = useTranslation();
  const title = displayTitle(item);
  const format =
    item.file_format || item.format || item.file_path?.match(/\.([^./\\]+)$/)?.[1] || "";
  return (
    <article className="book-card bg-slate-900 rounded-xl overflow-hidden border border-slate-800 relative group">
      <Cover item={item} index={index} />
      {item.id != null && !item.kavita_url && (
        <button
          data-action="deleteLibraryItem"
          title="Remove from library"
          aria-label="Remove from library"
          disabled={removing}
          onClick={() => remove(item)}
          className="absolute top-2 right-2 w-11 h-11 rounded-full bg-slate-950/80 text-white hover:bg-red-700 disabled:opacity-50"
        >
          ✕
        </button>
      )}
      <div className="p-3">
        <h3 className="text-sm font-semibold text-white">{title}</h3>
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
          <a href={item.abs_url} target="_blank" rel="noreferrer">
            {t("open_in_abs")}
          </a>
        )}
        {item.kavita_url && (
          <a href={item.kavita_url} target="_blank" rel="noreferrer">
            {t("open_in_kavita")}
          </a>
        )}
      </div>
    </article>
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
    [revision, setRevision] = useState(0);
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
  const remove = async (item: LibraryItem) => {
    if (
      removing ||
      item.id == null ||
      item.kavita_url ||
      !window.confirm(`Remove "${displayTitle(item)}" from library?`)
    )
      return;
    const type = category === "audiobooks"
      ? "audiobook"
      : category === "manga" ? "manga" : "book";
    const suffix = type === "manga" ? "?source=local" : "";
    setRemoving(true);
    try {
      await api(`/api/library/${type}/${encodeURIComponent(item.id)}${suffix}`, {
        method: "DELETE",
      });
      setNotice(`Removed "${displayTitle(item)}"`);
      setRevision((value) => value + 1);
    } catch (error) {
      setNotice(
        `Failed to remove: ${error instanceof Error ? error.message : "unknown error"}`,
      );
    } finally {
      setRemoving(false);
    }
  };
  let index = 0;
  const render = (item: LibraryItem) => (
    <Card
      key={`${item.id ?? displayTitle(item)}:${index}`}
      item={item}
      index={index++}
      category={category}
      remove={remove}
      removing={removing}
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
