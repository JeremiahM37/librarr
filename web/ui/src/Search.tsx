import { useEffect, useMemo, useRef, useState } from "react";
import {
  api,
  request,
  type DownloadJob,
  type SearchResult,
  type MediaType,
} from "./api";
import { useTranslation } from "./i18n";
type Sort = "relevance" | "seeders" | "size";
interface Outcome {
  state: "loading" | "success" | "error";
  detail?: string;
  jobId?: string;
  manualURL?: string;
}
interface DownloadResult {
  success?: boolean;
  job_id?: string;
  error?: string;
  in_library?: boolean;
  library_title?: string;
  library_item_id?: number;
}
export interface SearchIntent {
  query: string;
  media: MediaType;
  sequence: number;
}
const endpoints: Record<MediaType, string> = {
  ebook: "/api/search",
  audiobook: "/api/search/audiobooks",
  manga: "/api/search/manga",
};
function keyOf(result: SearchResult) {
  return [
    result.source,
    result.download_url,
    result.url,
    result.abb_url,
    result.info_hash,
    result.magnet,
    result.md5,
    result.source_id,
    result.title,
    result.author,
  ].join("|");
}
function size(value?: number | string) {
  if (typeof value === "number") return value;
  const match = value?.toUpperCase().match(/([\d.]+)\s*(GB|MB|KB|B)?/);
  return match
    ? Number(match[1]) *
        ({ B: 1, KB: 1024, MB: 1048576, GB: 1073741824 }[match[2] as "B"] || 1)
    : 0;
}
function formatSize(value?: number | string) {
  if (typeof value === "string") return value;
  if (!value) return "";
  const units = ["B", "KB", "MB", "GB"];
  let n = value,
    i = 0;
  while (n >= 1024 && i < 3) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
}
function Cover({ result }: { result: SearchResult }) {
  const [failed, setFailed] = useState(false);
  return result.cover_url && !failed ? (
    <img
      src={result.cover_url}
      className="w-full h-48 object-cover"
      alt=""
      loading="lazy"
      onError={() => setFailed(true)}
    />
  ) : (
    <div className="cover-placeholder w-full h-48 bg-gradient-to-br from-indigo-600 to-purple-700">
      {result.title.charAt(0).toUpperCase()}
    </div>
  );
}
export function Search({
  setNotice,
  intent,
  active = true,
}: {
  active?: boolean;
  setNotice: (message: string) => void;
  intent?: SearchIntent;
}) {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [media, setMedia] = useState<MediaType>("ebook");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [sort, setSort] = useState<Sort>("relevance");
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [error, setError] = useState("");
  const [outcomes, setOutcomes] = useState<Record<string, Outcome>>({});
  const abort = useRef<AbortController | null>(null);
  const generation = useRef(0);
  const pending = useRef(new Set<string>());
  const rendered = useMemo(
    () =>
      [...results].sort((a, b) =>
        sort === "seeders"
          ? (b.seeders || 0) - (a.seeders || 0)
          : sort === "size"
            ? size(b.size || b.size_human) - size(a.size || a.size_human)
            : 0,
      ),
    [results, sort],
  );
  const tracked = Object.entries(outcomes)
    .filter(([, outcome]) => outcome.state === "loading" && outcome.jobId)
    .map(([key, outcome]) => [key, outcome.jobId!] as const);
  const trackingKey = JSON.stringify(tracked);
  useEffect(() => {
    if (!tracked.length) return;
    let dead = false;
    let controller: AbortController | undefined;
    let inFlight = false;
    const pairs: readonly (readonly [string, string])[] =
      JSON.parse(trackingKey);
    async function poll() {
      if (dead || document.hidden || inFlight) return;
      inFlight = true;
      controller = new AbortController();
      try {
        const data = await api<{ downloads?: DownloadJob[] }>(
          "/api/downloads",
          { signal: controller.signal },
        );
        if (dead) return;
        setOutcomes((old) => {
          const next = { ...old };
          for (const [key, id] of pairs) {
            const job = data.downloads?.find(
              (job) => String(job.job_id) === id,
            );
            if (!job) continue;
            if (job.status === "completed")
              next[key] = { state: "success", detail: job.detail };
            else if (
              ["error", "dead_letter", "interrupted"].includes(job.status)
            )
              next[key] = {
                state: "error",
                detail: job.error || job.detail,
                manualURL: old[key]?.manualURL,
              };
            else next[key] = { ...old[key]!, detail: job.detail };
          }
          return next;
        });
      } catch (e) {
        if (!controller.signal.aborted)
          setError(e instanceof Error ? e.message : String(e));
      } finally {
        inFlight = false;
      }
    }
    void poll();
    const interval = window.setInterval(() => void poll(), 5000);
    document.addEventListener("visibilitychange", poll);
    return () => {
      dead = true;
      controller?.abort();
      window.clearInterval(interval);
      document.removeEventListener("visibilitychange", poll);
    };
  }, [trackingKey]);
  const completedKeys = JSON.stringify(
    Object.entries(outcomes)
      .filter(([, value]) => value.state === "success")
      .map(([key]) => key),
  );
  useEffect(() => {
    const keys: string[] = JSON.parse(completedKeys);
    if (!keys.length) return;
    const timer = window.setTimeout(
      () =>
        setOutcomes((old) => {
          const next = { ...old };
          for (const key of keys)
            if (next[key]?.state === "success") delete next[key];
          return next;
        }),
      2500,
    );
    return () => window.clearTimeout(timer);
  }, [completedKeys]);
  useEffect(() => () => abort.current?.abort(), []);
  async function search(value: string, kind: MediaType) {
    if (!value.trim()) return;
    abort.current?.abort();
    const controller = new AbortController();
    abort.current = controller;
    const id = ++generation.current;
    setLoading(true);
    setSearched(true);
    setResults([]);
    setError("");
    const endpoint = endpoints[kind];
    const apply = (rows: SearchResult[]) => {
      if (id === generation.current && !controller.signal.aborted)
        setResults(rows);
    };
    try {
      const response = await request(
        `${endpoint}/stream?q=${encodeURIComponent(value)}`,
        { signal: controller.signal, headers: { Accept: "text/event-stream" } },
      );
      if (
        !response.ok ||
        !response.body ||
        !response.headers.get("content-type")?.includes("text/event-stream")
      )
        throw new Error("Streaming search unavailable");
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      const frame = (text: string) => {
        let event = "message";
        const lines: string[] = [];
        for (const line of text.split("\n")) {
          if (line.startsWith("event:")) event = line.slice(6).trim();
          if (line.startsWith("data:")) lines.push(line.slice(5).trimStart());
        }
        if (event === "results" || event === "complete") {
          try {
            const data = JSON.parse(lines.join("\n")) as {
              results?: SearchResult[];
            };
            apply(data.results || []);
          } catch {
            /* A malformed partial source event must not discard other results. */
          }
        }
      };
      try {
        while (true) {
          const chunk = await reader.read();
          buffer = (
            buffer +
            (chunk.done
              ? decoder.decode()
              : decoder.decode(chunk.value, { stream: true }))
          ).replaceAll("\r\n", "\n");
          let boundary;
          while ((boundary = buffer.indexOf("\n\n")) !== -1) {
            frame(buffer.slice(0, boundary));
            buffer = buffer.slice(boundary + 2);
          }
          if (chunk.done) {
            if (buffer.trim()) frame(buffer);
            break;
          }
        }
      } finally {
        reader.releaseLock();
      }
    } catch (e) {
      if (controller.signal.aborted) return;
      try {
        const data = await api<{ results?: SearchResult[] }>(
          `${endpoint}?q=${encodeURIComponent(value)}`,
          { signal: controller.signal },
        );
        apply(data.results || []);
      } catch (failure) {
        if (!controller.signal.aborted && id === generation.current)
          setError(
            t("search_failed", {
              msg: failure instanceof Error ? failure.message : String(failure),
            }),
          );
      }
    } finally {
      if (id === generation.current && !controller.signal.aborted)
        setLoading(false);
    }
  }
  useEffect(() => {
    if (intent) {
      setQuery(intent.query);
      setMedia(intent.media);
      void search(intent.query, intent.media);
    }
  }, [intent?.sequence]);
  async function download(result: SearchResult) {
    const key = keyOf(result);
    if (pending.current.has(key)) return;
    pending.current.add(key);
    setOutcomes((old) => ({ ...old, [key]: { state: "loading" } }));
    try {
      const response = await request("/api/download", {
        method: "POST",
        body: JSON.stringify({
          title: result.title,
          download_url:
            result.download_url || result.url || result.epub_url || "",
          abb_url: result.abb_url || "",
          source: result.source,
          source_id: result.source_id || "",
          md5: result.md5 || "",
          author: result.author || "",
          info_hash: result.info_hash || "",
          magnet: result.magnet || "",
          media_type: result.media_type || "",
          download_protocol: result.download_protocol || "",
          force: !!result.in_library,
        }),
      });
      const data = (await response.json()) as DownloadResult;
      if (response.status === 409 && data.in_library) {
        setResults((old) =>
          old.map((row) =>
            keyOf(row) === key
              ? {
                  ...row,
                  in_library: true,
                  library_title: data.library_title,
                  library_item_id: data.library_item_id,
                }
              : row,
          ),
        );
        setOutcomes((old) => {
          const next = { ...old };
          delete next[key];
          return next;
        });
        setNotice(
          t("already_in_library", {
            title: data.library_title || result.title,
          }),
        );
        return;
      }
      if (!response.ok || (!data.success && !data.job_id))
        throw new Error(data.error || `API error: ${response.status}`);
      setOutcomes((old) => ({
        ...old,
        [key]: {
          state: data.job_id ? "loading" : "success",
          jobId: data.job_id ? String(data.job_id) : undefined,
          manualURL:
            result.source === "annas"
              ? result.download_url || result.url
              : undefined,
        },
      }));
      setNotice(t("download_started", { title: result.title }));
    } catch (e) {
      const detail = e instanceof Error ? e.message : String(e);
      setOutcomes((old) => ({
        ...old,
        [key]: {
          state: "error",
          detail,
          manualURL:
            result.source === "annas"
              ? result.download_url || result.url
              : undefined,
        },
      }));
      setNotice(t("download_failed", { msg: detail }));
    } finally {
      pending.current.delete(key);
    }
  }
  return (
    <section
      id="tab-search"
      hidden={!active}
      className={`tab-content ${active ? "active" : ""} space-y-4`}
    >
      <div className="flex flex-wrap gap-2">
        {(["ebook", "audiobook", "manga"] as const).map((kind) => (
          <button
            key={kind}
            data-action="switchSearchTab"
            data-arg={kind === "manga" ? "manga" : `${kind}s`}
            aria-pressed={media === kind}
            className={`px-4 py-2 rounded-lg ${media === kind ? "bg-indigo-600" : "bg-slate-800"}`}
            onClick={() => {
              setMedia(kind);
              if (query.trim()) void search(query, kind);
            }}
          >
            {t(
              kind === "ebook"
                ? "tab_ebooks"
                : kind === "audiobook"
                  ? "tab_audiobooks"
                  : "tab_manga",
            )}
          </button>
        ))}
      </div>
      <form
        className="flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          void search(query, media);
        }}
      >
        <input
          id="search-input"
          className="min-w-0 flex-1 bg-slate-900 border border-slate-700 rounded-xl px-4 py-3"
          aria-label={t("nav_search")}
          placeholder={t(
            media === "ebook"
              ? "search_placeholder"
              : media === "audiobook"
                ? "search_placeholder_ab"
                : "search_placeholder_manga",
          )}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button className="px-4 py-3 rounded-xl bg-indigo-600">
          {t("nav_search")}
        </button>
      </form>
      {error && (
        <p role="alert" className="text-red-400">
          {error}
        </p>
      )}
      {loading && (
        <p id="search-spinner" role="status">
          Searching…
        </p>
      )}
      {!searched && (
        <div id="search-empty" className="p-12 text-center">
          <h2>{t("search_empty_title")}</h2>
          <p className="text-slate-400">{t("search_empty_hint")}</p>
        </div>
      )}
      {searched && !loading && !results.length && !error && (
        <div id="search-no-results" className="p-12 text-center">
          <h2>{t("no_results")}</h2>
          <p>{t("no_results_hint")}</p>
        </div>
      )}
      {results.length > 0 && (
        <div id="search-sort-bar" className="flex flex-wrap gap-2 items-center">
          <span id="search-result-count">
            {t("n_results", { n: results.length })}
          </span>
          {(["relevance", "seeders", "size"] as const).map((mode) => (
            <button
              key={mode}
              className={`px-3 py-1 rounded ${sort === mode ? "bg-indigo-600" : "bg-slate-800"}`}
              data-action="setSortMode"
              data-arg={mode}
              onClick={() => setSort(mode)}
            >
              {t(`sort_${mode}`)}
            </button>
          ))}
        </div>
      )}
      <div
        id="search-results"
        className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 xl:grid-cols-6 gap-4"
      >
        {rendered.map((result, index) => {
          const outcome = outcomes[keyOf(result)];
          return (
            <article
              className="book-card bg-slate-900 border border-slate-800 rounded-xl overflow-hidden flex flex-col"
              key={`${keyOf(result)}:${index}`}
            >
              <div className="relative">
                <Cover result={result} />
                <span className="absolute top-2 left-2 bg-slate-800 text-white text-xs px-2 py-1 rounded">
                  {result.source}
                </span>
                {result.in_library && (
                  <span
                    className="result-in-library absolute top-2 right-2 bg-emerald-600 text-white px-2 py-1 rounded text-xs"
                    title={t("in_library_title", {
                      title: result.library_title || result.title,
                    })}
                  >
                    {t("in_library")}
                  </span>
                )}
              </div>
              <div className="p-3 flex-1 flex flex-col gap-2">
                <h3
                  title={result.title}
                  className="text-sm font-semibold line-clamp-2"
                >
                  {result.title}
                </h3>
                <p className="text-xs text-slate-400">{result.author}</p>
                <div className="mt-auto flex flex-wrap gap-2 text-xs text-slate-400">
                  {!!result.seeders && (
                    <span>{t("n_seeds", { n: result.seeders })}</span>
                  )}
                  {!!result.leechers && (
                    <span>{t("n_leech", { n: result.leechers })}</span>
                  )}
                  <span>
                    {formatSize(result.size) ||
                      result.size_human ||
                      result.sizeHuman}
                  </span>
                  <span>{result.format}</span>
                  {result.language && (
                    <span className="result-language uppercase text-sky-400">
                      {result.language}
                    </span>
                  )}
                  {result.year && (
                    <span className="result-year">{result.year}</span>
                  )}
                  {result.publisher && (
                    <span
                      className="result-publisher truncate"
                      title={result.publisher}
                    >
                      {result.publisher}
                    </span>
                  )}
                  {result.indexer && <span>{result.indexer}</span>}
                  {(result.copies || 0) > 1 && (
                    <span
                      className="result-copies"
                      title={t("n_copies_title", { n: result.copies! })}
                    >
                      {t("n_copies", { n: result.copies! })}
                    </span>
                  )}
                </div>
                <button
                  data-action="startDownload"
                  data-idx={index}
                  className={`w-full rounded-lg px-2 py-2 text-sm ${outcome?.state === "error" ? "bg-red-700" : outcome?.state === "success" ? "bg-emerald-700" : result.in_library ? "bg-amber-600" : "bg-indigo-600"}`}
                  disabled={
                    outcome?.state === "loading" || outcome?.state === "success"
                  }
                  aria-busy={outcome?.state === "loading"}
                  onClick={() => void download(result)}
                >
                  {outcome?.state === "loading"
                    ? outcome.detail || "Loading…"
                    : outcome?.state === "success"
                      ? t("download_added")
                      : outcome?.state === "error"
                        ? t("download_failed_state")
                        : result.in_library
                          ? t("download_anyway")
                          : t("download")}
                </button>
                {outcome?.state === "error" && (
                  <p className="text-xs text-red-300">
                    {outcome.detail}
                    {outcome.manualURL &&
                      /^https?:\/\//.test(outcome.manualURL) && (
                        <a
                          className="block underline"
                          href={outcome.manualURL}
                          target="_blank"
                          rel="noreferrer"
                        >
                          {t("download_failed_anna_no_match_action")}
                        </a>
                      )}
                  </p>
                )}
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}
