import { useCallback, useEffect, useState } from "react";
import { api, type DownloadJob } from "./api";
import { useTranslation, type TranslationKey } from "./i18n";

const active = new Set([
  "queued",
  "searching",
  "downloading",
  "organizing",
  "importing",
  "retry_wait",
]);
const labels: Record<string, string> = {
  queued: "Queued",
  searching: "Searching",
  downloading: "Downloading",
  organizing: "Organizing",
  importing: "Importing",
  retry_wait: "Retry Wait",
  completed: "Completed",
  error: "Error",
  dead_letter: "Dead Letter",
};

export function Downloads({
  setNotice,
}: {
  setNotice: (message: string) => void;
}): React.JSX.Element {
  const { t } = useTranslation();
  const [jobs, setJobs] = useState<DownloadJob[]>([]),
    [refreshing, setRefreshing] = useState(false),
    [clearing, setClearing] = useState(false),
    [pending, setPending] = useState<Set<string>>(new Set());
  const load = useCallback(
    async (manual = false) => {
      if (manual) setRefreshing(true);
      try {
        const data = await api<{
          downloads?: DownloadJob[];
          jobs?: DownloadJob[];
        }>("/api/downloads");
        setJobs(data.downloads ?? data.jobs ?? []);
      } catch (error) {
        setNotice(
          error instanceof Error ? error.message : t("failed_load_downloads"),
        );
      } finally {
        if (manual) setRefreshing(false);
      }
    },
    [setNotice],
  );
  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [load]);
  const retry = async (job: DownloadJob) => {
    const id = job.job_id;
    if (!id || pending.has(id)) return;
    setPending((old) => new Set(old).add(id));
    try {
      await api(`/api/downloads/jobs/${id}/retry`, { method: "POST" });
      setNotice(t("retrying_download"));
      await load();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : t("retry_failed"));
    } finally {
      setPending((old) => {
        const next = new Set(old);
        next.delete(id);
        return next;
      });
    }
  };
  const cancel = async (job: DownloadJob) => {
    const path = job.hash
      ? `/api/downloads/torrent/${job.hash}`
      : job.job_id
        ? `/api/downloads/novel/${job.job_id}`
        : "";
    if (!path || !window.confirm(`Cancel "${job.title || "Unknown"}"?`)) return;
    try {
      await api(path, { method: "DELETE" });
      setNotice("Download cancelled");
      await load();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Cancel failed");
    }
  };
  const clear = async () => {
    setClearing(true);
    try {
      await api("/api/downloads/clear", { method: "POST" });
      setNotice(t("cleared_completed"));
      await load();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : t("failed_clear"));
    } finally {
      setClearing(false);
    }
  };
  return (
    <section id="tab-downloads" className="tab-content active">
      <div className="flex justify-between">
        <h2>
          {t("downloads_title")}{" "}
          <span id="dl-badge">
            {jobs.filter((job) => active.has(job.status)).length || ""}
          </span>
        </h2>
        <div>
          <button
            id="downloads-refresh-btn"
            data-action="refreshDownloads"
            disabled={refreshing}
            aria-busy={refreshing || undefined}
            onClick={() => void load(true)}
          >
            {t("refresh")}
          </button>
          <button
            id="downloads-clear-btn"
            data-action="clearCompleted"
            disabled={clearing}
            aria-busy={clearing || undefined}
            onClick={() => void clear()}
          >
            {t("clear_completed")}
          </button>
        </div>
      </div>
      {jobs.length === 0 && (
        <div id="downloads-empty">
          <h2>{t("no_active_downloads")}</h2>
          <p>{t("no_downloads_hint")}</p>
        </div>
      )}
      <div id="downloads-list" className="space-y-3">
        {jobs.map((job, index) => {
          const progress = job.progress ?? 0,
            id = job.job_id ?? job.hash ?? String(index),
            retrying = pending.has(job.job_id ?? "");
          return (
            <article
              data-download-id={id}
              key={id}
              className="bg-slate-900 border rounded-xl p-4"
            >
              <span data-download-status={job.status}>
                {job.status in labels
                  ? t(`status_${job.status}` as TranslationKey)
                  : job.status}
              </span>
              {job.source && <span>{job.source}</span>}
              <h4 title={job.title ?? ""}>{job.title || "Unknown"}</h4>
              {job.detail && <p title={job.detail}>{job.detail}</p>}
              {(job.max_retries ?? 0) > 0 && (job.retry_count ?? 0) > 0 && (
                <p>
                  Attempt{" "}
                  {Math.min(
                    (job.retry_count ?? 0) + 1,
                    (job.max_retries ?? 0) + 1,
                  )}
                  /{(job.max_retries ?? 0) + 1}
                </p>
              )}
              {job.error && <p className="text-red-400">{job.error}</p>}
              {job.status === "downloading" && progress > 0 && (
                <>
                  <div
                    role="progressbar"
                    aria-valuenow={progress}
                    aria-valuemin={0}
                    aria-valuemax={100}
                  >
                    <div
                      className="progress-bar"
                      style={{ width: `${progress}%` }}
                    />
                  </div>
                  <span>{progress.toFixed(1)}%</span>
                </>
              )}
              {job.size && <span>{job.size}</span>}
              {job.speed && <span>{job.speed}</span>}
              {(job.status === "error" || job.status === "dead_letter") && (
                <button
                  data-action="retryDownload"
                  disabled={retrying}
                  aria-busy={retrying || undefined}
                  onClick={() => void retry(job)}
                >
                  {retrying ? "Loading…" : t("retry")}
                </button>
              )}
              {active.has(job.status) && (job.hash || job.job_id) && (
                <button
                  data-action="cancelDownload"
                  onClick={() => void cancel(job)}
                >
                  {t("cancel")}
                </button>
              )}
            </article>
          );
        })}
      </div>
    </section>
  );
}
