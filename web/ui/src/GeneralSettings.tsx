import { useEffect, useState } from "react";
import { api } from "./api";
import { useTranslation } from "./i18n";
type Value = string | number | boolean | null;
type Settings = Record<string, Value>;
interface Result {
  success: boolean;
  error?: string;
}
interface Source {
  name?: string;
  label?: string;
  enabled?: boolean;
  search_tab?: string;
  download_type?: string;
}
const integrations = [
  [
    "annas",
    "Anna’s Archive",
    ["annas_archive_domain", "annas_archive_secret_key"],
  ],
  ["prowlarr", "Prowlarr", ["prowlarr_url", "prowlarr_api_key"]],
  ["qbittorrent", "qBittorrent", ["qb_url", "qb_user", "qb_pass"]],
  [
    "transmission",
    "Transmission",
    [
      "transmission_url",
      "transmission_user",
      "transmission_pass",
      "torrent_client",
    ],
  ],
  [
    "sabnzbd",
    "SABnzbd",
    ["sabnzbd_url", "sabnzbd_api_key", "sabnzbd_category"],
  ],
  ["audiobookshelf", "Audiobookshelf", ["abs_url", "abs_token"]],
  [
    "kavita",
    "Kavita",
    [
      "kavita_url",
      "kavita_user",
      "kavita_pass",
      "kavita_ebook_library_id",
      "kavita_manga_library_id",
    ],
  ],
  ["komga", "Komga", ["komga_url", "komga_user", "komga_pass"]],
  ["calibre", "Calibre", ["calibre_url", "calibre_library_path"]],
] as const;
const toggles = [
  ["scheduler_enabled", "Run the wanted scheduler"],
  ["scheduler_auto_download", "Automatically download wanted matches"],
  ["auto_upgrade_enabled", "Enable quality upgrades"],
  ["upgrade_keep_old_files", "Keep old files after upgrades"],
  ["author_monitor_enabled", "Monitor followed authors"],
] as const;
const numbers = [
  ["scheduler_interval_hours", "Scheduler interval (hours)", 1],
  ["scheduler_min_score", "Minimum match score", 0],
  ["scheduler_item_delay_seconds", "Delay between items (seconds)", 0],
] as const;
const box = "bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3";
const input =
  "block w-full min-w-0 bg-slate-800 border border-slate-700 rounded-lg px-3 py-2";
const button =
  "px-3 py-2 rounded-lg bg-indigo-600 text-white disabled:opacity-50";
export function GeneralSettings() {
  const { t } = useTranslation();
  const [data, setData] = useState<Settings>({});
  const [sources, setSources] = useState<Source[]>([]);
  const [config, setConfig] = useState<Record<string, unknown>>({});
  const [scheduler, setScheduler] = useState<{
    last_run?: string;
    last_result?: string;
  }>({});
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [tests, setTests] = useState<Record<string, string>>({});
  async function load() {
    setData(await api<Settings>("/api/settings"));
  }
  useEffect(() => {
    void load().catch((e) => setError(String(e)));
    void api<Source[] | { sources: Source[] }>("/api/sources")
      .then((d) => setSources(Array.isArray(d) ? d : d.sources || []))
      .catch((e) => setError(String(e)));
    void api<Record<string, unknown>>("/api/config")
      .then(setConfig)
      .catch((e) => setError(String(e)));
    void api<{ status: { last_run?: string; last_result?: string } }>(
      "/api/scheduler/status",
    )
      .then((d) => setScheduler(d.status || {}))
      .catch((e) => setError(String(e)));
  }, []);
  function edit(key: string, value: Value) {
    setData((old) => ({ ...old, [key]: value }));
  }
  async function save(name: string, payload: Settings, reload = false) {
    setBusy(name);
    setError("");
    setNotice("");
    try {
      const result = await api<Result>("/api/settings", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      if (!result.success)
        throw new Error(result.error || "Failed to save settings");
      if (reload) {
        const fresh = await api<Settings>("/api/settings");
        setData((old) => ({
          ...old,
          ...Object.fromEntries(
            Object.keys(payload).map((key) => [
              key,
              fresh[key] ?? payload[key],
            ]),
          ),
          effective_import_mode: fresh.effective_import_mode ?? null,
        }));
      }
      setNotice(
        name === "integration"
          ? "Saved. Restart the container for new URLs/credentials to take effect."
          : t("settings_saved"),
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  }
  async function test(name: string) {
    setTests((old) => ({ ...old, [name]: t("testing") }));
    try {
      const result = await api<Result>(`/api/test/${name}`, { method: "POST" });
      setTests((old) => ({
        ...old,
        [name]: result.success
          ? t("connected")
          : result.error || t("conn_error"),
      }));
    } catch (e) {
      setTests((old) => ({
        ...old,
        [name]: e instanceof Error ? e.message : String(e),
      }));
    }
  }
  const effective = String(data.effective_import_mode || "");
  const descriptions: Record<string, string> = {
    move: "Imports move files into the library — a kept torrent could not seed.",
    hardlink:
      "Imports hardlink into the library — kept torrents keep seeding, at no extra disk.",
    copy: "Imports copy into the library — kept torrents keep seeding, using twice the disk.",
  };
  return (
    <div className="space-y-4">
      {error && (
        <p role="alert" className="text-red-400">
          {error}
        </p>
      )}
      {notice && (
        <p role="status" className="text-emerald-400">
          {notice}
        </p>
      )}
      <section className={box}>
        <h3 className="text-lg font-semibold">Downloads and imports</h3>
        <label className="block">
          Import mode
          <select
            id="setting-import_mode"
            className={input}
            value={String(data.import_mode ?? "")}
            disabled={!!busy}
            onChange={(e) =>
              void save("import", { import_mode: e.target.value }, true)
            }
          >
            <option value="">Automatic</option>
            <option value="move">Move</option>
            <option value="hardlink">Hardlink</option>
            <option value="copy">Copy</option>
          </select>
        </label>
        <p
          id="import-mode-effective"
          className={
            effective === "move" && data.remove_torrent_after_import === false
              ? "text-amber-400"
              : "text-indigo-300"
          }
        >
          {data.import_mode === "" && effective
            ? `Automatic → ${effective}. `
            : ""}
          {descriptions[effective]}
          {effective === "move" && data.remove_torrent_after_import === false
            ? " Pick Hardlink above, or turn the setting below back on."
            : ""}
        </p>
        <label className="block">
          <input
            id="remove-torrent-toggle"
            type="checkbox"
            checked={data.remove_torrent_after_import === true}
            disabled={!!busy}
            onChange={(e) =>
              void save(
                "remove",
                { remove_torrent_after_import: e.target.checked },
                true,
              )
            }
          />{" "}
          Remove torrents after import
        </label>
        <label className="block">
          <input
            id="foreign-lang-filter-toggle"
            type="checkbox"
            checked={data.foreign_lang_filter === true}
            disabled={!!busy}
            onChange={(e) =>
              void save(
                "filter",
                { foreign_lang_filter: e.target.checked },
                true,
              )
            }
          />{" "}
          {t("filter_non_english_desc")}
        </label>
      </section>
      <section id="wanted-settings" className={box}>
        <h3 className="text-lg font-semibold">{t("s_wanted_title")}</h3>
        {toggles.map(([key, label]) => (
          <label key={key} className="block">
            <input
              id={`setting-${key}`}
              type="checkbox"
              checked={data[key] === true}
              onChange={(e) => edit(key, e.target.checked)}
            />{" "}
            {label}
          </label>
        ))}
        <div className="grid gap-3 sm:grid-cols-3">
          {numbers.map(([key, label, min]) => (
            <label key={key}>
              {label}
              <input
                id={`setting-${key}`}
                className={input}
                type="number"
                min={min}
                value={Number(data[key] ?? min)}
                onChange={(e) => edit(key, Number(e.target.value))}
              />
            </label>
          ))}
        </div>
        <button
          disabled={!!busy}
          data-action="saveWantedSettings"
          className={button}
          onClick={() =>
            void save(
              "wanted",
              Object.fromEntries(
                [...toggles, ...numbers].map(([key]) => [
                  key,
                  data[key] ??
                    (toggles.some(([toggle]) => toggle === key) ? false : 0),
                ]),
              ),
            )
          }
        >
          Save
        </button>
        <p id="scheduler-status" className="text-sm text-slate-400">
          {scheduler.last_run
            ? `${new Date(scheduler.last_run).toLocaleString()} — ${scheduler.last_result || ""}`
            : ""}
        </p>
      </section>
      <section className={box}>
        <h3 className="text-lg font-semibold">Integrations</h3>
        <div className="grid gap-4 md:grid-cols-2">
          {integrations.map(([name, label, fields]) => (
            <form
              key={name}
              className="border border-slate-700 rounded-lg p-4 space-y-3 min-w-0"
              onSubmit={(e) => {
                e.preventDefault();
                void save(
                  "integration",
                  Object.fromEntries(
                    fields
                      .filter((key) => data[key] !== "--------")
                      .map((key) => [key, data[key] ?? ""]),
                  ),
                );
              }}
            >
              <h4 className="font-semibold">{label}</h4>
              {fields.map((key) => (
                <label key={key} className="block text-sm">
                  {key.replaceAll("_", " ")}
                  {key === "torrent_client" ? (
                    <select
                      id={`setting-${key}`}
                      className={input}
                      value={String(data[key] ?? "qbittorrent")}
                      onChange={(e) => edit(key, e.target.value)}
                    >
                      <option value="qbittorrent">qBittorrent</option>
                      <option value="transmission">Transmission</option>
                    </select>
                  ) : (
                    <input
                      id={`setting-${key}`}
                      className={input}
                      type={
                        /pass|token|secret|api_key/.test(key)
                          ? "password"
                          : "text"
                      }
                      autoComplete="off"
                      value={String(data[key] ?? "")}
                      onChange={(e) => edit(key, e.target.value)}
                    />
                  )}
                </label>
              ))}
              <div className="flex gap-2">
                <button
                  disabled={!!busy}
                  className={button}
                  data-action="saveIntegration"
                  data-arg={name}
                >
                  Save
                </button>
                {!["annas", "komga", "calibre"].includes(name) && (
                  <button
                    type="button"
                    className={button}
                    data-action="testConnection"
                    data-arg={name}
                    onClick={() => void test(name)}
                  >
                    Test connection
                  </button>
                )}
              </div>
              <p role="status" id={`test-${name}-status`}>
                {tests[name]}
              </p>
            </form>
          ))}
        </div>
      </section>
      <section className={box}>
        <h3 className="text-lg font-semibold">Sources</h3>
        <div id="sources-list" className="space-y-2">
          {sources.map((source, index) => (
            <div
              key={source.name || index}
              className="flex gap-2 justify-between"
            >
              <span>
                {source.label || source.name}{" "}
                <small>{source.search_tab || source.download_type}</small>
              </span>
              <span>
                {source.enabled === false ? t("disabled") : t("enabled")}
              </span>
            </div>
          ))}
        </div>
      </section>
      <section className={box}>
        <h3 className="text-lg font-semibold">Configuration</h3>
        <div id="config-info">
          {Object.entries(config)
            .filter(
              ([key, value]) =>
                (key.endsWith("_url") && typeof value === "string") ||
                (typeof value === "object" && value !== null && "url" in value),
            )
            .map(([key, value]) => (
              <p className="flex flex-wrap justify-between gap-2" key={key}>
                <span>{key.replaceAll("_", " ")}</span>
                <span className="text-slate-400 break-all">
                  {typeof value === "string"
                    ? value
                    : String(
                        (value as { url: unknown }).url || t("not_configured"),
                      )}
                </span>
              </p>
            ))}
        </div>
      </section>
    </div>
  );
}
