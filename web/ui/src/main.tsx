import { FormEvent, StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  api,
  authStatus,
  setUnauthorizedHandler,
  type Author,
  type AuthorCheckResult,
  type AuthStatus,
  type MediaType,
  type Profile,
  type WantedOutcome,
  type WishlistItem,
} from "./api";
import { text } from "./i18n/wanted";
import { Library } from "./Library";
import { Downloads } from "./Downloads";
import { AuthSettings } from "./AuthSettings";
import { AccountSettings } from "./AccountSettings";
import { GeneralSettings } from "./GeneralSettings";
import { useTranslation } from "./i18n";
import { Search, type SearchIntent } from "./Search";
import "./style.css";

type Tab = "search" | "library" | "downloads" | "wishlist" | "settings";
type AuthView = "login" | "register" | "totp";
const tabs: readonly Tab[] = [
  "search",
  "library",
  "downloads",
  "wishlist",
  "settings",
];

interface LoginResponse {
  success?: boolean;
  username?: string;
  role?: "admin" | "user";
  needs_totp?: boolean;
  session_pending?: string;
  backup_code_used?: boolean;
  error?: string;
}
interface RegisterResponse {
  success?: boolean;
  token?: string;
  username?: string;
  role?: "admin" | "user";
  error?: string;
}

function App(): React.JSX.Element {
  const { t, locale, setLanguage } = useTranslation();
  const [auth, setAuth] = useState<AuthStatus | null>(null);
  const [tab, setTab] = useState<Tab>("search");
  const [mobileNav, setMobileNav] = useState(false);
  const [navVersion, setNavVersion] = useState(0);
  const [searchIntent, setSearchIntent] = useState<SearchIntent>();
  useEffect(() => {
    function keys(e: KeyboardEvent) {
      if ((e.ctrlKey || e.metaKey) && e.key === "k") {
        e.preventDefault();
        setTab("search");
        requestAnimationFrame(() =>
          document.getElementById("search-input")?.focus(),
        );
      }
    }
    window.addEventListener("keydown", keys);
    return () => window.removeEventListener("keydown", keys);
  }, []);
  const [notice, setNotice] = useState("");
  const refreshAuth = async (): Promise<void> =>
    setAuth(await authStatus());
  useEffect(() => {
    void refreshAuth().catch((e: unknown) =>
      setNotice(e instanceof Error ? e.message : "Unable to reach Librarr"),
    );
  }, []);
  useEffect(() => {
    setUnauthorizedHandler(() =>
      setAuth((previous) => ({
        ...previous,
        has_users: previous?.has_users ?? true,
        oidc_enabled: previous?.oidc_enabled ?? false,
        authenticated: false,
      })),
    );
    return () => setUnauthorizedHandler(undefined);
  }, []);
  if (auth === null)
    return (
      <main className="p-8 text-slate-200" aria-busy="true">
        {notice ? (
          <>
            <p role="alert">{notice}</p>
            <button
              onClick={() => {
                setNotice("");
                void refreshAuth().catch((e) => setNotice(String(e)));
              }}
            >
              Retry
            </button>
          </>
        ) : (
          "Loading Librarr…"
        )}
      </main>
    );
  if (!auth.authenticated)
    return (
      <AuthScreen
        auth={auth}
        onAuthenticated={refreshAuth}
        notice={notice}
        setNotice={setNotice}
      />
    );
  return (
    <div
      id="app"
      className="min-h-screen bg-slate-950 text-slate-200 font-sans"
    >
      <Toast text={notice} />
      <header className="relative bg-slate-900 border-b border-slate-800">
        <div className="max-w-7xl mx-auto px-4 py-3 flex justify-between">
          <div className="flex items-center gap-2">
            <button
              className="hamburger-btn"
              data-action="toggleMobileNav"
              aria-label="Navigation menu"
              aria-expanded={mobileNav}
              aria-controls="main-nav"
              onClick={() => setMobileNav(!mobileNav)}
            >
              ☰
            </button>
            <h1 className="text-lg font-bold text-white">Librarr</h1>
          </div>
          <div className="flex gap-3 items-center">
            <button
              data-action="toggleLanguage"
              onClick={() => setLanguage(locale === "en" ? "ru" : "en")}
            >
              {locale === "en" ? "RU" : "EN"}
            </button>
            <span id="header-username">{auth.username}</span>
            <span id="header-role">{auth.role}</span>
            <button
              data-action="doLogout"
              onClick={() =>
                void api("/api/logout", { method: "POST" })
                  .then(() => setAuth({ ...auth, authenticated: false }))
                  .catch((e) => setNotice(String(e)))
              }
            >
              Sign out
            </button>
          </div>
        </div>
        <nav
          id="main-nav"
          aria-label="Primary navigation"
          className={`max-w-7xl mx-auto px-2 flex flex-wrap gap-1 ${mobileNav ? "mobile-open" : ""}`}
        >
          {tabs.map((item) => (
            <button
              key={item}
              data-action="switchTab"
              data-arg={item}
              data-tab={item}
              onClick={() => {
                setTab(item);
                setNavVersion((x) => x + 1);
                setMobileNav(false);
              }}
              className={`nav-tab px-2 py-2 ${tab === item ? "active" : ""}`}
            >
              {t(
                (
                  {
                    search: "nav_search",
                    library: "nav_library",
                    downloads: "nav_downloads",
                    wishlist: "nav_wishlist",
                    settings: "nav_settings",
                  } as const
                )[item],
              )}
            </button>
          ))}
        </nav>
      </header>
      <main className="max-w-7xl mx-auto p-4">
        <Search
          setNotice={setNotice}
          intent={searchIntent}
          active={tab === "search"}
        />
        {tab === "wishlist" && (
          <Wanted
            key={navVersion}
            admin={auth.role === "admin"}
            setNotice={setNotice}
            onSearch={(query, media) => {
              setSearchIntent({ query, media, sequence: Date.now() });
              setTab("search");
            }}
          />
        )}
        {tab === "settings" && (
          <Settings admin={auth.role === "admin"} setNotice={setNotice} />
        )}
        {tab === "library" && <Library setNotice={setNotice} />}
        {tab === "downloads" && <Downloads setNotice={setNotice} />}
      </main>
    </div>
  );
}

function Toast({ text }: { text: string }): React.JSX.Element {
  return (
    <div
      id="toast-container"
      aria-live="polite"
      className="fixed bottom-4 right-4 z-50"
    >
      {text && (
        <p className="bg-slate-800 border border-slate-600 rounded-lg px-4 py-3">
          {text}
        </p>
      )}
    </div>
  );
}

function AuthScreen({
  auth,
  onAuthenticated,
  notice,
  setNotice,
}: {
  auth: AuthStatus;
  onAuthenticated: () => Promise<void>;
  notice: string;
  setNotice: (message: string) => void;
}): React.JSX.Element {
  const [view, setView] = useState<AuthView>(
    auth.has_users ? "login" : "register",
  );
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [invite, setInvite] = useState("");
  const [code, setCode] = useState("");
  const [pending, setPending] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(
    () => setView(auth.has_users ? "login" : "register"),
    [auth.has_users],
  );
  async function login(event: FormEvent): Promise<void> {
    event.preventDefault();
    if (!username.trim() || !password)
      return setNotice("Username and password are required");
    setBusy(true);
    try {
      const result = await api<LoginResponse>("/api/login", {
        method: "POST",
        body: JSON.stringify({ username: username.trim(), password }),
      });
      if (!result.success)
        return setNotice(result.error ?? "Invalid credentials");
      if (result.needs_totp) {
        setPending(result.session_pending ?? "");
        setCode("");
        setView("totp");
        return;
      }
      await onAuthenticated();
      setNotice("Signed in");
    } catch (e) {
      setNotice(e instanceof Error ? e.message : "Connection failed");
    } finally {
      setBusy(false);
    }
  }
  async function register(event: FormEvent): Promise<void> {
    event.preventDefault();
    if (!username.trim() || !password)
      return setNotice("Username and password are required");
    setBusy(true);
    try {
      const result = await api<RegisterResponse>("/api/register", {
        method: "POST",
        body: JSON.stringify({
          username: username.trim(),
          password,
          ...(invite.trim() ? { invite_code: invite.trim() } : {}),
        }),
      });
      if (!result.success)
        return setNotice(result.error ?? "Registration failed");
      if (result.token) {
        await onAuthenticated();
        setNotice("Administrator account created");
      } else {
        setPassword("");
        setInvite("");
        setView("login");
        setNotice("Account created. Sign in to continue.");
      }
    } catch (e) {
      setNotice(e instanceof Error ? e.message : "Connection failed");
    } finally {
      setBusy(false);
    }
  }
  async function verifyTOTP(event: FormEvent): Promise<void> {
    event.preventDefault();
    if (!code.trim())
      return setNotice("Enter your authenticator or backup code");
    setBusy(true);
    try {
      const result = await api<LoginResponse>("/api/login/totp", {
        method: "POST",
        body: JSON.stringify({ session_pending: pending, code: code.trim() }),
      });
      if (!result.success) return setNotice(result.error ?? "Invalid code");
      await onAuthenticated();
      setNotice(
        result.backup_code_used
          ? "Signed in; a backup code was used."
          : "Signed in",
      );
    } catch (e) {
      setNotice(e instanceof Error ? e.message : "Connection failed");
    } finally {
      setBusy(false);
    }
  }
  return (
    <main
      id="login-modal"
      className="min-h-screen bg-slate-950 flex items-center justify-center p-4"
    >
      <Toast text={notice} />
      <section
        className="bg-slate-900 border border-slate-700 rounded-xl p-8 w-full max-w-md shadow-2xl"
        aria-labelledby="auth-title"
      >
        <h1 id="auth-title" className="text-xl font-bold text-white">
          Librarr
        </h1>
        <p id="login-subtitle" className="text-sm text-slate-400 mb-5">
          {view === "register"
            ? auth.has_users
              ? "Create your account"
              : "Create your administrator account"
            : view === "totp"
              ? "Two-factor authentication"
              : "Sign in to continue"}
        </p>
        {!auth.has_users && (
          <p id="first-run-banner" className="mb-4 text-sm text-indigo-200">
            First-run setup: the first account becomes administrator.
          </p>
        )}
        {view === "login" && (
          <form id="login-form" onSubmit={login} className="space-y-4">
            <Field
              id="login-username"
              label="Username"
              value={username}
              onChange={setUsername}
              autoComplete="username"
            />
            <Field
              id="login-password"
              label="Password"
              value={password}
              onChange={setPassword}
              type="password"
              autoComplete="current-password"
            />
            <button type="submit" disabled={busy}>
              {busy ? "Signing in…" : "Sign in"}
            </button>
            {auth.oidc_enabled && (
              <a
                id="oidc-login-link"
                href="/auth/oidc/login"
                className="block text-center"
              >
                Continue with {auth.oidc_provider_name || "SSO"}
              </a>
            )}
            <p id="login-register-link">
              No account?{" "}
              <button
                type="button"
                data-action="showRegisterForm"
                onClick={() => setView("register")}
              >
                Register
              </button>
            </p>
          </form>
        )}
        {view === "register" && (
          <form id="register-form" onSubmit={register} className="space-y-4">
            <Field
              id="register-username"
              label="Username"
              value={username}
              onChange={setUsername}
              autoComplete="username"
            />
            <Field
              id="register-password"
              label="Password"
              value={password}
              onChange={setPassword}
              type="password"
              autoComplete="new-password"
            />
            <Field
              id="register-invite-code"
              label="Invite code"
              value={invite}
              onChange={setInvite}
              hidden={!auth.has_users}
            />
            <button type="submit" disabled={busy}>
              {busy ? "Creating…" : "Create account"}
            </button>
            {auth.has_users && (
              <button
                type="button"
                data-action="showLoginForm"
                onClick={() => setView("login")}
              >
                Back to sign in
              </button>
            )}
          </form>
        )}
        {view === "totp" && (
          <form id="totp-form" onSubmit={verifyTOTP} className="space-y-4">
            <Field
              id="totp-code"
              label="Authenticator or backup code"
              value={code}
              onChange={setCode}
              autoComplete="one-time-code"
            />
            <button type="submit" disabled={busy}>
              {busy ? "Verifying…" : "Verify"}
            </button>
            <button
              type="button"
              data-action="showLoginForm"
              onClick={() => {
                setPending("");
                setView("login");
              }}
            >
              Back to sign in
            </button>
          </form>
        )}
      </section>
    </main>
  );
}

function Field({
  id,
  label,
  value,
  onChange,
  type = "text",
  autoComplete,
  hidden = false,
}: {
  id: string;
  label: string;
  value: string;
  onChange: (value: string) => void;
  type?: string;
  autoComplete?: string;
  hidden?: boolean;
}): React.JSX.Element {
  if (hidden) return <></>;
  return (
    <label className="block">
      <span className="block text-sm mb-1">{label}</span>
      <input
        id={id}
        type={type}
        required
        value={value}
        autoComplete={autoComplete}
        onChange={(event) => onChange(event.target.value)}
        className="w-full bg-slate-800 border border-slate-600 rounded-lg px-3 py-2"
      />
    </label>
  );
}

function WantedOutcomeTable({
  outcome,
  dryRun,
}: {
  outcome: WantedOutcome;
  dryRun: boolean;
}): React.JSX.Element {
  const { locale } = useTranslation();
  const decisions = outcome.decisions ?? [];
  return (
    <div data-wanted-outcome className="mt-3 border-t border-slate-800 pt-3">
      <p className="text-xs text-slate-300">
        <strong>{outcome.action ?? ""}</strong> — {outcome.reason ?? ""}
        {outcome.candidate && (
          <span className="text-slate-400"> ({outcome.candidate})</span>
        )}
        {dryRun && (
          <span className="text-slate-500"> {text(locale, "dry_run")}</span>
        )}
      </p>
      {decisions.length > 0 && (
        <div className="overflow-x-auto mt-2">
          <table
            className="text-xs w-full"
            data-wanted-decisions={decisions.length}
          >
            <thead className="text-slate-500">
              <tr>
                <th aria-label="Accepted"></th>
                <th>Format</th>
                <th>Score</th>
                <th>Release</th>
                <th>Decision</th>
              </tr>
            </thead>
            <tbody>
              {decisions.map((decision, index) => (
                <tr
                  key={`${decision.title ?? ""}:${decision.format ?? ""}:${index}`}
                  className={
                    decision.accepted ? "text-emerald-300" : "text-slate-400"
                  }
                >
                  <td>
                    {decision.accepted ? (decision.upgrade ? "↑" : "✓") : "✗"}
                  </td>
                  <td className="font-mono">
                    {(decision.format ?? "?").toUpperCase()}
                  </td>
                  <td>{Math.round(decision.score ?? 0)}</td>
                  <td title={decision.title ?? ""}>
                    {decision.title ?? ""}{" "}
                    <span className="text-slate-600">
                      {decision.source ?? ""}
                    </span>
                  </td>
                  <td>{decision.reason ?? ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function Wanted({
  onSearch,
  admin,
  setNotice,
}: {
  admin: boolean;
  onSearch: (query: string, media: MediaType) => void;
  setNotice: (s: string) => void;
}): React.JSX.Element {
  const { locale } = useTranslation();
  const [items, setItems] = useState<WishlistItem[]>([]);
  const [counts, setCounts] = useState<Record<string, number>>({});
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [filter, setFilter] = useState("");
  const [form, setForm] = useState(false);
  const [title, setTitle] = useState("");
  const [author, setAuthor] = useState("");
  const [media, setMedia] = useState<MediaType>("ebook");
  const [profileID, setProfileID] = useState(0);
  useEffect(() => {
    if (!form) return;
    function close(e: KeyboardEvent) {
      if (e.key === "Escape") setForm(false);
    }
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, [form]);
  const [details, setDetails] = useState<
    Record<number, { outcome: WantedOutcome; dryRun: boolean }>
  >({});
  const [upgrades, setUpgrades] = useState(true);
  const load = async () => {
    try {
      const [wanted, quality] = await Promise.all([
        api<{
          items?: WishlistItem[];
          counts?: Record<string, number>;
          upgrades_enabled?: boolean;
        }>("/api/wishlist"),
        api<Profile[]>("/api/quality-profiles"),
      ]);
      setItems(wanted.items ?? []);
      setCounts(wanted.counts ?? {});
      setUpgrades(wanted.upgrades_enabled !== false);
      setProfiles(quality);
    } catch (e) {
      setNotice(e instanceof Error ? e.message : "Load failed");
    }
  };
  useEffect(() => {
    void load();
  }, []);
  const patch = async (id: number, body: Partial<WishlistItem>) => {
    setItems((old) =>
      old.map((item) => (item.id === id ? { ...item, ...body } : item)),
    );
    try {
      await api(`/api/wishlist/${id}`, {
        method: "PATCH",
        body: JSON.stringify(body),
      });
      await load();
    } catch (e) {
      setNotice(e instanceof Error ? e.message : "Update failed");
      await load();
    }
  };
  const run = async (id: number, dryRun: boolean) => {
    try {
      const response = await api<{ outcome?: WantedOutcome }>(
        `/api/wishlist/${id}/search`,
        { method: "POST", body: JSON.stringify({ dry_run: dryRun }) },
      );
      const outcome = response.outcome ?? {};
      setDetails((previous) => ({ ...previous, [id]: { outcome, dryRun } }));
      setNotice(`Search finished: ${outcome.reason ?? outcome.action ?? ""}`);
      await load();
    } catch (e) {
      setNotice(e instanceof Error ? e.message : "Search failed");
    }
  };
  const shown = items.filter((item) => !filter || item.state === filter);
  const summary = items.length
    ? ["missing", "upgrade", "downloading", "satisfied", "unmonitored"]
        .filter((state) => counts[state])
        .map(
          (state) =>
            `${counts[state]} ${text(locale, `wanted_state_${state}`).toLowerCase()}`,
        )
        .join(" · ")
    : text(locale, "wanted_intro");
  return (
    <section id="tab-wishlist" className="tab-content active">
      <div className="flex flex-wrap gap-2 items-center">
        <h2>Wanted</h2>
        <p id="wanted-summary">{summary}</p>
        <select
          id="wanted-filter"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        >
          <option value="">All</option>
          {[
            "missing",
            "upgrade",
            "downloading",
            "satisfied",
            "unmonitored",
          ].map((state) => (
            <option key={state} value={state}>
              {state}
            </option>
          ))}
        </select>
        {admin && (
          <button
            id="wanted-search-all"
            data-action="runSchedulerNow"
            onClick={() =>
              void api("/api/scheduler/run?wait=1", { method: "POST" })
                .then(load)
                .catch((e) => setNotice(String(e)))
            }
          >
            Search all now
          </button>
        )}
        <button data-action="showWishlistForm" onClick={() => setForm(true)}>
          Add Book
        </button>
      </div>
      {!upgrades && items.length > 0 && (
        <p id="wanted-upgrades-off">Automatic upgrades are off.</p>
      )}
      {form && (
        <form
          id="wishlist-form"
          onSubmit={(e) => {
            e.preventDefault();
            void api("/api/wishlist", {
              method: "POST",
              body: JSON.stringify({
                title,
                author,
                media_type: media,
                quality_profile_id: profileID,
              }),
            })
              .then(() => {
                setForm(false);
                setTitle("");
                return load();
              })
              .catch((e) => setNotice(String(e)));
          }}
        >
          <input
            id="wl-title"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            required
          />
          <input
            id="wl-author"
            value={author}
            onChange={(e) => setAuthor(e.target.value)}
          />
          <select
            id="wl-type"
            value={media}
            onChange={(e) => setMedia(e.target.value as MediaType)}
          >
            {(["ebook", "audiobook", "manga"] as const).map((x) => (
              <option key={x}>{x}</option>
            ))}
          </select>
          <select
            id="wl-profile"
            aria-label="Quality profile"
            value={profileID}
            onChange={(e) => setProfileID(Number(e.target.value))}
          >
            <option value="0">Default profile</option>
            {profiles
              .filter((p) => p.media_type === media)
              .map((p) => (
                <option key={p.id} value={p.id}>
                  {p.name}
                </option>
              ))}
          </select>
          <button data-action="addWishlistItem">Add</button>
          <button type="button" onClick={() => setForm(false)}>
            Cancel
          </button>
        </form>
      )}
      {!items.length && (
        <p id="wishlist-empty" className="p-8 text-center">
          {text(locale, "wanted_intro")}
        </p>
      )}
      <div id="wishlist-list">
        {shown.map((item) => (
          <article
            key={item.id}
            data-wanted-id={item.id}
            className="bg-slate-900 p-3"
          >
            <h4>{item.title}</h4>
            <span data-wanted-state={item.state ?? "missing"}>
              {item.state ?? "missing"}
            </span>
            {item.current_format && (
              <span>{item.current_format.toUpperCase()} on disk</span>
            )}
            <p title={item.last_result}>{item.last_result}</p>
            <label>
              <input
                data-action-change="toggleWantedMonitored"
                type="checkbox"
                checked={item.monitored ?? true}
                onChange={(e) =>
                  void patch(item.id, { monitored: e.target.checked })
                }
              />
              Monitored
            </label>
            <select
              data-action-change="setWantedProfile"
              value={item.quality_profile_id ?? 0}
              onChange={(e) =>
                void patch(item.id, {
                  quality_profile_id: Number(e.target.value),
                })
              }
            >
              <option value="0">Default profile</option>
              {profiles
                .filter((p) => p.media_type === item.media_type)
                .map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name}
                  </option>
                ))}
            </select>
            <button
              data-action="searchWishlistItem"
              onClick={() => onSearch(item.title, item.media_type)}
            >
              Search
            </button>
            {admin && (
              <>
                <button
                  data-action="searchWantedNow"
                  onClick={() => void run(item.id, false)}
                >
                  Search now
                </button>
                <button
                  data-action="explainWanted"
                  onClick={() => void run(item.id, true)}
                >
                  Why?
                </button>
              </>
            )}
            <button
              data-action="deleteWishlistItem"
              onClick={() =>
                void api(`/api/wishlist/${item.id}`, { method: "DELETE" })
                  .then(load)
                  .catch((e) => setNotice(String(e)))
              }
            >
              Delete
            </button>
            {(() => {
              const detail = details[item.id];
              return detail ? <WantedOutcomeTable {...detail} /> : null;
            })()}
          </article>
        ))}
      </div>
    </section>
  );
}

function Settings({
  admin,
  setNotice,
}: {
  admin: boolean;
  setNotice: (message: string) => void;
}): React.JSX.Element {
  const { locale } = useTranslation();
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [formats, setFormats] = useState<Record<string, string[]>>({});
  const [authors, setAuthors] = useState<Author[]>([]);
  const [authorInterval, setAuthorInterval] = useState(7);
  const [authorAutoAdd, setAuthorAutoAdd] = useState(true);
  const [name, setName] = useState("");
  const [media, setMedia] = useState<MediaType>("ebook");
  const load = async () => {
    if (!admin) return;
    try {
      const [quality, known, monitored] = await Promise.all([
        api<Profile[]>("/api/quality-profiles"),
        api<{ formats: Record<string, string[]> }>(
          "/api/quality-profiles/formats",
        ),
        api<{ authors?: Author[] }>("/api/authors"),
      ]);
      setProfiles(quality);
      setFormats(known.formats);
      setAuthors(monitored.authors ?? []);
    } catch (e) {
      setNotice(e instanceof Error ? e.message : "Settings failed");
    }
  };
  useEffect(() => {
    void load();
  }, [admin]);
  if (!admin)
    return (
      <section id="tab-settings" className="tab-content active">
        <AuthSettings />
        <AccountSettings />
      </section>
    );
  const saveProfile = async (profile: Profile) => {
    try {
      await api(
        typeof profile.id === "string"
          ? "/api/quality-profiles"
          : `/api/quality-profiles/${profile.id}`,
        {
          method: typeof profile.id === "string" ? "POST" : "PUT",
          body: JSON.stringify({ ...profile, id: undefined }),
        },
      );
      await load();
    } catch (e) {
      setNotice(e instanceof Error ? e.message : "Profile save failed");
    }
  };
  const edit = (id: Profile["id"], fn: (profile: Profile) => Profile) =>
    setProfiles((rows) => rows.map((row) => (row.id === id ? fn(row) : row)));
  const checkAuthor = async (id: number) => {
    try {
      const data = await api<{ result?: AuthorCheckResult }>(
        `/api/authors/${id}/check`,
        { method: "POST" },
      );
      const result = data.result ?? {};
      const summary = result.error
        ? result.error
        : result.baseline
          ? text(locale, "author_baseline", { seen: result.seen ?? 0 })
          : [
              text(locale, "author_new", { count: result.new?.length ?? 0 }),
              result.added
                ? text(locale, "author_added", { count: result.added })
                : "",
            ]
              .filter(Boolean)
              .join(", ");
      setNotice(
        text(locale, "author_checked", {
          author: result.author ?? "",
          summary,
        }),
      );
      await load();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to save");
    }
  };
  return (
    <section id="tab-settings" className="tab-content active">
      <AuthSettings />
      <AccountSettings />
      <GeneralSettings />
      <div>
        <button
          data-action="qpNew"
          data-arg={media}
          onClick={() =>
            setProfiles((rows) => [
              ...rows,
              {
                id: `new-${Date.now()}`,
                name: "",
                media_type: media,
                format_ranking: [...(formats[media] ?? [])].slice(0, 2),
                upgrade_allowed: true,
              },
            ])
          }
        >
          New profile
        </button>
        <select
          value={media}
          onChange={(e) => setMedia(e.target.value as MediaType)}
        >
          {(["ebook", "audiobook", "manga"] as const).map((x) => (
            <option key={x}>{x}</option>
          ))}
        </select>
      </div>
      <div id="quality-profiles-list">
        {profiles.map((profile) => (
          <article data-qp={profile.id} key={profile.id}>
            <input
              data-field="name"
              value={profile.name}
              onChange={(e) =>
                edit(profile.id, (p) => ({ ...p, name: e.target.value }))
              }
            />
            {profile.builtin && <span>Built-in</span>}
            <div data-qp-ranking>
              {profile.format_ranking.map((format, index) => (
                <div data-qp-row={format} key={format}>
                  <input
                    data-action-change="qpToggleFormat"
                    data-format={format}
                    type="checkbox"
                    checked
                    onChange={() =>
                      edit(profile.id, (p) => {
                        const ranking = p.format_ranking.filter(
                          (x) => x !== format,
                        );
                        return {
                          ...p,
                          format_ranking: ranking,
                          cutoff_format: ranking.includes(p.cutoff_format || "")
                            ? p.cutoff_format
                            : ranking[0] || "",
                        };
                      })
                    }
                  />
                  {format}
                  <button
                    data-action="qpMove"
                    data-format={format}
                    data-dir="-1"
                    disabled={index === 0}
                    onClick={() =>
                      edit(profile.id, (p) => {
                        const r = [...p.format_ranking];
                        [r[index - 1]!, r[index]!] = [r[index]!, r[index - 1]!];
                        return { ...p, format_ranking: r };
                      })
                    }
                  >
                    ▲
                  </button>
                  <button
                    data-action="qpMove"
                    data-format={format}
                    data-dir="1"
                    disabled={index === profile.format_ranking.length - 1}
                    onClick={() =>
                      edit(profile.id, (p) => {
                        const r = [...p.format_ranking];
                        [r[index]!, r[index + 1]!] = [r[index + 1]!, r[index]!];
                        return { ...p, format_ranking: r };
                      })
                    }
                  >
                    ▼
                  </button>
                </div>
              ))}
            </div>
            {(formats[profile.media_type] || [])
              .filter((format) => !profile.format_ranking.includes(format))
              .map((format) => (
                <label className="block" key={format}>
                  <input
                    data-action-change="qpToggleFormat"
                    data-format={format}
                    type="checkbox"
                    checked={false}
                    onChange={() =>
                      edit(profile.id, (p) => ({
                        ...p,
                        format_ranking: [...p.format_ranking, format],
                        cutoff_format: p.cutoff_format || format,
                      }))
                    }
                  />
                  {format.toUpperCase()} · not grabbed
                </label>
              ))}
            <select
              data-field="cutoff_format"
              value={profile.cutoff_format ?? ""}
              onChange={(e) =>
                edit(profile.id, (p) => ({
                  ...p,
                  cutoff_format: e.target.value,
                }))
              }
            >
              {profile.format_ranking.map((f) => (
                <option key={f}>{f}</option>
              ))}
            </select>
            <label>
              <input
                type="checkbox"
                checked={profile.upgrade_allowed ?? false}
                onChange={(e) =>
                  edit(profile.id, (p) => ({
                    ...p,
                    upgrade_allowed: e.target.checked,
                  }))
                }
              />
              Upgrade until cutoff
            </label>
            <label>
              Preferred size (MiB){" "}
              <input
                data-field="preferred_size_min"
                type="number"
                min="0"
                value={Math.round((profile.preferred_size_min ?? 0) / 1048576)}
                onChange={(e) =>
                  edit(profile.id, (p) => ({
                    ...p,
                    preferred_size_min:
                      Math.max(0, Number(e.target.value) || 0) * 1048576,
                  }))
                }
              />
              <span>to</span>
              <input
                data-field="preferred_size_max"
                type="number"
                min="0"
                value={Math.round((profile.preferred_size_max ?? 0) / 1048576)}
                onChange={(e) =>
                  edit(profile.id, (p) => ({
                    ...p,
                    preferred_size_max:
                      Math.max(0, Number(e.target.value) || 0) * 1048576,
                  }))
                }
              />
            </label>
            <button
              data-action="qpSave"
              onClick={() => void saveProfile(profile)}
            >
              Save
            </button>
            {!profile.builtin && (
              <button
                data-action="qpDelete"
                onClick={() => {
                  if (!window.confirm(`Delete profile ${profile.name}?`))
                    return;
                  if (typeof profile.id === "string") {
                    setProfiles((old) =>
                      old.filter((p) => p.id !== profile.id),
                    );
                    return;
                  }
                  void api(`/api/quality-profiles/${profile.id}`, {
                    method: "DELETE",
                  })
                    .then(load)
                    .catch((e) => setNotice(String(e)));
                }}
              >
                Delete
              </button>
            )}
          </article>
        ))}
      </div>
      <div>
        <input
          id="author-name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <label>
          Check interval (days)
          <input
            id="author-interval"
            type="number"
            min="1"
            value={authorInterval}
            onChange={(e) => setAuthorInterval(Number(e.target.value))}
          />
        </label>
        <label>
          <input
            id="author-auto-add"
            type="checkbox"
            checked={authorAutoAdd}
            onChange={(e) => setAuthorAutoAdd(e.target.checked)}
          />{" "}
          Automatically add new works
        </label>
        <button
          data-action="addAuthor"
          onClick={() =>
            void api("/api/authors/monitor", {
              method: "POST",
              body: JSON.stringify({
                name,
                check_interval_days: authorInterval,
                auto_add: authorAutoAdd,
              }),
            })
              .then(() => {
                setName("");
                return load();
              })
              .catch((e) => setNotice(String(e)))
          }
        >
          Follow
        </button>
        {authors.map((a) => (
          <article data-author-id={a.id} key={a.id}>
            <strong>{a.name}</strong>
            <p className="text-xs text-slate-400">
              {a.seen_works
                ? `${a.seen_works} works known · ${a.last_checked && !a.last_checked.startsWith("0001") ? new Date(a.last_checked).toLocaleString() : "never"}`
                : "not checked yet"}
            </p>
            <input
              data-action-change="authorInterval"
              type="number"
              value={a.check_interval_days}
              onChange={(e) =>
                void api(`/api/authors/${a.id}`, {
                  method: "PATCH",
                  body: JSON.stringify({
                    check_interval_days: Number(e.target.value),
                  }),
                })
                  .then(load)
                  .catch((e) => setNotice(String(e)))
              }
            />
            <label>
              <input
                data-action-change="authorAutoAdd"
                type="checkbox"
                checked={a.auto_add}
                onChange={(e) => {
                  const auto_add = e.target.checked;
                  setAuthors((old) =>
                    old.map((row) =>
                      row.id === a.id ? { ...row, auto_add } : row,
                    ),
                  );
                  void api(`/api/authors/${a.id}`, {
                    method: "PATCH",
                    body: JSON.stringify({ auto_add }),
                  })
                    .then(load)
                    .catch((e) => {
                      setNotice(String(e));
                      void load();
                    });
                }}
              />
              auto-add
            </label>
            <button
              data-action="checkAuthor"
              onClick={() => void checkAuthor(a.id)}
            >
              Check now
            </button>
            <button
              data-action="deleteAuthor"
              onClick={() => {
                if (window.confirm(`Unfollow ${a.name}?`))
                  void api(`/api/authors/${a.id}`, { method: "DELETE" })
                    .then(load)
                    .catch((e) => setNotice(String(e)));
              }}
            >
              Unfollow
            </button>
          </article>
        ))}
      </div>
    </section>
  );
}
const root = document.getElementById("root");
if (root === null) throw new Error("Missing root");
createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
