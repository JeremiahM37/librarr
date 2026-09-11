import { useEffect, useState } from "react";
import { api,authStatus } from "./api";
import { useTranslation } from "./i18n";
interface Status {
  authenticated: boolean;
  user_id?: number;
  role?: "admin" | "user";
}
interface Result {
  success: boolean;
  error?: string;
}
interface User {
  id: number;
  username: string;
  role: "admin" | "user";
  totp_enabled: boolean;
  last_login?: string;
}
interface Invite {
  id: number;
  code: string;
  role: "admin" | "user";
  uses: number;
  max_uses: number;
  expires_at: number;
}
const card = "bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3";
const input =
  "block w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white";
const button =
  "px-3 py-2 rounded-lg bg-indigo-600 text-white disabled:opacity-50";
function message(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
async function mutate(path: string, method: string, body?: unknown) {
  const result = await api<Result>(path, {
    method,
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  if (!result.success)
    throw new Error(result.error || "The change could not be saved.");
}

export function AccountSettings() {
  const [status, setStatus] = useState<Status>();
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    void authStatus()
      .then((value) => {
        if (active) setStatus(value);
      })
      .catch((error) => {
        if (active) setError(message(error));
      });
    return () => {
      active = false;
    };
  }, []);
  if (error) return <p role="alert">{error}</p>;
  if (!status?.authenticated) return null;
  return (
    <>
      {status.user_id && <PasswordSettings />}
      {status.role === "admin" && <UserSettings />}
    </>
  );
}
function PasswordSettings() {
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [confirm, setConfirm] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  async function save() {
    setError("");
    setNotice("");
    if (!current || !next || !confirm) {
      setError("All three fields are required");
      return;
    }
    if (next.length < 6) {
      setError("New password must be at least 6 characters");
      return;
    }
    if (next !== confirm) {
      setError("New password and confirmation do not match");
      return;
    }
    setBusy(true);
    try {
      await mutate("/api/me/password", "POST", {
        current_password: current,
        new_password: next,
      });
      setCurrent("");
      setNext("");
      setConfirm("");
      setNotice("Password changed.");
    } catch (error) {
      setError(message(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <section id="change-password-section" className={card}>
      <h3 className="text-lg font-semibold text-white">Change password</h3>
      <form
        id="change-password-form"
        className="space-y-3"
        onSubmit={(event) => {
          event.preventDefault();
          void save();
        }}
      >
        <label className="block">
          Current password
          <input
            id="cp-current"
            className={input}
            type="password"
            autoComplete="current-password"
            value={current}
            onChange={(event) => setCurrent(event.target.value)}
          />
        </label>
        <label className="block">
          New password
          <input
            id="cp-new"
            className={input}
            type="password"
            autoComplete="new-password"
            value={next}
            onChange={(event) => setNext(event.target.value)}
          />
        </label>
        <label className="block">
          Confirm new password
          <input
            id="cp-confirm"
            className={input}
            type="password"
            autoComplete="new-password"
            value={confirm}
            onChange={(event) => setConfirm(event.target.value)}
          />
        </label>
        {error && (
          <p id="cp-error" role="alert" className="text-red-400">
            {error}
          </p>
        )}
        {notice && (
          <p role="status" className="text-emerald-400">
            {notice}
          </p>
        )}
        <button disabled={busy} className={button}>
          {busy ? "Saving…" : "Change password"}
        </button>
      </form>
    </section>
  );
}
function UserSettings() {
  const { t } = useTranslation();
  const [users, setUsers] = useState<User[]>([]);
  const [invites, setInvites] = useState<Invite[]>([]);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<"admin" | "user">("user");
  const [maxUses, setMaxUses] = useState(1);
  const [days, setDays] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  async function load() {
    const [userData, inviteData] = await Promise.all([
      api<Result & { users?: User[] }>("/api/users"),
      api<Result & { invites?: Invite[] }>("/api/invites"),
    ]);
    if (!userData.success)
      throw new Error(userData.error || "Failed to load users");
    setUsers(userData.users || []);
    setInvites(inviteData.invites || []);
  }
  useEffect(() => {
    void load().catch((error) => setError(message(error)));
  }, []);
  async function action(run: () => Promise<void>, notice: string) {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      await run();
      setNotice(notice);
      await load();
    } catch (error) {
      setError(message(error));
    } finally {
      setBusy(false);
    }
  }
  async function add() {
    if (!username.trim() || !password) {
      setError(t("err_credentials_required"));
      return;
    }
    await action(async () => {
      await mutate("/api/register", "POST", {
        username: username.trim(),
        password,
      });
      setUsername("");
      setPassword("");
    }, t("user_created"));
  }
  async function copy(code: string) {
    try {
      await navigator.clipboard.writeText(code);
      setNotice("Copied");
    } catch {
      setError("Copy failed — select the code manually");
    }
  }
  return (
    <section id="user-management" className={card}>
      <h3 className="text-lg font-semibold text-white">User management</h3>
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
      <div id="users-list" className="space-y-2">
        {users.map((user) => (
          <div
            key={user.id}
            className="flex flex-wrap justify-between gap-2 bg-slate-800 rounded-lg p-3"
          >
            <div>
              <strong>{user.username}</strong>
              {user.totp_enabled && (
                <span className="ml-2 text-emerald-400">2FA</span>
              )}
              <p className="text-xs text-slate-400">
                {t("last_login", {
                  date: user.last_login
                    ? new Date(user.last_login).toLocaleDateString()
                    : t("never"),
                })}
              </p>
            </div>
            <div className="flex gap-2">
              <select
                aria-label={`Role for ${user.username}`}
                value={user.role}
                disabled={busy}
                className="bg-slate-700 rounded px-2"
                onChange={(event) =>
                  void action(
                    () =>
                      mutate(`/api/users/${user.id}`, "PATCH", {
                        role: event.target.value,
                      }),
                    t("user_role_updated"),
                  )
                }
              >
                <option value="user">{t("role_user")}</option>
                <option value="admin">{t("role_admin")}</option>
              </select>
              <button
                className="px-3 rounded bg-red-800"
                disabled={busy}
                onClick={() => {
                  if (
                    window.confirm(
                      t("confirm_delete_user", { username: user.username }),
                    )
                  )
                    void action(
                      () => mutate(`/api/users/${user.id}`, "DELETE"),
                      t("user_deleted"),
                    );
                }}
              >
                Delete {user.username}
              </button>
            </div>
          </div>
        ))}
      </div>
      <form
        className="flex flex-wrap gap-2"
        onSubmit={(event) => {
          event.preventDefault();
          void add();
        }}
      >
        <label className="flex-1 min-w-0">
          Username
          <input
            id="new-user-name"
            className={input}
            autoComplete="off"
            value={username}
            onChange={(event) => setUsername(event.target.value)}
          />
        </label>
        <label className="flex-1 min-w-0">
          Password
          <input
            id="new-user-pass"
            className={input}
            type="password"
            autoComplete="new-password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
          />
        </label>
        <button disabled={busy} className={button}>
          Add user
        </button>
      </form>
      <h4 className="font-semibold">Invite codes</h4>
      <div id="invite-codes-list" className="space-y-2">
        {!invites.length && <p>No invite codes yet.</p>}
        {invites.map((invite) => (
          <div
            key={invite.id}
            className="bg-slate-800 p-3 rounded-lg flex flex-wrap gap-2 justify-between"
          >
            <div className="min-w-0">
              <code className="select-all break-all text-indigo-300">
                {invite.code}
              </code>
              <p className="text-xs text-slate-400">
                role: {invite.role} · uses: {invite.uses} / {invite.max_uses} ·
                expires:{" "}
                {invite.expires_at
                  ? new Date(invite.expires_at * 1000).toLocaleDateString()
                  : "Never"}
              </p>
            </div>
            <div className="flex gap-2">
              <button className={button} onClick={() => void copy(invite.code)}>
                Copy
              </button>
              <button
                disabled={busy}
                className="px-3 rounded bg-red-800"
                onClick={() => {
                  if (
                    window.confirm(
                      "Revoke this invite code? Anyone with it will no longer be able to register.",
                    )
                  )
                    void action(
                      () => mutate(`/api/invites/${invite.id}`, "DELETE"),
                      "Invite revoked",
                    );
                }}
              >
                Revoke
              </button>
            </div>
          </div>
        ))}
      </div>
      <form
        className="flex flex-wrap items-end gap-2"
        onSubmit={(event) => {
          event.preventDefault();
          void action(
            () =>
              mutate("/api/invites", "POST", {
                role,
                max_uses: maxUses,
                expires_in: days * 86400,
              }),
            "Invite code generated",
          );
        }}
      >
        <label>
          Role
          <select
            id="invite-role"
            className={input}
            value={role}
            onChange={(event) =>
              setRole(event.target.value === "admin" ? "admin" : "user")
            }
          >
            <option value="user">User</option>
            <option value="admin">Admin</option>
          </select>
        </label>
        <label>
          Maximum uses
          <input
            id="invite-max-uses"
            className={input}
            type="number"
            min={1}
            required
            value={maxUses}
            onChange={(event) => setMaxUses(Number(event.target.value))}
          />
        </label>
        <label>
          Expires in days (0 = never)
          <input
            id="invite-expires-days"
            className={input}
            type="number"
            min={0}
            required
            value={days}
            onChange={(event) => setDays(Number(event.target.value))}
          />
        </label>
        <button disabled={busy} className={button}>
          Generate invite
        </button>
      </form>
    </section>
  );
}
