import { useEffect, useState } from "react";
import { api } from "./api";
import { useTranslation } from "./i18n";

interface Enrollment {
  success: boolean;
  error?: string;
  secret: string;
  qr_url?: string;
  qr_png?: string;
  backup_codes: string[];
}
interface Result {
  success: boolean;
  error?: string;
}
type Mode = "status" | "enroll" | "disable";
export function AuthSettings() {
  const { t } = useTranslation();
  const [enabled, setEnabled] = useState<boolean>();
  const [mode, setMode] = useState<Mode>("status");
  const [enrollment, setEnrollment] = useState<Enrollment>();
  const [code, setCode] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  useEffect(() => {
    let active = true;
    void api<{ enabled: boolean }>("/api/totp/status")
      .then((d) => {
        if (active) setEnabled(d.enabled);
      })
      .catch((e) => {
        if (active) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      active = false;
    };
  }, []);
  async function setup() {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const d = await api<Enrollment>("/api/totp/setup", { method: "POST" });
      if (!d.success)
        throw new Error(
          d.error || "Failed to set up two-factor authentication",
        );
      setEnrollment(d);
      setMode("enroll");
      setCode("");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }
  async function submit() {
    if (!code.trim()) {
      setError("Enter your authentication code.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const disable = mode === "disable";
      const d = await api<Result>(
        disable ? "/api/totp/disable" : "/api/totp/verify",
        { method: "POST", body: JSON.stringify({ code: code.trim() }) },
      );
      if (!d.success) throw new Error(d.error || "Invalid authentication code");
      setEnabled(!disable);
      setNotice(
        disable
          ? "Two-factor authentication disabled."
          : "Two-factor authentication enabled.",
      );
      setMode("status");
      setEnrollment(undefined);
      setCode("");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }
  function cancel() {
    setMode("status");
    setEnrollment(undefined);
    setCode("");
    setError("");
  }
  return (
    <section
      id="totp-settings"
      className="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3"
    >
      <h3 className="text-lg font-semibold text-white">{t("s_totp_title")}</h3>
      <p className="text-sm text-slate-400">{t("s_totp_desc")}</p>
      {error && (
        <p role="alert" className="text-sm text-red-400">
          {error}
        </p>
      )}
      {notice && (
        <p role="status" className="text-sm text-emerald-400">
          {notice}
        </p>
      )}
      {enabled === undefined && <p className="text-slate-400">Loading…</p>}
      {mode === "status" && enabled === false && (
        <div id="totp-disabled-section">
          <p className="text-sm text-slate-400">
            Two-factor authentication is not enabled.
          </p>
          <button
            disabled={busy}
            onClick={() => void setup()}
            className="px-4 py-2 mt-3 bg-indigo-600 text-white rounded-lg"
          >
            {busy ? "Preparing…" : t("enable_2fa")}
          </button>
        </div>
      )}
      {mode === "status" && enabled === true && (
        <div id="totp-enabled-section">
          <p className="text-emerald-400">{t("totp_enabled_msg")}</p>
          <button
            onClick={() => {
              setMode("disable");
              setCode("");
              setNotice("");
            }}
            className="px-4 py-2 mt-3 bg-red-700 text-white rounded-lg"
          >
            {t("disable_2fa")}
          </button>
        </div>
      )}
      {mode === "enroll" && enrollment && (
        <div id="totp-setup-section" className="space-y-3">
          <p className="text-sm text-slate-400">{t("totp_scan_qr")}</p>
          {enrollment.qr_png?.startsWith("data:image/png;base64,") && (
            <div id="totp-qr-wrap">
              <img
                id="totp-qr-img"
                src={enrollment.qr_png}
                alt="Authenticator setup QR code"
                width={200}
                height={200}
              />
            </div>
          )}
          <code
            id="totp-secret-display"
            className="block break-all select-all text-sm text-indigo-300"
          >
            {enrollment.secret}
          </code>
          <code
            id="totp-otpauth-uri"
            className="block break-all select-all text-xs text-slate-400"
          >
            {enrollment.qr_url}
          </code>
          <p className="text-sm text-slate-400">{t("totp_backup_codes")}</p>
          <div id="totp-backup-codes" className="grid grid-cols-2 gap-2">
            {enrollment.backup_codes.map((c) => (
              <code
                key={c}
                className="bg-slate-800 rounded p-2 text-sm select-all"
              >
                {c}
              </code>
            ))}
          </div>
        </div>
      )}
      {(mode === "enroll" || mode === "disable") && (
        <form
          id={mode === "disable" ? "totp-disable-section" : "totp-verify-form"}
          onSubmit={(e) => {
            e.preventDefault();
            void submit();
          }}
          className="space-y-3"
        >
          <label className="block text-sm text-slate-300">
            {mode === "disable"
              ? "Current authentication code"
              : "Authentication code"}
            <input
              id={mode === "disable" ? "totp-disable-code" : "totp-verify-code"}
              value={code}
              autoComplete="one-time-code"
              inputMode="numeric"
              onChange={(e) => setCode(e.target.value)}
              className="block w-full mt-2 px-3 py-2 bg-slate-800 border border-slate-700 rounded-lg"
            />
          </label>
          <div className="flex gap-2">
            <button
              type="submit"
              disabled={busy}
              className="px-4 py-2 bg-indigo-600 text-white rounded-lg"
            >
              {busy
                ? "Checking…"
                : mode === "disable"
                  ? "Disable two-factor authentication"
                  : t("verify_enable")}
            </button>
            <button
              type="button"
              disabled={busy}
              onClick={cancel}
              className="px-4 py-2 bg-slate-700 rounded-lg"
            >
              {t("cancel")}
            </button>
          </div>
        </form>
      )}
    </section>
  );
}
