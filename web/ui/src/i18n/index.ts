import { useSyncExternalStore } from "react";
import { catalog } from "./catalog";
export type Locale = "en" | "ru";
export type TranslationKey = keyof typeof catalog.en;
const event = "librarr:language";
const current = (): Locale =>
  localStorage.getItem("librarr_lang") === "ru" ? "ru" : "en";
function subscribe(listener: () => void) {
  window.addEventListener(event, listener);
  window.addEventListener("storage", listener);
  return () => {
    window.removeEventListener(event, listener);
    window.removeEventListener("storage", listener);
  };
}
export function setLanguage(locale: Locale) {
  localStorage.setItem("librarr_lang", locale);
  document.documentElement.lang = locale;
  window.dispatchEvent(new Event(event));
}
export function translate(
  locale: Locale,
  key: TranslationKey,
  values: Record<string, string | number> = {},
) {
  const selected: Partial<Record<TranslationKey, string>> = catalog[locale];
  let result = selected[key] || catalog.en[key];
  for (const [name, value] of Object.entries(values))
    result = result.replaceAll(`{${name}}`, String(value));
  return result;
}
export function useTranslation() {
  const locale = useSyncExternalStore(subscribe, current);
  return {
    locale,
    t: (key: TranslationKey, values?: Record<string, string | number>) =>
      translate(locale, key, values),
    setLanguage,
  };
}
