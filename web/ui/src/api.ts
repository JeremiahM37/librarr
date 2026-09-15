export type MediaType = "ebook" | "audiobook" | "manga";
export type Role = "admin" | "user";

export interface AuthStatus {
  user_id?:number;
  authenticated: boolean;
  has_users: boolean;
  oidc_enabled: boolean;
  oidc_provider_name?: string;
  username?: string;
  role?: Role;
}
export interface SearchResult {
  title: string;
  author?: string;
  source: string;
  size_human?: string;
  size?: number | string;
  seeders?: number;
  format?: string;
  cover_url?: string;
  download_url?: string;
  epub_url?: string;
  url?: string;
  in_library?: boolean;
  language?: string;
  year?: string | number;
  publisher?: string;
  copies?: number;
  library_title?: string;
  library_item_id?: number;
  abb_url?: string;
  info_hash?: string;
  magnet?: string;
  md5?: string;
  source_id?: string;
  download_protocol?: string;
  media_type?: string;
  sizeHuman?: string;
  leechers?: number;
  indexer?: string;
}
export interface WishlistItem {
  id: number;
  title: string;
  author?: string;
  media_type: MediaType;
  state?: string;
  monitored?: boolean;
  quality_profile_id?: number;
  profile_name?: string;
  current_format?: string;
  cutoff_met?: boolean;
  last_result?: string;
}
export interface Profile {
  id: number | string;
  name: string;
  media_type: MediaType;
  builtin?: boolean;
  format_ranking: string[];
  cutoff_format?: string;
  upgrade_allowed?: boolean;
  preferred_size_min?: number;
  preferred_size_max?: number;
}
export interface Author {
  id: number;
  name: string;
  check_interval_days: number;
  auto_add: boolean;
  seen_works?: number;
  last_checked?: string;
  last_book_found?: string;
}
export interface WantedDecision {
  accepted?: boolean;
  upgrade?: boolean;
  format?: string;
  score?: number;
  title?: string;
  source?: string;
  reason?: string;
}
export interface WantedOutcome {
  action?: string;
  reason?: string;
  candidate?: string;
  decisions?: WantedDecision[];
}
export interface AuthorCheckResult {
  author?: string;
  baseline?: boolean;
  seen?: number;
  new?: string[];
  added?: number;
  error?: string;
}
export interface LibraryItem {
  id?: string | number;
  title?: string;
  name?: string;
  author?: string;
  series?: string;
  cover_url?: string;
  format?: string;
  file_path?: string;
  file_format?: string;
  file_size?: number;
  size?: number | string;
  duration_hours?: number;
  num_files?: number;
  pages?: number;
  library?: string;
  abs_url?: string;
  kavita_url?: string;
}
export interface LibraryResponse {
  items?: LibraryItem[];
  total?: number;
  page?: number;
  pages?: number;
}
export type DownloadStatusName =
  | "queued"
  | "searching"
  | "downloading"
  | "organizing"
  | "importing"
  | "retry_wait"
  | "completed"
  | "error"
  | "dead_letter";
export interface DownloadJob {
  source?: string;
  title?: string;
  status: DownloadStatusName | string;
  progress?: number;
  size?: number | string;
  speed?: string;
  hash?: string;
  job_id?: string;
  error?: string;
  detail?: string;
  retry_count?: number;
  max_retries?: number;
}
export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}
let onUnauthorized: (() => void) | undefined;
export function setUnauthorizedHandler(
  handler: (() => void) | undefined,
): void {
  onUnauthorized = handler;
}
export async function request(
  path: string,
  init: RequestInit = {},
): Promise<Response> {
  const url = new URL(path, window.location.origin);
  const apiKey = localStorage.getItem("librarr_apikey");
  if (apiKey) url.searchParams.set("apikey", apiKey);
  const headers = new Headers(init.headers);
  if (init.body !== undefined && !headers.has("Content-Type"))
    headers.set("Content-Type", "application/json");
  const response = await fetch(url, {
    ...init,
    headers,
    credentials: "same-origin",
  });
  if (response.status === 401) onUnauthorized?.();
  return response;
}
export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await request(path, init);
  const body: unknown = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message =
      typeof body === "object" &&
      body !== null &&
      "error" in body &&
      typeof body.error === "string"
        ? body.error
        : response.statusText;
    throw new ApiError(response.status, message);
  }
  return body as T;
}

// /auth/status is intentionally public and skips API-key middleware. Validate a
// saved key against the protected config endpoint before treating it as a login.
export async function authStatus():Promise<AuthStatus>{
 const status=await api<AuthStatus>('/api/auth/status');
 if(!status.authenticated&&localStorage.getItem('librarr_apikey')){
  const config=await api<{current_user?:string;current_role?:Role}>('/api/config');
  if(config.current_user&&config.current_role)return {...status,authenticated:true,username:config.current_user,role:config.current_role};
 }
 return status;
}
