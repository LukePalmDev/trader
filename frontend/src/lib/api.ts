// Client API (portato da viewer/modules/api.js).
// Modalità statica (export_static / GitHub Pages): legge i JSON in ./data/.
export const IS_STATIC =
  typeof window !== "undefined" && window.location.hostname.endsWith("github.io");

const STATIC_MAP: Record<string, string> = {
  "/api/sources": "./data/sources.json",
  "/api/combined/latest": "./data/combined-latest.json",
  "/api/db/products": "./data/db-products.json",
  "/api/db/base-models": "./data/db-base-models.json",
  "/api/db/storage-sizes": "./data/db-storage-sizes.json",
  "/api/db/standard-groups": "./data/db-standard-groups.json",
  "/api/db/price-history": "./data/db-price-history.json",
  "/api/subito/ads": "./data/subito-ads.json",
  "/api/subito/stats": "./data/subito-stats.json",
  "/api/subito/sold": "./data/subito-sold.json",
  "/api/subito/sold-stats": "./data/subito-sold-stats.json",
  "/api/subito/pending-reviews": "./data/subito-pending-reviews.json",
  "/api/ebay/sold": "./data/ebay-sold.json",
  "/api/ebay/stats": "./data/ebay-stats.json",
  "/api/valuation/subito-opportunities": "./data/valuation-opportunities.json",
};

export interface ApiResponse<T = unknown> { ok: boolean; status: number; body: T | null; }

let apiToken = "";

function _cache(token: string): void {
  try { window.sessionStorage.setItem("trader_api_token", token); } catch { /* no-op */ }
}
try {
  apiToken = window.sessionStorage.getItem("trader_api_token") || "";
} catch { /* no-op */ }

export async function bootstrapToken(): Promise<void> {
  if (IS_STATIC) return;
  try {
    const res = await fetch("/api/token");
    if (res.ok) {
      const data = await res.json();
      if (data && data.token) { apiToken = data.token; _cache(apiToken); }
    }
  } catch { /* server non raggiungibile */ }
}

function authHeaders(headers: Record<string, string> = {}): Record<string, string> {
  return apiToken ? { ...headers, Authorization: `Bearer ${apiToken}` } : headers;
}

export async function fetchJson<T = unknown>(
  url: string, options: RequestInit = {},
): Promise<ApiResponse<T>> {
  if (IS_STATIC && url.startsWith("/api/")) {
    const method = (options.method || "GET").toUpperCase();
    if (method !== "GET") return { ok: false, status: 405, body: null };
    const staticFile = STATIC_MAP[url.split("?")[0]];
    if (!staticFile) return { ok: false, status: 404, body: null };
    try {
      const res = await fetch(staticFile);
      let body: T | null = null;
      try { body = await res.json(); } catch { body = null; }
      return { ok: res.ok, status: res.status, body };
    } catch {
      return { ok: false, status: 503, body: null };
    }
  }
  const opts: RequestInit = { ...options, headers: authHeaders((options.headers as Record<string, string>) || {}) };
  const response = await fetch(url, opts);
  let body: T | null = null;
  try { body = await response.json(); } catch { body = null; }
  return { ok: response.ok, status: response.status, body };
}
