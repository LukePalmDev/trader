// Store reattivo (Preact signals) — replica tryAutoLoad del viewer attuale:
// bootstrap token → fetch paralleli (allSettled) → sanitize → popola i signal.
import { signal } from "@preact/signals";
import { bootstrapToken, fetchJson } from "../lib/api";
import { sanitizeCollection, sanitizeRecord } from "../lib/sanitize";

type Row = Record<string, unknown>;

export const sources = signal<Row[]>([]);
export const allProducts = signal<Row[]>([]);
export const dbProducts = signal<Row[]>([]);
export const baseModels = signal<Row[]>([]);
export const storageSizes = signal<Row[]>([]);
export const standardGroups = signal<Row[]>([]);
export const priceHistory = signal<Row[]>([]);
export const subitoAds = signal<Row[]>([]);
export const subitoSold = signal<Row[]>([]);
export const ebaySold = signal<Row[]>([]);
export const subitoPendingReviews = signal<Row[]>([]);
export const subitoOpportunities = signal<Row[]>([]);

export const loading = signal<boolean>(false);
export const errorMsg = signal<string>("");
export const lastUpdate = signal<string>("");

async function getRows(url: string): Promise<Row[]> {
  const res = await fetchJson<unknown>(url);
  if (!res.ok || !res.body) return [];
  const body = res.body;
  if (Array.isArray(body)) return sanitizeCollection(body as Row[]);
  // /api/combined/latest → { products: [...] }
  const prods = (body as { products?: Row[] }).products;
  return Array.isArray(prods) ? sanitizeCollection(prods) : [];
}

export async function loadAll(): Promise<void> {
  loading.value = true;
  errorMsg.value = "";
  try {
    await bootstrapToken();
    const [
      src, combined, dbp, base, storage, stdg, hist,
      sAds, sSold, eSold, pending, opp,
    ] = await Promise.allSettled([
      fetchJson<Row[]>("/api/sources"),
      getRows("/api/combined/latest"),
      getRows("/api/db/products"),
      getRows("/api/db/base-models"),
      getRows("/api/db/storage-sizes"),
      getRows("/api/db/standard-groups"),
      getRows("/api/db/price-history"),
      getRows("/api/subito/ads"),
      getRows("/api/subito/sold"),
      getRows("/api/ebay/sold"),
      getRows("/api/subito/pending-reviews"),
      getRows("/api/valuation/subito-opportunities"),
    ]);
    const val = <T,>(r: PromiseSettledResult<T>, d: T): T => (r.status === "fulfilled" ? r.value : d);

    const srcRes = val(src, { ok: false, status: 0, body: null } as Awaited<ReturnType<typeof fetchJson<Row[]>>>);
    sources.value = srcRes.ok && Array.isArray(srcRes.body) ? sanitizeCollection(srcRes.body as Row[]) : [];
    allProducts.value = val(combined, []);
    dbProducts.value = val(dbp, []);
    baseModels.value = val(base, []);
    storageSizes.value = val(storage, []);
    standardGroups.value = val(stdg, []);
    priceHistory.value = val(hist, []);
    subitoAds.value = val(sAds, []);
    subitoSold.value = val(sSold, []);
    ebaySold.value = val(eSold, []);
    subitoPendingReviews.value = val(pending, []);
    subitoOpportunities.value = val(opp, []);

    const latest = sources.value
      .map((s) => sanitizeRecord(s).last_scraped as string)
      .filter(Boolean)
      .sort()
      .pop();
    lastUpdate.value = latest || "";
  } catch (e) {
    errorMsg.value = e instanceof Error ? e.message : String(e);
  } finally {
    loading.value = false;
  }
}
