// Formatter portati da viewer/app.js.
export function fmtDate(iso: unknown): string {
  if (!iso) return "—";
  return new Date(iso as string).toLocaleString("it-IT", {
    day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit",
  });
}

export function avg(arr: number[]): number | null {
  return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
}

export function minPrice(items: Record<string, any>[]): number | null {
  const prices = items.map((p) => p.price ?? p.last_price).filter((v) => v != null) as number[];
  return prices.length ? Math.min(...prices) : null;
}

export function fmtPrice(val: number | null | undefined): string {
  return val != null ? "€ " + val.toFixed(2) : "—";
}
