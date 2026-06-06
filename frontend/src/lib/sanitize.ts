// Sanitizzazione difensiva (portata da viewer/modules/sanitize.js).
// Preact escapa già il testo nei nodi JSX, ma manteniamo la pulizia per URL e
// per i pochi punti con dangerouslySetInnerHTML (grafici SVG).
export function sanitizeText(value: unknown): string {
  if (value == null) return "";
  return String(value)
    .replace(/[<>"'`]/g, " ")
    .replace(/[\x00-\x1f\x7f]/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

export function sanitizeUrl(value: unknown): string {
  const raw = sanitizeText(value);
  if (!raw) return "";
  try {
    const parsed = new URL(raw, window.location.origin);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return "";
    return parsed.href;
  } catch {
    return "";
  }
}

function sanitizeValue(key: string, value: unknown): unknown {
  if (value == null) return value;
  if (Array.isArray(value)) return value.map((item) => sanitizeValue("", item));
  if (typeof value === "object") return sanitizeRecord(value as Record<string, unknown>);
  if (typeof value === "string") {
    return key.toLowerCase().includes("url") ? sanitizeUrl(value) : sanitizeText(value);
  }
  return value;
}

export function sanitizeRecord<T extends Record<string, unknown>>(record: T): T {
  if (!record || typeof record !== "object") return {} as T;
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(record)) out[key] = sanitizeValue(key, value);
  return out as T;
}

export function sanitizeCollection<T extends Record<string, unknown>>(items: T[]): T[] {
  if (!Array.isArray(items)) return [];
  return items.map((item) => sanitizeRecord(item));
}
