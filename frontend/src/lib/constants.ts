// Costanti e lookup portate da viewer/app.js (verbatim per parità di classificazione).
export const CONSOLE_FAMILIES = [
  { key: "series", label: "Series", re: /\bxbox\s+series(?:\s*[xs])?\b|\bseries\s*[xs]\b/i },
  { key: "one", label: "One", re: /\bxbox\s+one(?:\s*[xs])?\b|\bone\s*[xs]\b|\bone\b/i },
  { key: "360", label: "360", re: /\bxbox\s*360\b|\bxbox360\b|\b360\s*[se]?\b/i },
  { key: "original", label: "Original", re: /\boriginal\b|\bxbox\s+classic\b/i },
] as const;

export const STRIP_RE: Record<string, RegExp> = {
  series: /^(Microsoft\s+)?(Console\s+)?Xbox\s+Series\s*/i,
  "series-x": /^(Microsoft\s+)?(Console\s+)?Xbox\s+Series\s+X\s*/i,
  "series-s": /^(Microsoft\s+)?(Console\s+)?Xbox\s+Series\s+S\s*/i,
  one: /^(Microsoft\s+)?(Console\s+)?Xbox\s+One\s*/i,
  "one-x": /^(Microsoft\s+)?(Console\s+)?Xbox\s+One\s+X\s*/i,
  "one-s": /^(Microsoft\s+)?(Console\s+)?Xbox\s+One\s+S\s*/i,
  "360": /^(Microsoft\s+)?(Console\s+)?Xbox\s+(360\s*|250\s*GB\s*)/i,
  original: /^(Microsoft\s+)?(Console\s+)?Xbox\s*/i,
};

export const STORE_META: Record<string, { label: string; accent: string }> = {
  gamelife: { label: "GameLife", accent: "#82b8d8" },
  gameshock: { label: "GameShock", accent: "#82c882" },
  gamepeople: { label: "GamePeople", accent: "#c082d0" },
  rebuy: { label: "ReBuy", accent: "#d4a870" },
  cex: { label: "CEX", accent: "#e6b800" },
  subito: { label: "Subito.it", accent: "#ff6600" },
};

export const FAMILY_LABELS: Record<string, string> = {
  series: "Xbox Series", "series-x": "Xbox Series X", "series-s": "Xbox Series S",
  one: "Xbox One", "one-x": "Xbox One X", "one-s": "Xbox One S",
  "360": "Xbox 360", original: "Xbox Original", other: "???",
};

export const MANUAL_OVERRIDES: Record<number, { s?: string; c?: string; e?: string }> = {
  1122: { e: "Digital" }, 1123: { e: "Digital" }, 1124: { e: "Digital" },
  1154: { e: "Halo Infinite" }, 1155: { e: "Halo Infinite" }, 1156: { e: "Halo Infinite" },
  2021: { c: "Bianco" }, 2022: { c: "Bianco" }, 2029: { e: "Anthem" }, 2030: { e: "Anthem" },
  2037: { e: "Digital" }, 2038: { e: "Digital" }, 2039: { c: "Bianco" }, 2040: { c: "Bianco" },
  2041: { c: "Bianco" }, 2042: { c: "Bianco" }, 2049: { c: "Nero" }, 2050: { c: "Nero" },
  2065: { s: "512 GB", c: "Bianco" }, 2066: { s: "512 GB", c: "Bianco" },
  2067: { s: "512 GB" }, 2068: { s: "512 GB" }, 2069: { s: "512 GB" }, 2070: { s: "512 GB" },
  2071: { s: "512 GB", c: "Bianco" }, 2072: { s: "512 GB", c: "Bianco" },
  2076: { s: "1 TB", c: "Nero" }, 2077: { s: "1 TB", c: "Nero" },
  2078: { s: "1 TB" }, 2079: { s: "1 TB" }, 2080: { s: "1 TB" },
  2081: { s: "1 TB", e: "Diablo IV" }, 2082: { s: "1 TB" }, 2083: { s: "1 TB" },
  3002: { c: "Bianco" }, 3003: { e: "Diablo IV" }, 3004: { c: "Nero" }, 3032: { c: "Nero" },
  4018: { c: "Bianco" }, 4019: { s: "1 TB", c: "Bianco" },
  4031: { s: "512 GB", c: "Bianco" }, 4032: { s: "1 TB", c: "Nero" },
};

export const PARTS_RE = /ricamb[io]|per pezzi|for parts|non funzionante|rott[ao]|difettosa|malfunzionante|cannibalizz/i;
