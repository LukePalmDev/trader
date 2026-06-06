// Arricchimento prodotti (port verbatim da viewer/app.js per parità classificazione).
/* eslint-disable @typescript-eslint/no-explicit-any */
import { CONSOLE_FAMILIES, STRIP_RE, STORE_META, FAMILY_LABELS, MANUAL_OVERRIDES } from "./constants";

export type Product = Record<string, any>;

export function familyKey(name: unknown): string {
  const n = String(name || "").toLowerCase();
  if (!n.trim()) return "other";
  const specificity: Record<string, number> = { series: 4, one: 3, "360": 2 };
  const candidates: { key: string; idx: number; spec: number }[] = [];
  for (const f of CONSOLE_FAMILIES) {
    if (f.key === "original") continue;
    const idx = n.search(f.re);
    if (idx >= 0) candidates.push({ key: f.key, idx, spec: specificity[f.key] || 0 });
  }
  if (candidates.length) {
    candidates.sort((a, b) => a.idx - b.idx || b.spec - a.spec);
    return candidates[0].key;
  }
  if (/\boriginal\b|\bxbox\s+classic\b/i.test(n)) return "original";
  if (/\bxbox\b/i.test(n)) return "original";
  return "other";
}

export function canonicalFamilyKey(key: unknown): string {
  const fk = String(key || "").toLowerCase().trim();
  if (fk === "series-x" || fk === "series-s") return "series";
  if (fk === "one-x" || fk === "one-s") return "one";
  if (fk === "series" || fk === "one" || fk === "360" || fk === "original") return fk;
  return fk || "other";
}

export function shortName(name: string, fk: string): string {
  const re = STRIP_RE[fk];
  if (!re) return name;
  return (
    name.replace(re, "").replace(/^[\s[(\-,]+/, "").replace(/[\s\])]+$/, "").trim() || "Base"
  );
}

export const storeLabel = (id: string): string => STORE_META[id]?.label ?? id;
export const storeAccent = (id: string): string => STORE_META[id]?.accent ?? "#cccccc";

export function bibleId(row: Product): string {
  const id = row?.bible_id || row?.ai_taxonomy_id || row?.canonical_model || "";
  return id && id !== "other" && id !== "unknown" ? String(id) : "";
}

export function bibleLabel(row: Product): string {
  if (row?.bible_label) return row.bible_label;
  const id = bibleId(row);
  if (id) return id;
  return FAMILY_LABELS[row?.console_family || "other"] || row?.console_family || "Altro";
}

export function subitoPublicId(row: Product): string {
  const raw = String(row?.urn_id || row?.id || "");
  const match = raw.match(/(\d{5,})$/);
  return match ? match[1] : raw;
}

export function enhanceProduct(p: Product): Product {
  if (p._enhanced) return p;

  const ov = MANUAL_OVERRIDES[p.display_id as number];
  if (ov) {
    if (ov.s) p.storage_label = ov.s;
    if (ov.c) p._manual_color = ov.c;
    if (ov.e) p._manual_edition = ov.e;
  }

  const rawFamily = p.console_family || familyKey(p.name);
  const fk = canonicalFamilyKey(rawFamily);
  p.console_family = fk;

  let pf = "Altro";
  if (fk === "series") pf = "Serie";
  else if (fk === "one") pf = "One";
  else if (fk === "360") pf = "360";
  else if (fk === "original") pf = "Original";

  const t = (p.name || "").toLowerCase();
  let ps = p.sub_model || "Base";
  if (ps === "Slim") ps = "S";
  if (!p.sub_model && (rawFamily === "series-x" || rawFamily === "one-x")) ps = "X";
  else if (!p.sub_model && (rawFamily === "series-s" || rawFamily === "one-s")) ps = "S";
  else if (!p.sub_model && fk === "series") {
    if (/\bseries\s*x\b|\bserie\s+x\b/.test(t)) ps = "X";
    else if (/\bseries\s*s\b|\bserie\s+s\b/.test(t)) ps = "S";
    else ps = "Unknown";
  } else if (!p.sub_model && fk === "one") {
    if (/\bone\s*x\b/.test(t)) ps = "X";
    else if (/\bone\s*s\b/.test(t)) ps = "S";
    else ps = "Base";
  } else if (!p.sub_model && fk === "360") {
    if (/\b(?:xbox\s*)?360\s*"?[eE]"?\b/.test(t)) ps = "E";
    else if (/\b360\s*slim\b|\bslim\b|\b360s\b/.test(t)) ps = "S";
    else if (/\belite\b/.test(t)) ps = "Elite";
  } else if (!p.sub_model && fk === "one") {
    if (/\belite\b/.test(t)) ps = "Elite";
  }

  let pk = false;
  if (/\bkinect\b/.test(t) && !/\b(?:no|senza)\s+kinect\b|\(no\s+kinect\)/.test(t)) pk = true;

  const colors: string[] = [];
  const isMinecraft = /\bminecraft\b/.test(t);
  const isGears = /\bgears\b/.test(t);
  const isGoldRush = /\bgold\s*rush\b/.test(t);
  if (/\brosso\b|\bred\b/.test(t) && !isGears) colors.push("Rosso");
  if (/\bblu\b|\bblue\b/.test(t)) colors.push("Blu");
  if (/\bverde\b|\bgreen\b/.test(t) && !isMinecraft) colors.push("Verde");
  if (/\bbianc[oa]\b|\bwhite\b/.test(t) && !isGears) colors.push("Bianco");
  if (/\bnero\b|\bblack\b/.test(t)) colors.push("Nero");
  if (/\bgrigio\b|\bgrey\b|\bgray\b/.test(t) && !isGears && !isGoldRush) colors.push("Grigio");
  if (/\bcristallo\b|\bcrystal\b/.test(t)) colors.push("Cristallo");
  if (/\bviola\b|\bpurple\b/.test(t)) colors.push("Viola");
  if (/\boro\b|\bgold\b/.test(t) && !isGoldRush) colors.push("Oro");
  if (p._manual_color && !colors.includes(p._manual_color)) colors.push(p._manual_color);
  const parsed_color = colors.length ? colors.join(", ") : "";

  let specialEd = "";
  if (/\b(?:call\s+of\s+duty|cod|mw2|mw3|advanced\s+warfare|black\s+ops)\b/.test(t)) specialEd = "Call Of Duty";
  else if (/\bhalo\b/.test(t)) specialEd = "Halo";
  else if (isGears) specialEd = "Gears Of War";
  else if (/\bforza(?:\s+motorsport|\s*horizon)?\b/.test(t)) specialEd = "Forza";
  else if (isMinecraft) specialEd = "Minecraft";
  else if (/\bcyberpunk\b/.test(t)) specialEd = "Cyberpunk 2077";
  else if (/\bbattlefield\b/.test(t)) specialEd = "Battlefield";
  else if (/\bstar\s+wars|r2-?d2\b/.test(t)) specialEd = "Star Wars";
  else if (/\bfortnite\b/.test(t)) specialEd = "Fortnite";
  else if (/\bproject\s+scorpio\b/.test(t)) specialEd = "Project Scorpio";
  else if (/\bresident\s+evil|re5\b/.test(t)) specialEd = "Resident Evil";
  else if (/\bsimpsons?\b/.test(t)) specialEd = "The Simpsons";
  else if (/\bmountain\s+dew\b/.test(t)) specialEd = "Mountain Dew";
  else if (/\btaco\s+bell\b/.test(t)) specialEd = "Taco Bell";
  else if (/\bhyperspace\b/.test(t)) specialEd = "Hyperspace";
  else if (/\bdeep\s+blue\b/.test(t)) specialEd = "Deep Blue";
  else if (/\brobot\s+white\b/.test(t)) specialEd = "Robot White";
  else if (/\bday\s+one\b/.test(t)) specialEd = "Day One";
  else if (/\bconker\b/.test(t)) specialEd = "Conker";
  else if (/\bskeleton\b/.test(t)) specialEd = "Skeleton";
  else if (/\bkasumi\b/.test(t)) specialEd = "Kasumi-Chan";
  else if (/\bpanzer\s+dragoon\b/.test(t)) specialEd = "Panzer Dragoon";
  else if (isGoldRush) specialEd = "Gold Rush";
  else if (/\bdiablo\b/.test(t)) specialEd = "Diablo";
  else if (/\bcelebrity\b/.test(t)) specialEd = "Celebrity";
  else if (/(?:bundle|\+\s*.+gioco|\+\s*.+game)/i.test(t) && t.indexOf("controller") === -1) specialEd = "Bundle";

  let ed = p._manual_edition || specialEd || p.edition_class || "standard";
  ed = ed.charAt(0).toUpperCase() + ed.slice(1);
  const parsed_edition = p._manual_edition ? p._manual_edition : parsed_color ? parsed_color : ed;

  p.parsed_family = pf;
  p.parsed_segment = ps;
  p.parsed_kinect = pk;
  p.parsed_color = parsed_color;
  p.parsed_edition = parsed_edition;

  if (!p.storage_label) {
    const tb = t.match(/(\d+(?:[.,]\d+)?)\s*tb/i);
    if (tb) p.storage_label = tb[1].replace(",", ".") + " TB";
    else {
      const gb = t.match(/(\d+(?:[.,]\d+)?)\s*gb/i);
      if (gb) p.storage_label = gb[1].replace(",", ".") + " GB";
    }
  }

  const storage = p.storage_label || "—";
  const condition = p.condition || (p.seller_type ? "Usato" : "—");
  const pack = p.packaging_state || "Imballata";
  p.combo_key = `${pf}|${ps}|${parsed_edition}|${pk}|${storage}|${condition}|${pack}`;
  p._enhanced = true;
  return p;
}
