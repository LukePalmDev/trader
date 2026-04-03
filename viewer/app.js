'use strict';

// ============================================================
// Configurazione
// ============================================================

const CONSOLE_FAMILIES = [
  { key: 'series-x', label: 'Series X', re: /series\s*x/i },
  { key: 'series-s', label: 'Series S', re: /series\s*s/i },
  { key: 'one-x',    label: 'One X',    re: /one\s*x/i    },
  { key: 'one-s',    label: 'One S',    re: /one\s*s/i    },
  { key: 'one',      label: 'One',      re: /\bone\b/i    },
  { key: '360',      label: '360',      re: /360/         },
  { key: 'original', label: 'Original', re: /\bxbox\b/i   },
];

const STRIP_RE = {
  'series-x': /^(Microsoft\s+)?(Console\s+)?Xbox\s+Series\s+X\s*/i,
  'series-s': /^(Microsoft\s+)?(Console\s+)?Xbox\s+Series\s+S\s*/i,
  'one-x':    /^(Microsoft\s+)?(Console\s+)?Xbox\s+One\s+X\s*/i,
  'one-s':    /^(Microsoft\s+)?(Console\s+)?Xbox\s+One\s+S\s*/i,
  'one':      /^(Microsoft\s+)?(Console\s+)?Xbox\s+One\s*/i,
  '360':      /^(Microsoft\s+)?(Console\s+)?Xbox\s+(360\s*|250\s*GB\s*)/i,
  'original': /^(Microsoft\s+)?(Console\s+)?Xbox\s*/i,
};

const STORE_META = {
  gamelife:   { label: 'GameLife',   accent: '#82b8d8' },
  gameshock:  { label: 'GameShock',  accent: '#82c882' },
  gamepeople: { label: 'GamePeople', accent: '#c082d0' },
  rebuy:      { label: 'ReBuy',      accent: '#d4a870' },
  cex:        { label: 'CEX',        accent: '#e6b800' },
  subito:     { label: 'Subito.it',  accent: '#ff6600' },
};

const FAMILY_LABELS = {
  'series-x': 'Xbox Series X',
  'series-s': 'Xbox Series S',
  'one-x':    'Xbox One X',
  'one-s':    'Xbox One S',
  'one':      'Xbox One',
  '360':      'Xbox 360',
  'original': 'Xbox Original',
  'other':    '???',
};

const VIEWER_STATE = window.ViewerState || {
  sourcesMeta: [],
  allProducts: [],
  allData: {},
  dbProducts: [],
  baseModels: [],
  storageSizes: [],
  subitoAds: [],
  subitoOpportunities: [],
  ebaySold: [],
  currentSort: { key: 'last_price', dir: 1 },
};

const SAN = window.ViewerSanitize || {
  sanitizeText: v => String(v ?? ''),
  sanitizeRecord: v => v || {},
  sanitizeCollection: v => (Array.isArray(v) ? v : []),
};

const API = window.ViewerApi || {
  fetchJson: async (url, options = {}) => {
    const res = await fetch(url, options);
    let body = null;
    try { body = await res.json(); } catch { body = null; }
    return { ok: res.ok, status: res.status, body };
  },
};

// ============================================================
// Stato globale
// ============================================================
let SOURCES_META   = VIEWER_STATE.sourcesMeta;
let ALL_PRODUCTS   = VIEWER_STATE.allProducts;   // da snapshot JSON (usato in "Tutto", escluso subito)
let PRICE_HISTORY  = [];                         // Storico prezzi globali (Statistiche 3,4,5)
let ALL_DATA       = VIEWER_STATE.allData;       // { source_id: { scraped_at, products[] } }
let DB_PRODUCTS    = VIEWER_STATE.dbProducts;    // da DB SQLite trader.db (usato in "Catalogo", escluso subito)
let BASE_MODELS    = VIEWER_STATE.baseModels;    // DB prodotti con is_base_model=1 (usato in "Home")
let STANDARD_GROUPS= VIEWER_STATE.standardGroups;// gruppi nome standard -> nomi originali
let STORAGE_SIZES  = VIEWER_STATE.storageSizes;  // dimensioni archiviazione dal DB
let SUBITO_ADS     = VIEWER_STATE.subitoAds;     // annunci Subito dal DB dedicato (subito.db)
let SUBITO_OPPS    = VIEWER_STATE.subitoOpportunities; // score fair-value/qualita'
let EBAY_SOLD      = VIEWER_STATE.ebaySold;      // lotti venduti eBay dal DB dedicato (ebay.db)
let currentSort    = VIEWER_STATE.currentSort;

// ============================================================
// Utilità
// ============================================================

/**
 * Estrae i valori da Promise.allSettled: fulfilled → valore, rejected → fallback.
 * Permette al viewer di caricare dati parziali se un endpoint fallisce.
 */
function _settled(results, fallbacks) {
  return results.map((r, i) => {
    if (r.status === 'fulfilled') return r.value;
    console.warn(`API #${i} fallita:`, r.reason?.message || r.reason);
    return fallbacks[i] !== undefined ? fallbacks[i] : { ok: false, body: null };
  });
}

function _familyKey(name) {
  const n = String(name || '').toLowerCase();
  if (!n.trim()) return 'other';

  const specificity = {
    'series-x': 4,
    'series-s': 4,
    'one-x': 3,
    'one-s': 3,
    'one': 2,
    '360': 2,
  };

  const candidates = [];
  for (const f of CONSOLE_FAMILIES) {
    if (f.key === 'original') continue; // fallback esplicito
    const idx = n.search(f.re);
    if (idx >= 0) candidates.push({ key: f.key, idx, spec: specificity[f.key] || 0 });
  }
  if (candidates.length) {
    candidates.sort((a, b) => (a.idx - b.idx) || (b.spec - a.spec));
    return candidates[0].key;
  }

  if (/\boriginal\b|\bxbox\s+classic\b/i.test(n)) return 'original';
  if (/\bxbox\b/i.test(n)) return 'original';
  return 'other';
}

function _shortName(name, familyKey) {
  const re = STRIP_RE[familyKey];
  if (!re) return name;
  return name
    .replace(re, '')
    .replace(/^[\s\[\(\-,]+/, '')
    .replace(/[\s\]\)]+$/, '')
    .trim() || 'Base';
}

function _storeLabel(id)  { return STORE_META[id]?.label  ?? id;        }
function _storeAccent(id) { return STORE_META[id]?.accent ?? '#cccccc'; }

const MANUAL_OVERRIDES = {
  1122: { e: 'Digital' },
  1123: { e: 'Digital' },
  1124: { e: 'Digital' },
  1154: { e: 'Halo Infinite' },
  1155: { e: 'Halo Infinite' },
  1156: { e: 'Halo Infinite' },
  2021: { c: 'Bianco' },
  2022: { c: 'Bianco' },
  2029: { e: 'Anthem' },
  2030: { e: 'Anthem' },
  2037: { e: 'Digital' },
  2038: { e: 'Digital' },
  2039: { c: 'Bianco' },
  2040: { c: 'Bianco' },
  2041: { c: 'Bianco' },
  2042: { c: 'Bianco' },
  2049: { c: 'Nero' },
  2050: { c: 'Nero' },
  2065: { s: '512 GB', c: 'Bianco' },
  2066: { s: '512 GB', c: 'Bianco' },
  2067: { s: '512 GB' },
  2068: { s: '512 GB' },
  2069: { s: '512 GB' },
  2070: { s: '512 GB' },
  2071: { s: '512 GB', c: 'Bianco' },
  2072: { s: '512 GB', c: 'Bianco' },
  2076: { s: '1 TB', c: 'Nero' },
  2077: { s: '1 TB', c: 'Nero' },
  2078: { s: '1 TB' },
  2079: { s: '1 TB' },
  2080: { s: '1 TB' },
  2081: { s: '1 TB', e: 'Diablo IV' },
  2082: { s: '1 TB' },
  2083: { s: '1 TB' },
  3002: { c: 'Bianco' },
  3003: { e: 'Diablo IV' },
  3004: { c: 'Nero' },
  3032: { c: 'Nero' },
  4018: { c: 'Bianco' },
  4019: { s: '1 TB', c: 'Bianco' },
  4031: { s: '512 GB', c: 'Bianco' },
  4032: { s: '1 TB', c: 'Nero' }
};

function enhanceProduct(p) {
  if (p._enhanced) return p;

  if (MANUAL_OVERRIDES[p.display_id]) {
    const ov = MANUAL_OVERRIDES[p.display_id];
    if (ov.s) p.storage_label = ov.s;
    if (ov.c) p._manual_color = ov.c;
    if (ov.e) p._manual_edition = ov.e;
  }

  const fk = p.console_family || _familyKey(p.name);
  let pf = 'Altro';
  if (fk === 'series-x' || fk === 'series-s') pf = 'Serie';
  else if (fk === 'one' || fk === 'one-s' || fk === 'one-x') pf = 'One';
  else if (fk === '360') pf = '360';
  else if (fk === 'original') pf = 'Original';

  const t = (p.name || '').toLowerCase();
  let ps = 'Base';
  if (fk === 'series-x' || fk === 'one-x') ps = 'X';
  else if (fk === 'series-s' || fk === 'one-s') ps = 'S';
  else if (fk === '360') {
    if (/\b(?:xbox\s*)?360\s*"?[eE]"?\b/.test(t)) ps = 'E';
    else if (/\b360\s*slim\b|\bslim\b|\b360s\b/.test(t)) ps = 'Slim';
    else if (/\belite\b/.test(t)) ps = 'Elite';
  }
  else if (fk === 'one') {
    if (/\belite\b/.test(t)) ps = 'Elite';
  }

  let pk = false;
  if (/\bkinect\b/.test(t) && !/\b(?:no|senza)\s+kinect\b|\(no\s+kinect\)/.test(t)) {
    pk = true;
  }

  const colors = [];
  const isMinecraft = /\bminecraft\b/.test(t);
  const isGears = /\bgears\b/.test(t);
  const isGoldRush = /\bgold\s*rush\b/.test(t);

  if (/\brosso\b|\bred\b/.test(t) && !isGears) colors.push('Rosso');
  if (/\bblu\b|\bblue\b/.test(t)) colors.push('Blu');
  if (/\bverde\b|\bgreen\b/.test(t) && !isMinecraft) colors.push('Verde');
  if (/\bbianc[oa]\b|\bwhite\b/.test(t) && !isGears) colors.push('Bianco');
  if (/\bnero\b|\bblack\b/.test(t)) colors.push('Nero');
  if (/\bgrigio\b|\bgrey\b|\bgray\b/.test(t) && !isGears && !isGoldRush) colors.push('Grigio');
  if (/\bcristallo\b|\bcrystal\b/.test(t)) colors.push('Cristallo');
  if (/\bviola\b|\bpurple\b/.test(t)) colors.push('Viola');
  if (/\boro\b|\bgold\b/.test(t) && !isGoldRush) colors.push('Oro');

  if (p._manual_color && !colors.includes(p._manual_color)) {
    colors.push(p._manual_color);
  }

  const parsed_color = colors.length ? colors.join(', ') : '';

  let specialEd = '';
  if (/\b(?:call\s+of\s+duty|cod|mw2|mw3|advanced\s+warfare|black\s+ops)\b/.test(t)) specialEd = 'Call Of Duty';
  else if (/\bhalo\b/.test(t)) specialEd = 'Halo';
  else if (isGears) specialEd = 'Gears Of War';
  else if (/\bforza(?:\s+motorsport|\s*horizon)?\b/.test(t)) specialEd = 'Forza';
  else if (isMinecraft) specialEd = 'Minecraft';
  else if (/\bcyberpunk\b/.test(t)) specialEd = 'Cyberpunk 2077';
  else if (/\bbattlefield\b/.test(t)) specialEd = 'Battlefield';
  else if (/\bstar\s+wars|r2-?d2\b/.test(t)) specialEd = 'Star Wars';
  else if (/\bfortnite\b/.test(t)) specialEd = 'Fortnite';
  else if (/\bproject\s+scorpio\b/.test(t)) specialEd = 'Project Scorpio';
  else if (/\bresident\s+evil|re5\b/.test(t)) specialEd = 'Resident Evil';
  else if (/\bsimpsons?\b/.test(t)) specialEd = 'The Simpsons';
  else if (/\bmountain\s+dew\b/.test(t)) specialEd = 'Mountain Dew';
  else if (/\btaco\s+bell\b/.test(t)) specialEd = 'Taco Bell';
  else if (/\bhyperspace\b/.test(t)) specialEd = 'Hyperspace';
  else if (/\bdeep\s+blue\b/.test(t)) specialEd = 'Deep Blue';
  else if (/\brobot\s+white\b/.test(t)) specialEd = 'Robot White';
  else if (/\bday\s+one\b/.test(t)) specialEd = 'Day One';
  else if (/\bconker\b/.test(t)) specialEd = 'Conker';
  else if (/\bskeleton\b/.test(t)) specialEd = 'Skeleton';
  else if (/\bkasumi\b/.test(t)) specialEd = 'Kasumi-Chan';
  else if (/\bpanzer\s+dragoon\b/.test(t)) specialEd = 'Panzer Dragoon';
  else if (isGoldRush) specialEd = 'Gold Rush';
  else if (/\bdiablo\b/.test(t)) specialEd = 'Diablo';
  else if (/\bcelebrity\b/.test(t)) specialEd = 'Celebrity';
  else if (/(?:bundle|\+\s*.+gioco|\+\s*.+game)/i.test(t) && t.indexOf('controller') === -1) specialEd = 'Bundle';

  let ed = p._manual_edition || specialEd || p.edition_class || 'standard';
  ed = ed.charAt(0).toUpperCase() + ed.slice(1);
  const parsed_edition = p._manual_edition ? p._manual_edition : (parsed_color ? parsed_color : ed);

  p.parsed_family = pf;
  p.parsed_segment = ps;
  p.parsed_kinect = pk;
  p.parsed_color = parsed_color;
  p.parsed_edition = parsed_edition;

  p.parsed_edition = parsed_edition;

  if (!p.storage_label) {
    const tb = t.match(/(\d+(?:[.,]\d+)?)\s*tb/i);
    if (tb) p.storage_label = tb[1].replace(',', '.') + ' TB';
    else {
      const gb = t.match(/(\d+(?:[.,]\d+)?)\s*gb/i);
      if (gb) p.storage_label = gb[1].replace(',', '.') + ' GB';
    }
  }

  const storage = p.storage_label || '—';
  const condition = p.condition || (p.seller_type ? 'Usato' : '—'); // Se ha seller_type è un ad Subito
  const pack = p.packaging_state || 'Imballata';

  p.combo_key = `${pf}|${ps}|${parsed_edition}|${pk}|${storage}|${condition}|${pack}`;
  p._enhanced = true;
  return p;
}

function _fmtDate(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('it-IT', {
    day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit',
  });
}

function _avg(arr) {
  return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
}

function _minPrice(items) {
  const prices = items.map(p => p.price ?? p.last_price).filter(v => v != null);
  return prices.length ? Math.min(...prices) : null;
}

function _fmtPrice(val) {
  return val != null ? '€ ' + val.toFixed(2) : '—';
}

function _sanitizeRows(items) {
  return SAN.sanitizeCollection(items || []);
}

function _sanitizeRow(item) {
  return SAN.sanitizeRecord(item || {});
}

function _syncState() {
  VIEWER_STATE.sourcesMeta = SOURCES_META;
  VIEWER_STATE.allProducts = ALL_PRODUCTS;
  VIEWER_STATE.allData = ALL_DATA;
  VIEWER_STATE.dbProducts = DB_PRODUCTS;
  VIEWER_STATE.baseModels = BASE_MODELS;
  VIEWER_STATE.standardGroups = STANDARD_GROUPS;
  VIEWER_STATE.storageSizes = STORAGE_SIZES;
  VIEWER_STATE.subitoAds = SUBITO_ADS;
  VIEWER_STATE.subitoOpportunities = SUBITO_OPPS;
  VIEWER_STATE.ebaySold = EBAY_SOLD;
  VIEWER_STATE.currentSort = currentSort;
}

// ============================================================
// Navigazione tab
// ============================================================
document.querySelectorAll('.tab').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('view-' + btn.dataset.tab).classList.add('active');
    if (btn.dataset.tab === 'home')   renderHome();
    if (btn.dataset.tab === 'standardi') renderStandardGroups();
    if (btn.dataset.tab === 'subito') loadSubitoData();
    if (btn.dataset.tab === 'ebay')   loadEbayData();
    if (btn.dataset.tab === 'statistiche') renderStatistiche();
    if (btn.dataset.tab === 'trend') renderTrend();
    if (btn.dataset.tab === 'ricerca') initRicerca();
  });
});

// ============================================================
// Dark mode toggle con persistenza localStorage
// ============================================================
(function _initTheme() {
  // Dark è il tema predefinito — light solo se esplicitamente scelto
  const saved = localStorage.getItem('xbox-tracker-theme');
  if (saved !== 'light') document.documentElement.setAttribute('data-theme', 'dark');
  const btn = document.getElementById('btn-theme');
  if (!btn) return;
  _updateThemeIcon();
  btn.addEventListener('click', () => {
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    if (isDark) {
      document.documentElement.removeAttribute('data-theme');
      localStorage.setItem('xbox-tracker-theme', 'light');
    } else {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('xbox-tracker-theme', 'dark');
    }
    _updateThemeIcon();
  });
})();

function _updateThemeIcon() {
  const btn = document.getElementById('btn-theme');
  if (!btn) return;
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  btn.textContent = isDark ? '☀️' : '🌙';
  btn.title = isDark ? 'Passa al tema chiaro' : 'Passa al tema scuro';
}

// ============================================================
// Helpers condivisi per Home/Mercato
// ============================================================
function _comboCondOrder(combo) {
  const c = combo.rep.condition || '';
  const p = (combo.rep.packaging_state || '').toLowerCase();
  if (c === 'Nuovo') return 1;
  if (c === 'Usato' && p.includes('non imball')) return 3;
  if (c === 'Usato' && p.includes('imball')) return 2;
  return 3;
}

function _comboTitle(combo) {
  const r = combo.rep;
  const kinect = r.parsed_kinect ? ' + Kinect' : '';
  const ed = r.parsed_edition && r.parsed_edition.toLowerCase() !== 'standard' ? ` ${r.parsed_edition}` : '';
  return `Xbox ${r.parsed_family || ''} ${r.parsed_segment || ''}${ed}${kinect}`.trim();
}

function _comboStorageGb(combo) {
  const s = (combo.rep.storage_label || '').toLowerCase().trim();
  const tb = s.match(/(\d+(\.\d+)?)\s*tb/);
  if (tb) return parseFloat(tb[1]) * 1024;
  const gb = s.match(/(\d+(\.\d+)?)\s*gb/);
  if (gb) return parseFloat(gb[1]);
  return 0;
}

function _comboShortTitle(rep) {
  const kinectStr = rep.parsed_kinect ? ' + Kinect' : '';
  const edStr = rep.parsed_edition && rep.parsed_edition.toLowerCase() !== 'standard' ? ` ${rep.parsed_edition}` : '';
  return `${rep.parsed_segment || ''}${edStr}${kinectStr}`.trim() || `Xbox ${rep.parsed_family}`;
}

function _comboPackBadge(rep) {
  const packState = (rep.packaging_state || '').toLowerCase();
  if (packState.includes('non imball')) return '<span class="badge-pack badge-pack-nonimb">Non Imballata</span>';
  if (packState.includes('imball')) return '<span class="badge-pack badge-pack-imb">Imballata</span>';
  return '';
}

function _renderSourceCell(p) {
  const storeLabel = _storeLabel(p.source);
  const accent = _storeAccent(p.source);
  return p.url
    ? `<a href="${p.url}" target="_blank" rel="noopener" title="${p.name}" style="text-decoration:none;"><span class="badge-source" style="border-color:${accent}40; color:var(--text)">${storeLabel} ↗</span></a>`
    : `<span class="badge-source" style="border-color:${accent}40; color:var(--text)">${storeLabel}</span>`;
}

/**
 * Funzione unificata per renderizzare la griglia base models.
 * @param {Object} opts
 * @param {string} opts.gridId       — ID del container grid
 * @param {string} opts.emptyId      — ID del messaggio empty
 * @param {boolean} opts.availOnly   — true = mostra solo combo con almeno 1 disponibile (Mercato)
 */
function _renderBaseModelGrid({ gridId, emptyId, availOnly }) {
  const grid  = document.getElementById(gridId);
  const empty = document.getElementById(emptyId);
  grid.innerHTML = '';

  if (!BASE_MODELS.length) {
    empty.style.display = availOnly ? 'block' : '';
    grid.style.display  = availOnly ? '' : 'none';
    return;
  }

  empty.style.display = 'none';
  grid.style.display  = '';

  const uniqueGroups = getUniqueBaseModels();

  const byFamily = {};
  const totalByFamily = {};
  for (const group of uniqueGroups) {
    const matching = group.items;
    if (!matching.length) continue;

    const rep = matching[0];
    const fk = rep.console_family || _familyKey(rep.name);
    if (!totalByFamily[fk]) totalByFamily[fk] = 0;
    totalByFamily[fk]++;

    if (availOnly) {
      const availItems = matching.filter(p => p.last_available || p.available);
      if (!availItems.length) continue;
      if (!byFamily[fk]) byFamily[fk] = [];
      byFamily[fk].push({ key: group.key, rep, matching, availItems });
    } else {
      if (!byFamily[fk]) byFamily[fk] = [];
      byFamily[fk].push({ key: group.key, rep, matching });
    }
  }

  if (availOnly) {
    const hasAny = Object.values(byFamily).some(arr => arr.length > 0);
    if (!hasAny) { empty.style.display = 'block'; return; }
  }

  const familyOrder = [...CONSOLE_FAMILIES.map(f => f.key), 'other'];
  for (const fk of familyOrder) {
    const combos = byFamily[fk];
    if (!combos?.length) continue;

    // Counter label
    const countLabel = availOnly
      ? `${combos.length}/${totalByFamily[fk]}`
      : `${combos.filter(c => c.matching.some(p => p.last_available || p.available)).length}/${combos.length}`;

    const section = document.createElement('div');
    section.className = 'home-section';
    const title = document.createElement('h2');
    title.className = 'home-section-title';
    title.innerHTML = `${FAMILY_LABELS[fk] || fk} <span style="font-size:16px; font-weight:normal; color:var(--text-muted); margin-left:12px;">${countLabel}</span>`;
    section.appendChild(title);

    const container = document.createElement('div');
    container.className = 'home-combos-container';

    combos.sort((a, b) => {
      const diff = _comboCondOrder(a) - _comboCondOrder(b);
      if (diff !== 0) return diff;
      const titleDiff = _comboTitle(a).localeCompare(_comboTitle(b), 'it');
      if (titleDiff !== 0) return titleDiff;
      return _comboStorageGb(b) - _comboStorageGb(a);
    });

    for (const combo of combos) {
      const { rep, matching } = combo;

      // Items da mostrare nella tabella dettagli (solo fonti store, no Subito/eBay)
      const STORE_SOURCES = new Set(['cex', 'gamelife', 'gamepeople', 'gameshock', 'rebuy']);
      const displayItems = availOnly
        ? [...combo.availItems].filter(p => STORE_SOURCES.has(p.source)).sort((a, b) => (a.last_price ?? a.price ?? Infinity) - (b.last_price ?? b.price ?? Infinity))
        : [...matching].filter(p => STORE_SOURCES.has(p.source)).sort((a, b) => {
            const da = (a.last_available || a.available) ? 0 : 1;
            const db2 = (b.last_available || b.available) ? 0 : 1;
            if (da !== db2) return da - db2;
            return (a.last_price ?? a.price ?? Infinity) - (b.last_price ?? b.price ?? Infinity);
          });

      const prices = displayItems.map(p => p.last_price ?? p.price).filter(v => v != null);
      const minPx = prices.length ? Math.min(...prices) : null;
      const maxPx = prices.length ? Math.max(...prices) : null;

      const shortTitle = _comboShortTitle(rep);
      const packBadge = _comboPackBadge(rep);

      // Stats line
      const statsLine = availOnly
        ? `<div style="color:var(--avail-ok); font-weight:600;">${displayItems.length} disponibili</div>`
        : `<div style="color:var(--text);">${displayItems.filter(p => p.last_available || p.available).length} store disponibili</div>`;
      const priceLine = prices.length
        ? (prices.length > 1 ? `da €${Math.round(minPx)} a €${Math.round(maxPx)}` : `€${Math.round(minPx)}`)
        : (availOnly ? '—' : 'esaurito');

      const row = document.createElement('div');
      row.className = 'home-combo-row';

      const header = document.createElement('div');
      header.className = 'home-combo-header';
      header.onclick = () => row.classList.toggle('open');

      header.innerHTML = `
        <div>
          <div class="home-combo-title">${shortTitle}</div>
          <div class="home-combo-tags">
            <span class="badge ${rep.condition === 'Nuovo' ? 'badge-nuovo' : rep.condition === 'Usato' ? 'badge-usato' : 'badge-nd'}">${rep.condition}</span>
            <span class="storage-badge">${rep.storage_label || '—'}</span>
            ${packBadge}
          </div>
        </div>
        <div class="home-combo-stats">
          ${statsLine}
          <div style="color:var(--text-muted); font-size: 12px; margin-top:2px;">${priceLine}</div>
        </div>
      `;

      const details = document.createElement('div');
      details.className = 'home-combo-details';

      const table = document.createElement('table');
      table.className = 'home-table';
      table.innerHTML = '<thead><tr><th>Fonte</th><th style="text-align:right;">Prezzo</th><th style="text-align:center;">Disp.</th></tr></thead>';
      const tbody = document.createElement('tbody');

      for (const p of displayItems) {
        const tr = document.createElement('tr');
        const isA = p.last_available || p.available;
        const px = p.last_price ?? p.price;
        tr.innerHTML = `
          <td>${_renderSourceCell(p)}</td>
          <td style="text-align:right; font-weight:600;">${px != null ? '€ ' + Math.round(px) : '—'}</td>
          <td style="text-align:center;"><span class="avail-dot ${isA ? 'ok': 'ko'}"></span></td>
        `;
        tbody.appendChild(tr);
      }

      table.appendChild(tbody);
      details.appendChild(table);
      row.appendChild(header);
      row.appendChild(details);
      container.appendChild(row);
    }

    section.appendChild(container);
    grid.appendChild(section);
  }
}

// ============================================================
// HOME VIEW
// ============================================================
function renderHome() {
  const availOnly = document.getElementById('home-filter-avail')?.checked ?? false;
  _renderBaseModelGrid({ gridId: 'home-grid', emptyId: 'home-empty', availOnly });
}

// ============================================================
// SHOP VIEW (Tutto) — identico a prima
// ============================================================
function renderShop() {
  _renderStoreCards();
  _renderGlobalStats();
  _renderConsoleSummary();
}

function _renderStoreCards() {
  const grid = document.getElementById('shop-grid');
  grid.innerHTML = '';

  const sources = SOURCES_META.filter(s => ALL_DATA[s.id]).map(s => s.id);
  if (!sources.length) {
    grid.innerHTML = '<div class="empty-shop">Nessun dato — esegui <code>python3 run.py --all</code></div>';
    return;
  }

  for (const srcId of sources) {
    if (srcId === 'subito') continue;   // Subito ha la sua sezione dedicata
    const { scraped_at, products } = ALL_DATA[srcId];
    const meta   = STORE_META[srcId] ?? { label: srcId, accent: '#ccc' };
    const availN = products.filter(p => p.available !== false).length;

    const byFamily = {};
    for (const p of products) {
      const fk = _familyKey(p.name);
      if (!byFamily[fk]) byFamily[fk] = [];
      byFamily[fk].push(p);
    }

    for (const fk of Object.keys(byFamily)) {
      if (srcId === 'gamelife') {
        const condOrder = c => c === 'Nuovo' ? 0 : c === 'Usato' ? 1 : 2;
        byFamily[fk].sort((a, b) => {
          const nameCmp = a.name.localeCompare(b.name, 'it');
          if (nameCmp !== 0) return nameCmp;
          return condOrder(a.condition) - condOrder(b.condition);
        });
      } else if (srcId === 'cex') {
        const gradeOrder = g => g === 'Imballata' ? 0 : g === 'Non Imballata' ? 1 : g === 'Scontata' ? 2 : 3;
        const _cexBase = n => n.replace(/,?\s*(Imballata|Non Imballata|Scontata)\s*$/i, '').trim();
        byFamily[fk].sort((a, b) => {
          const nameCmp = _cexBase(a.name).localeCompare(_cexBase(b.name), 'it');
          if (nameCmp !== 0) return nameCmp;
          return gradeOrder(a.grade || '') - gradeOrder(b.grade || '');
        });
      } else {
        byFamily[fk].sort((a, b) => {
          const da = a.available !== false ? 0 : 1;
          const db = b.available !== false ? 0 : 1;
          if (da !== db) return da - db;
          return (a.price ?? Infinity) - (b.price ?? Infinity);
        });
      }
    }

    const card = document.createElement('div');
    card.className = 'store-card';
    card.style.setProperty('--store-accent', meta.accent);

    const header = document.createElement('div');
    header.className = 'store-card-header';
    header.innerHTML = `
      <span class="store-name">${meta.label}</span>
      <button class="btn-filter-avail" title="Mostra solo disponibili">Disp</button>
      <span class="store-header-right">
        <span class="store-count">${availN} disponibili · ${products.length} totali</span>
        <span class="store-meta">Agg. ${_fmtDate(scraped_at)}</span>
      </span>
    `;
    header.querySelector('.btn-filter-avail').addEventListener('click', function() {
      _toggleAvailFilter(this);
    });
    card.appendChild(header);

    const table = document.createElement('table');
    table.className = 'console-table';

    const thead = document.createElement('thead');
    const thStyle = 'padding:6px 10px; font-size:9.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.7px; color:var(--text-muted);';
    thead.innerHTML = `<tr style="background:var(--bg); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
          <th style="${thStyle} padding-left:18px;">Prodotto</th>
          <th style="${thStyle} text-align:center;">Cond.</th>
          <th style="${thStyle} text-align:right; padding-right:18px;">Prezzo</th>
          <th style="${thStyle} text-align:center;">Disp.</th>
        </tr>`;
    table.appendChild(thead);

    const tbody     = document.createElement('tbody');
    let firstFamily = true;

    const familyOrder = [...CONSOLE_FAMILIES.map(f => f.key), 'other'];
    for (const fk of familyOrder) {
      const items = byFamily[fk];
      if (!items?.length) continue;

      const familyLabel = fk === 'other'
        ? '???'
        : 'Xbox ' + CONSOLE_FAMILIES.find(f => f.key === fk).label;
      const famAvail = items.filter(p => p.available !== false).length;

      const famRow = document.createElement('tr');
      famRow.className = 'family-header-row';
      const famTd = document.createElement('td');
      famTd.colSpan = 4;
      famTd.className = 'family-header-cell' + (firstFamily ? ' no-top-border' : '');
      famTd.innerHTML = `
        <div class="family-cell-inner">
          <span class="family-label">${familyLabel}</span>
          <span class="family-count">${famAvail} / ${items.length} pz</span>
        </div>
      `;
      famRow.appendChild(famTd);
      tbody.appendChild(famRow);
      firstFamily = false;

      for (const p of items) {
        const isAvail = p.available !== false;

        let shortName = fk === 'other' ? p.name : _shortName(p.name, fk);
        if (srcId === 'cex' && p.grade) {
          shortName = shortName.replace(new RegExp(',?\\s*' + p.grade.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*$', 'i'), '').replace(/,\s*$/, '').trim() || shortName;
        }

        let condClass, condLetter, condTitle;
        if (srcId === 'cex' && p.grade) {
          condClass  = p.grade === 'Imballata'     ? 'badge-nuovo'
                     : p.grade === 'Non Imballata' ? 'badge-usato' : 'badge-nd';
          condLetter = p.grade === 'Imballata'     ? 'IMB'
                     : p.grade === 'Non Imballata' ? 'NIB' : 'SCO';
          condTitle  = p.grade;
        } else {
          condClass  = p.condition === 'Nuovo' ? 'badge-nuovo' : p.condition === 'Usato' ? 'badge-usato' : 'badge-nd';
          condLetter = p.condition === 'Nuovo' ? 'N' : p.condition === 'Usato' ? 'U' : '?';
          condTitle  = p.condition || '';
        }

        const tr = document.createElement('tr');
        tr.className = 'product-row' + (isAvail ? '' : ' product-row-esaurito');

        const tdName = document.createElement('td');
        tdName.className = 'prod-name-cell';
        if (p.url) {
          const a = document.createElement('a');
          a.href = p.url; a.target = '_blank'; a.rel = 'noopener noreferrer';
          a.className = 'prod-link'; a.textContent = shortName; a.title = p.name;
          tdName.appendChild(a);
        } else {
          const span = document.createElement('span');
          span.className = 'prod-link'; span.textContent = shortName; span.title = p.name;
          tdName.appendChild(span);
        }
        tr.appendChild(tdName);

        const tdCond = document.createElement('td');
        tdCond.className = 'prod-cond-cell';
        const badge = document.createElement('span');
        badge.className = 'badge ' + condClass;
        badge.textContent = condLetter; badge.title = condTitle;
        tdCond.appendChild(badge);
        tr.appendChild(tdCond);

        const tdPrice = document.createElement('td');
        tdPrice.className = 'prod-price-cell';
        tdPrice.textContent = p.price != null ? '€ ' + p.price.toFixed(2) : '—';
        tr.appendChild(tdPrice);

        const tdAvail = document.createElement('td');
        tdAvail.className = 'prod-avail-cell';
        const dot = document.createElement('span');
        dot.className = 'avail-dot ' + (isAvail ? 'ok' : 'ko');
        dot.title = isAvail ? 'Disponibile' : 'Esaurito';
        tdAvail.appendChild(dot);
        tr.appendChild(tdAvail);

        tbody.appendChild(tr);
      }
    }

    table.appendChild(tbody);
    const tableWrapper = document.createElement('div');
    tableWrapper.className = 'console-table-wrapper';
    tableWrapper.appendChild(table);
    card.appendChild(tableWrapper);
    grid.appendChild(card);
  }
}

function _toggleAvailFilter(btn) {
  const isActive = btn.classList.toggle('active');
  btn.title = isActive ? 'Mostra tutti' : 'Mostra solo disponibili';
  const card = btn.closest('.store-card');
  const rows = Array.from(card.querySelectorAll('tbody tr'));

  rows.forEach(row => {
    if (row.classList.contains('product-row')) {
      row.style.display = (isActive && row.classList.contains('product-row-esaurito')) ? 'none' : '';
    }
  });

  rows.forEach(row => {
    if (!row.classList.contains('family-header-row')) return;
    let sib = row.nextElementSibling;
    let hasVisible = false;
    while (sib && !sib.classList.contains('family-header-row')) {
      if (sib.style.display !== 'none') { hasVisible = true; break; }
      sib = sib.nextElementSibling;
    }
    row.style.display = hasVisible ? '' : 'none';
  });
}

function _renderGlobalStats() {
  const avail   = ALL_PRODUCTS.filter(p => p.available !== false);
  const uPrices = avail.filter(p => p.condition === 'Usato').map(p => p.price).filter(v => v != null);
  const nPrices = avail.filter(p => p.condition === 'Nuovo').map(p => p.price).filter(v => v != null);
  const allPx   = avail.map(p => p.price).filter(v => v != null);

  const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  set('gs-total',  ALL_PRODUCTS.length);
  set('gs-avail',  avail.length);
  set('gs-stores', SOURCES_META.filter(s => ALL_DATA[s.id] && s.id !== 'subito' && s.id !== 'ebay').length);
}

function _renderConsoleSummary() {
  const STORE_ONLY = new Set(['cex', 'gamelife', 'gamepeople', 'gameshock', 'rebuy']);
  const sources = SOURCES_META.filter(s => ALL_DATA[s.id] && STORE_ONLY.has(s.id)).map(s => s.id);
  if (!sources.length) return;

  const thead = document.getElementById('summary-thead');
  thead.innerHTML = '';
  const htr = document.createElement('tr');
  let html = '<th>Console</th>';
  for (const srcId of sources) html += `<th>${_storeLabel(srcId)}</th>`;
  html += '<th class="th-ref">Ref. Usato</th><th class="th-ref">Ref. Nuovo</th>';
  htr.innerHTML = html;
  thead.appendChild(htr);

  const tbody = document.getElementById('summary-tbody');
  tbody.innerHTML = '';

  for (const f of CONSOLE_FAMILIES) {
    const tr = document.createElement('tr');

    const tdFam = document.createElement('td');
    tdFam.className = 'summ-family';
    tdFam.textContent = 'Xbox ' + f.label;
    tr.appendChild(tdFam);

    const refUsato = [], refNuovo = [];

    for (const srcId of sources) {
      const items = (ALL_DATA[srcId].products ?? []).filter(p => _familyKey(p.name) === f.key);
      const avail = items.filter(p => p.available !== false);

      avail.filter(p => p.condition === 'Usato' && p.price != null).forEach(p => refUsato.push(p.price));
      avail.filter(p => p.condition === 'Nuovo' && p.price != null).forEach(p => refNuovo.push(p.price));

      const td = document.createElement('td');
      td.className = 'summ-store-cell';

      if (!items.length) {
        td.innerHTML = '<span class="summ-none">—</span>';
      } else if (!avail.length) {
        td.innerHTML = `<span class="summ-esaurito">esaurito</span><span class="summ-sub">${items.length} tot.</span>`;
      } else {
        const minPx   = _minPrice(avail) ?? 0;
        const showFrom = avail.length > 1;
        td.innerHTML = `
          <span class="summ-avail-count">${avail.length}</span> dispo
          <span class="summ-price">${showFrom ? 'da ' : ''}€ ${minPx.toFixed(2)}</span>
        `;
      }
      tr.appendChild(td);
    }

    const avgU = _avg(refUsato);
    const tdRefU = document.createElement('td');
    tdRefU.className = 'summ-ref-cell';
    tdRefU.innerHTML = avgU != null ? `€ ${avgU.toFixed(0)}` : '<span class="summ-none">—</span>';
    tr.appendChild(tdRefU);

    const avgN = _avg(refNuovo);
    const tdRefN = document.createElement('td');
    tdRefN.className = 'summ-ref-cell';
    tdRefN.innerHTML = avgN != null ? `€ ${avgN.toFixed(0)}` : '<span class="summ-none">—</span>';
    tr.appendChild(tdRefN);

    tbody.appendChild(tr);
  }
}

// ============================================================
// CATALOGO VIEW  (legge da DB_PRODUCTS)
// ============================================================
function _populateSourceFilter() {
  const sel = document.getElementById('filter-source');
  sel.innerHTML = '<option value="">Tutte le fonti</option>';
  for (const src of SOURCES_META) {
    if (src.id === 'subito') continue;   // Subito ha sezione dedicata
    const opt = document.createElement('option');
    opt.value = src.id; opt.textContent = _storeLabel(src.id);
    sel.appendChild(opt);
  }
}

function _populateStorageFilter() {
  const sel = document.getElementById('filter-storage');
  sel.innerHTML = '<option value="">Tutti gli storage</option>';
  for (const s of STORAGE_SIZES) {
    const opt = document.createElement('option');
    opt.value = s.label; opt.textContent = s.label;
    sel.appendChild(opt);
  }
}

function _populateCatalogoFilters() {
  // Populate edition dropdown dynamically from DB_PRODUCTS or ALL_PRODUCTS parsed_edition
  const edSel = document.getElementById('filter-edition');
  if (!edSel) return;

  const source = DB_PRODUCTS.length ? DB_PRODUCTS : ALL_PRODUCTS;

  // Keep first option and the 4 static types (indices 0-4), remove the rest
  while (edSel.options.length > 5) edSel.remove(5);

  // Collect unique parsed_edition values that are "specific" (not the 4 generic types)
  const genericTypes = new Set(['standard', 'limited', 'special', 'bundle', '']);
  const editions = new Map(); // edition → count
  source.forEach(p => {
    const ed = (p.parsed_edition || '').trim();
    if (ed && !genericTypes.has(ed.toLowerCase())) {
      editions.set(ed, (editions.get(ed) || 0) + 1);
    }
  });

  // Sort by count descending, add separator optgroup then options
  if (editions.size > 0) {
    const optgroup = document.createElement('optgroup');
    optgroup.label = '── Edizioni specifiche ──';
    edSel.appendChild(optgroup);

    [...editions.entries()]
      .sort((a, b) => b[1] - a[1])
      .forEach(([ed, count]) => {
        const opt = new Option(`${ed} (${count})`, ed);
        edSel.appendChild(opt);
      });
  }
}

function toggleChip(btn, groupId) {
  btn.classList.toggle('active');
  applyFilters();
}

function _getChipValues(groupId) {
  const chips = document.querySelectorAll(`#${groupId} .chip.active`);
  return [...chips].map(c => c.dataset.value);
}

function applyFilters() {
  const q           = document.getElementById('search').value.toLowerCase().trim();
  const srcFilter   = document.getElementById('filter-source').value;
  const familyValues  = _getChipValues('chips-family');
  const conditionValues = _getChipValues('chips-condition');
  const storageFilter= document.getElementById('filter-storage').value;
  const segmentValues = _getChipValues('chips-segment');
  const editionFilter = document.getElementById('filter-edition').value;
  const onlyAvail   = document.getElementById('filter-available').checked;
  const onlyBase    = document.getElementById('filter-base').checked;

  // Usa DB_PRODUCTS se disponibili, fallback su ALL_PRODUCTS
  const source = DB_PRODUCTS.length ? DB_PRODUCTS : ALL_PRODUCTS.map(p => ({
    ...p, last_price: p.price, last_available: p.available ? 1 : 0,
    console_family: _familyKey(p.name),
    model_segment: 'unknown',
    edition_class: 'standard',
    standard_name: '',
    standard_key: '',
    packaging_state: p.source === 'cex' ? 'N/D' : 'Imballata',
  }));

  let filtered = source.slice();

  filtered.forEach(p => enhanceProduct(p));

  if (q)             filtered = filtered.filter(p =>
    (p.name || '').toLowerCase().includes(q) ||
    (p.standard_name || '').toLowerCase().includes(q)
  );
  if (srcFilter)       filtered = filtered.filter(p => p.source === srcFilter);
  if (familyValues.length)    filtered = filtered.filter(p => familyValues.some(v => (p.console_family||'').toLowerCase().includes(v.toLowerCase())));
  if (conditionValues.length) filtered = filtered.filter(p => conditionValues.includes(p.condition));
  if (storageFilter)   filtered = filtered.filter(p => p.storage_label === storageFilter);
  if (segmentValues.length)   filtered = filtered.filter(p => segmentValues.includes(p.parsed_segment));
  if (editionFilter)   filtered = filtered.filter(p => (p.edition_class || 'standard').toLowerCase() === editionFilter.toLowerCase() || (p.parsed_edition || '').toLowerCase().includes(editionFilter.toLowerCase()) || (p.edition_name || '').toLowerCase().includes(editionFilter.toLowerCase()));
  if (onlyAvail)     filtered = filtered.filter(p => p.last_available || p.available);
  if (onlyBase)      filtered = filtered.filter(p => p.is_base_model);

  filtered.sort((a, b) => {
    let va = a[currentSort.key], vb = b[currentSort.key];
    if (va == null) va = currentSort.key === 'last_price' ? Infinity : '';
    if (vb == null) vb = currentSort.key === 'last_price' ? Infinity : '';
    if (typeof va === 'string') va = va.toLowerCase();
    if (typeof vb === 'string') vb = vb.toLowerCase();
    return va < vb ? -currentSort.dir : va > vb ? currentSort.dir : 0;
  });

  const rcEl = document.getElementById('ricerca-count');
  if (rcEl) rcEl.textContent = `Trovati ${filtered.length} prodotti (${source.length} corrispondono ai modelli cercati)`;
  _renderTable(filtered);
}

/* ============================================================
   STATISTICHE (Analisi Matematica)
   ============================================================ */

const StatEngine = {
  mean(arr) {
    if (!arr.length) return 0;
    return arr.reduce((a, b) => a + b, 0) / arr.length;
  },
  stdDev(arr, mean) {
    if (arr.length <= 1) return 0;
    let sum = 0;
    for (const val of arr) sum += Math.pow(val - mean, 2);
    return Math.sqrt(sum / (arr.length - 1));
  },
  zScore(val, arr) {
    if (arr.length <= 1) return null;
    const m = this.mean(arr);
    const sd = this.stdDev(arr, m);
    if (sd === 0) return 0;
    return (val - m) / sd;
  },
  bidAskSpread(items) {
    const b2c = ['cex', 'rebuy', 'gameshock'];
    const c2c = ['subito', 'ebay'];
    
    const askPrices = items.filter(i => b2c.includes(i.source)).map(i => i.last_price ?? i.price).filter(px => px != null && px > 0);
    const bidPrices = items.filter(i => c2c.includes(i.source)).map(i => i.last_price ?? i.price).filter(px => px != null && px > 0);
    
    if (askPrices.length === 0 || bidPrices.length === 0) return null;
    
    const askMin = Math.min(...askPrices);
    const bidAvg = this.mean(bidPrices);
    
    return askMin - bidAvg;
  },
  getDailyHistory(productIds) {
    if (!PRICE_HISTORY || !PRICE_HISTORY.length) return [];
    const items = PRICE_HISTORY.filter(h => productIds.includes(h.product_id) && h.price_new > 0);
    const byDate = {};
    for (const h of items) {
      const d = h.changed_at.split('T')[0];
      if (!byDate[d] || h.price_new < byDate[d]) {
        byDate[d] = h.price_new;
      }
    }
    const dates = Object.keys(byDate).sort();
    return dates.map(d => ({ date: d, price: byDate[d] }));
  },
  ema(productIds, periods = 7) {
    const series = this.getDailyHistory(productIds);
    if (series.length < 2) return null;
    let emaVal = series[0].price;
    const k = 2 / (periods + 1);
    for (let i = 1; i < series.length; i++) {
        emaVal = (series[i].price * k) + (emaVal * (1 - k));
    }
    return emaVal;
  },
  linearRegressionSlope(productIds) {
    const series = this.getDailyHistory(productIds);
    if (series.length < 2) return null;
    const x = Array.from({length: series.length}, (_, i) => i);
    const y = series.map(s => s.price);
    const xMean = this.mean(x);
    const yMean = this.mean(y);
    let num = 0, den = 0;
    for (let i = 0; i < series.length; i++) {
      num += (x[i] - xMean) * (y[i] - yMean);
      den += Math.pow(x[i] - xMean, 2);
    }
    if (den === 0) return 0;
    return num / den;
  },
  poissonProbOutofStock(productIds) {
    if (!PRICE_HISTORY || !PRICE_HISTORY.length) return null;
    const items = PRICE_HISTORY.filter(h => productIds.includes(h.product_id));
    if (items.length === 0) return null;
    
    // Sort array in chronological order
    const sorted = [...items].sort((a,b) => a.changed_at.localeCompare(b.changed_at));
    
    let soldEvents = 0;
    const firstDate = new Date(sorted[0].changed_at);
    const lastDate = new Date(); 
    const daysDiff = Math.max(1, (lastDate - firstDate) / (1000 * 3600 * 24));
    
    let lastAvail = {}; 
    for(const ev of sorted) {
      if (lastAvail[ev.product_id] === 1 && ev.available_new === 0) {
        soldEvents++;
      }
      lastAvail[ev.product_id] = ev.available_new;
    }
    
    const lambdaDay = soldEvents / daysDiff;
    if (lambdaDay === 0) return 0;
    
    // Prob di almeno 1 vendita nel prossimo giorno: 1 - e^(-lambda)
    return 1 - Math.exp(-lambdaDay);
  },
  pearsonCorrelation(x, y) {
    if (x.length !== y.length || x.length < 2) return null;
    const xM = this.mean(x);
    const yM = this.mean(y);
    let num = 0, denX = 0, denY = 0;
    for (let i = 0; i < x.length; i++) {
        const dx = x[i] - xM;
        const dy = y[i] - yM;
        num += dx * dy;
        denX += dx * dx;
        denY += dy * dy;
    }
    if (denX === 0 || denY === 0) return 0;
    return num / Math.sqrt(denX * denY);
  },
  storagePriceCorrelation(family, segment) {
    const products = ALL_PRODUCTS.filter(p => p.parsed_family === family && p.parsed_segment === segment && p.last_price > 0);
    const x = [], y = [];
    for (const p of products) {
        if (!p.storage_label) continue;
        let gb = 0;
        if (p.storage_label.toUpperCase().includes('TB')) gb = parseFloat(p.storage_label) * 1000;
        else gb = parseFloat(p.storage_label);
        
        if (gb > 0) {
            x.push(gb);
            y.push(p.last_price);
        }
    }
    const uniqueStorage = new Set(x);
    if (uniqueStorage.size < 2) return null; // Must have variation to correlate
    
    return this.pearsonCorrelation(x, y);
  },
  kMeans1D(coords, k = 3) {
    if (coords.length < k) {
        return coords.map(p => ({ centroid: p, items: [p] })).sort((a,b) => a.centroid - b.centroid);
    }
    const min = Math.min(...coords);
    const max = Math.max(...coords);
    let centroids = [];
    for (let i = 0; i < k; i++) centroids.push(min + (max - min) * (i / (k - 1)));
    
    let clusters = [];
    let changed = true, maxIter = 50, it = 0;
    while(changed && it < maxIter) {
      changed = false;
      it++;
      clusters = Array.from({length: k}, () => []);
      for(const p of coords) {
        let minDist = Infinity, cIdx = 0;
        for(let j=0; j<k; j++) {
           let d = Math.abs(p - centroids[j]);
           if (d < minDist) { minDist = d; cIdx = j; }
        }
        clusters[cIdx].push(p);
      }
      for(let j=0; j<k; j++) {
        if(clusters[j].length > 0) {
          const newC = this.mean(clusters[j]);
          if(Math.abs(newC - centroids[j]) > 0.1) {
             centroids[j] = newC;
             changed = true;
          }
        }
      }
    }
    return clusters.map((c, i) => ({ centroid: centroids[i], items: c })).sort((a,b) => a.centroid - b.centroid);
  },
  bayesianPremium(g) {
    if (!g.family || !g.segment) return null;
    // Identifica i prezzi dell'edizione Standard per questo esatto modello/spazio
    const stdItems = ALL_PRODUCTS.filter(p => p.parsed_family === g.family && p.parsed_segment === g.segment && p.storage_label === g.storage && (!p.parsed_edition || p.parsed_edition.toLowerCase() === 'standard') && p.last_price > 0);
    
    // Trova i prezzi di QUESTA combo (potrebbe essere una special edition)
    const comboPrices = g.items.map(i => i.last_price ?? i.price).filter(p => p != null && p > 0);
    
    if (stdItems.length === 0 || comboPrices.length === 0) return null;
    
    const stdMean = this.mean(stdItems.map(i => i.last_price));
    const thisMean = this.mean(comboPrices);
    
    const rawPremium = thisMean - stdMean;
    
    // Bayesian shrinking towards a prior of +30 EUR with weight C=3
    const priorPremium = 30; 
    const C = 3;
    const N = comboPrices.length;
    
    return ((N * rawPremium) + (C * priorPremium)) / (N + C);
  },
  confidenceInterval(prices) {
    if (prices.length < 2) return null;
    const m = this.mean(prices);
    const s = this.stdDev(prices, m);
    if (s === 0) return { lower: m, upper: m, margin: 0, volatile: false };
    
    const margin = 1.96 * (s / Math.sqrt(prices.length));
    const lower = Math.max(0, m - margin);
    const upper = m + margin;
    const volatile = (margin / m) > 0.25; 
    
    return { lower, upper, margin, volatile };
  },
  shapleyKinect(g) {
    if (g.family !== '360' && g.family !== 'One') return null;
    
    const withKinect = ALL_PRODUCTS.filter(p => p.parsed_family === g.family && p.has_kinect === 1 && p.last_price > 0);
    const woKinect = ALL_PRODUCTS.filter(p => p.parsed_family === g.family && p.has_kinect === 0 && p.last_price > 0);
    
    if (withKinect.length === 0 || woKinect.length === 0) return null;
    
    const mWith = this.mean(withKinect.map(p => p.last_price));
    const mWo = this.mean(woKinect.map(p => p.last_price));
    
    return mWith - mWo;
  }
};

/**
 * Raggruppa tutti i BASE_MODELS per combo_key.
 */
function _buildComboTitle(p) {
  const kinect = p.parsed_kinect ? ' + Kinect' : '';
  const edition = p.parsed_edition && p.parsed_edition.toLowerCase() !== 'standard' ? ` ${p.parsed_edition}` : '';
  const storage = p.storage_label ? ` · ${p.storage_label}` : '';
  const cond = p.condition ? ` [${p.condition}]` : '';
  return `Xbox ${p.parsed_family || '?'} ${p.parsed_segment || ''}${edition}${kinect}${storage}${cond}`.trim();
}

function getUniqueBaseModels() {
  // BASE_MODELS dal DB ha standard_key, model_segment, edition_class ma NON parsed_*.
  // ALL_PRODUCTS viene arricchito da enhanceProduct() e ha combo_key e parsed_*.
  // Strategia: raggruppiamo ALL_PRODUCTS per combo_key, poi teniamo solo i gruppi
  // il cui representative ha uno standard_key che è presente in BASE_MODELS.
  
  // Raggruppiamo ALL_PRODUCTS che hanno un combo_key presente in BASE_MODELS.
  const baseKeys = new Set(BASE_MODELS.map(bm => {
    enhanceProduct(bm);
    return bm.combo_key;
  }));
  
  // Se nessun preferito, fallback: nessun risultato.
  if (baseKeys.size === 0) {
    return [];
  }

  // Passo 2: raggruppa tutti i prodotti per combo_key
  const groups = {};
  const unifiedPool = [];
  const seenUrls = new Set();
  const addPool = (list) => {
    if (!list) return;
    for (const p of list) {
      if (p.url && seenUrls.has(p.url)) continue;
      if (p.url) seenUrls.add(p.url);
      enhanceProduct(p);
      unifiedPool.push(p);
    }
  };
  addPool(BASE_MODELS);
  addPool(DB_PRODUCTS);
  addPool(ALL_PRODUCTS);
  
  // Aggiungiamo anche gli annunci Subito approvati dall'AI
  const approvedSubito = SUBITO_ADS
    .filter(a => a.ai_status === 'approved')
    .map(a => ({
      ...a,
      source: 'subito',
      condition: 'Usato',
      price: a.last_price,
      available: a.last_available === 1
    }));
  addPool(approvedSubito);

  unifiedPool.forEach(p => {
    const gkey = p.combo_key;
    if (!gkey) return;
    if (!groups[gkey]) {
      groups[gkey] = {
        key: gkey,
        title: _buildComboTitle(p),
        family: p.parsed_family,
        segment: p.parsed_segment,
        storage: p.storage_label,
        condition: p.condition,
        count: 0,
        items: []
      };
    }
    groups[gkey].items.push(p);
    groups[gkey].count++;
  });

  // Passo 3: teniamo solo i combo_key presenti nei preferiti
  const favCombos = new Set();
  unifiedPool.forEach(p => {
    if (baseKeys.has(p.combo_key)) {
      favCombos.add(p.combo_key);
    }
  });

  // Ritorna i gruppi preferiti, o TUTTI se nessun match (fallback per mostrare qualcosa)
  const filtered = Object.values(groups).filter(g => favCombos.has(g.key));
  
  // Applica deduplica per ReBuy: tenere solo il prodotto col prezzo più alto
  filtered.forEach(g => {
    const rebuyItems = g.items.filter(p => p.source === 'rebuy');
    if (rebuyItems.length > 1) {
      const highestRebuy = rebuyItems.reduce((prev, curr) => {
        const pPrice = prev.last_price ?? prev.price ?? 0;
        const cPrice = curr.last_price ?? curr.price ?? 0;
        return cPrice > pPrice ? curr : prev;
      });
      g.items = [...g.items.filter(p => p.source !== 'rebuy'), highestRebuy];
    }
  });

  return filtered.length > 0 ? filtered : Object.values(groups);
}

function renderStatistiche() {
  const container = document.getElementById('stats-grid');
  if (!container) return;

  // Ordine: Original → 360 → One → Serie (parsed_family values)
  const STAT_FAMILY_ORDER = ['Original', '360', 'One', 'Serie', 'Altro'];
  const STAT_SEG_ORDER    = ['Base', 'S', 'X', 'Slim', 'E', 'Elite'];
  const uniqueModels = getUniqueBaseModels().sort((a, b) => {
    const af = STAT_FAMILY_ORDER.indexOf(a.family || 'Altro');
    const bf = STAT_FAMILY_ORDER.indexOf(b.family || 'Altro');
    if (af !== bf) return (af < 0 ? 99 : af) - (bf < 0 ? 99 : bf);
    // Stesso family → ordina per segmento
    const as_ = STAT_SEG_ORDER.indexOf(a.segment || '');
    const bs_ = STAT_SEG_ORDER.indexOf(b.segment || '');
    if (as_ !== bs_) return (as_ < 0 ? 99 : as_) - (bs_ < 0 ? 99 : bs_);
    // Stesso segmento → ordina per storage (numerico)
    const aGB = parseInt(a.storage) || 0;
    const bGB = parseInt(b.storage) || 0;
    return aGB - bGB;
  });
  
  console.log('[Statistiche] BASE_MODELS:', BASE_MODELS.length, 'uniqueModels:', uniqueModels.length);

  if (uniqueModels.length === 0) {
    container.innerHTML = `
      <div style="padding:40px;text-align:center;color:var(--text-muted);">
        <div style="font-size:48px;margin-bottom:16px;">⭐</div>
        <div style="font-size:18px;font-weight:600;margin-bottom:8px;">Nessun modello preferito trovato</div>
        <div>Vai nella sezione <strong>Home</strong> e clicca ⭐ accanto ai modelli che vuoi monitorare.<br>
        Oppure verifica che BASE_MODELS sia caricato correttamente (${BASE_MODELS.length} prodotti ora in memoria).</div>
      </div>`;
    return;
  }
  
  let html = `
    <div style="overflow-x:auto;">
    <table class="home-table" style="white-space: nowrap;">
      <thead>
        <tr>
          <th>Modello Unico</th>
          <th style="text-align:center;">Campioni (N)</th>
          <th style="text-align:right;">Prezzo Minimo</th>
          <th style="text-align:center;">Z-Score (Anomalia) <span class="info-icon" tabindex="0" data-tip="Misura quanto il prezzo minimo è anomalo. Sopra 2 = prezzo insolito, possibile errore o affare.">ℹ️</span></th>
          <th style="text-align:center;">Spread B2C-C2C <span class="info-icon" tabindex="0" data-tip="Differenza tra prezzo store (B2C) e prezzo privato (C2C). Alto = ampio margine di arbitraggio.">ℹ️</span></th>
          <th style="text-align:center;">EMA (7g) <span class="info-icon" tabindex="0" data-tip="Media mobile esponenziale ultimi 7 giorni. Più reattiva della media semplice ai cambiamenti recenti.">ℹ️</span></th>
          <th style="text-align:center;">Trend Regr. <span class="info-icon" tabindex="0" data-tip="Variazione prezzo giornaliera (€/giorno) dalla regressione lineare. Positivo = prezzi in salita.">ℹ️</span></th>
          <th style="text-align:center;">Rischio Esaur. <span class="info-icon" tabindex="0" data-tip="Probabilità (modello Poisson) che il prodotto vada esaurito. Alto = raro/scarso.">ℹ️</span></th>
          <th style="text-align:center;">Impatto Storage <span class="info-icon" tabindex="0" data-tip="Correlazione Pearson tra capienza GB e prezzo. Vicino a 1 = più storage = più costoso.">ℹ️</span></th>
          <th style="text-align:center;">Clustering (Fasce Prezzo) <span class="info-icon" tabindex="0" data-tip="Fasce di prezzo naturali rilevate con k-means (3 cluster). Mostra la distribuzione reale dei prezzi.">ℹ️</span></th>
          <th style="text-align:center;">Premium Apparente <span class="info-icon" tabindex="0" data-tip="Stima bayesiana del valore aggiunto rispetto alla versione standard. Quanto vale l'edizione speciale?">ℹ️</span></th>
          <th style="text-align:center;">Affidabilità (CI 95%) <span class="info-icon" tabindex="0" data-tip="Margine di errore con 95% di confidenza: la media reale cade nell'intervallo ±X€.">ℹ️</span></th>
          <th style="text-align:center;">Valore Kinect <span class="info-icon" tabindex="0" data-tip="Valore medio aggiunto dal Kinect (metodo Shapley). Quanto paga in più chi compra con Kinect?">ℹ️</span></th>
        </tr>
      </thead>
      <tbody>
  `;

  uniqueModels.forEach((g, idx) => {
    // Stat 1: Z-Score sul prezzo min
    const prices = g.items
      .map(i => i.last_price ?? i.price)
      .filter(px => px != null && px > 0);
      
    let zScoreStr = 'N/D';
    let minPxStr = '—';
    
    if (prices.length > 0) {
      const currentMin = Math.min(...prices);
      minPxStr = '€ ' + currentMin.toFixed(2);
      
      if (prices.length > 2) {
        const z = StatEngine.zScore(currentMin, prices);
        if (z !== null) {
          zScoreStr = z.toFixed(2);
          if (z <= -1.5) zScoreStr = `<strong style="color:var(--avail-ok)">${zScoreStr} 🔥</strong>`;
          else if (z >= 1.5) zScoreStr = `<span style="color:var(--avail-ko)">${zScoreStr}</span>`;
        }
      }
    }
    
    // Stat 2: Spread
    const spread = StatEngine.bidAskSpread(g.items);
    let spreadStr = 'N/D';
    if (spread !== null) {
      spreadStr = (spread > 0 ? '+' : '') + spread.toFixed(2) + ' €';
      if (spread > 50) spreadStr = `<strong style="color:var(--avail-ok)">${spreadStr}</strong>`;
      else if (spread < 0) spreadStr = `<strong style="color:var(--avail-ko)">${spreadStr}</strong>`;
    }

    // Stat 3 & 4
    const pIds = g.items.map(i => i.id);
    const emaVal = StatEngine.ema(pIds, 7);
    let emaStr = 'N/D';
    if (emaVal !== null) emaStr = '€ ' + emaVal.toFixed(2);
    
    const slope = StatEngine.linearRegressionSlope(pIds);
    let slopeStr = 'N/D';
    if (slope !== null) {
        slopeStr = (slope > 0 ? '+' : '') + slope.toFixed(2) + ' €/g';
        if (slope < -1) slopeStr = `<span style="color:var(--avail-ok)">${slopeStr} 📉</span>`;
        if (slope > 1) slopeStr = `<span style="color:var(--avail-ko)">${slopeStr} 📈</span>`;
    }

    // Stat 5
    const probOut = StatEngine.poissonProbOutofStock(pIds);
    let pOutStr = 'N/D';
    if (probOut !== null) {
      const pct = (probOut * 100).toFixed(1);
      pOutStr = pct + '%';
      if (probOut > 0.5) pOutStr = `<strong style="color:var(--avail-ko)">${pOutStr} ⚠️</strong>`;
      else if (probOut < 0.1) pOutStr = `<span style="color:var(--text-muted)">${pOutStr}</span>`;
    }
    
    // Stat 6
    const pearson = StatEngine.storagePriceCorrelation(g.family, g.segment);
    let pearsonStr = 'N/D';
    if (pearson !== null) {
      pearsonStr = pearson.toFixed(2);
      if (pearson > 0.5) pearsonStr = `<strong style="color:var(--avail-ok)">${pearsonStr} (Forte)</strong>`;
      else if (pearson < 0.2 && pearson > -0.2) pearsonStr = `<strong style="color:var(--text-muted)">${pearsonStr} (Nullo)</strong>`;
    }
    
    // Stat 7
    let clusterStr = 'N/D';
    if (prices.length >= 3) {
      const clusters = StatEngine.kMeans1D(prices, 3);
      const cStr = clusters.filter(c => c.items.length > 0).map(c => Math.round(c.centroid) + '€').join(' | ');
      clusterStr = `<span style="font-size:11px; letter-spacing:-0.5px;">${cStr}</span>`;
    }
    
    // Stat 8
    const bayesPrem = StatEngine.bayesianPremium(g);
    let premStr = '—'; // If standard, we might leave it as — or 0
    if (bayesPrem !== null) {
      premStr = (bayesPrem > 0 ? '+' : '') + bayesPrem.toFixed(0) + '€';
      if (bayesPrem > 40) premStr = `<strong style="color:var(--avail-ok)">${premStr}</strong>`;
    }
    
    // Stat 9
    const ci = StatEngine.confidenceInterval(prices);
    let ciStr = 'N/D';
    if (ci !== null) {
      ciStr = `±${ci.margin.toFixed(0)}€`;
      if (ci.volatile) ciStr = `<span style="color:var(--avail-ko)" title="Alta Varietà">${ciStr} ⚠️</span>`;
      else ciStr = `<span style="color:var(--avail-ok)">${ciStr} ✓</span>`;
    }
    
    // Stat 10
    const kVal = StatEngine.shapleyKinect(g);
    let kStr = 'N/D';
    if (kVal !== null) {
        kStr = (kVal > 0 ? '+' : '') + kVal.toFixed(0) + '€';
    }

    // Costruisci sub-panel con i link ai prodotti del gruppo
    const rowId = `stat-row-${idx}`;
    const availItems = g.items.filter(p => p.last_available || p.available);
    const allItemsSorted = [...g.items].sort((a, b) => {
      const pa = a.last_price ?? a.price ?? Infinity;
      const pb = b.last_price ?? b.price ?? Infinity;
      return pa - pb;
    });

    const detailRows = allItemsSorted.map(p => {
      const px = p.last_price ?? p.price;
      const isAvail = p.last_available || p.available;
      const pxStr = px != null ? `€ ${px.toFixed(2)}` : '—';
      const availBadge = isAvail
        ? `<span style="color:var(--avail-ok);font-size:11px;">● Dispon.</span>`
        : `<span style="color:var(--avail-ko);font-size:11px;">● Esaurito</span>`;
      const store = _storeLabel(p.source || '');
      const linkEl = p.url
        ? `<a href="${p.url}" target="_blank" rel="noopener" style="color:var(--accent);font-size:12px;">🔗 Apri</a>`
        : `<span style="color:var(--text-muted);font-size:12px;">nessun link</span>`;
      const pack = p.packaging_state ? `<span style="font-size:10px;color:var(--text-muted);">${p.packaging_state}</span>` : '';
      return `<tr style="background:var(--bg);border-top:1px solid var(--border);">
        <td style="padding:4px 8px;font-size:12px;">${store}</td>
        <td style="padding:4px 8px;font-size:12px;">${p.name || '—'}</td>
        <td style="padding:4px 8px;font-size:12px;">${pack}</td>
        <td style="padding:4px 8px;font-weight:600;color:var(--text);">${pxStr}</td>
        <td style="padding:4px 8px;">${availBadge}</td>
        <td style="padding:4px 8px;">${linkEl}</td>
      </tr>`;
    }).join('');

    const expandPanel = `<tr id="${rowId}" style="display:none;">
      <td colspan="13" style="padding:0;background:var(--card);">
        <div style="padding:8px 16px 12px;">
          <div style="font-size:11px;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px;">
            ${g.items.length} prodotti trovati · ${availItems.length} disponibili
          </div>
          <table style="width:100%;border-collapse:collapse;font-size:12px;">
            <thead>
              <tr style="color:var(--text-muted);font-size:11px;text-align:left;">
                <th style="padding:2px 8px;">Store</th>
                <th style="padding:2px 8px;">Nome</th>
                <th style="padding:2px 8px;">Imballo</th>
                <th style="padding:2px 8px;">Prezzo</th>
                <th style="padding:2px 8px;">Stato</th>
                <th style="padding:2px 8px;">Link</th>
              </tr>
            </thead>
            <tbody>${detailRows || '<tr><td colspan="6" style="padding:8px;color:var(--text-muted);">Nessun prodotto</td></tr>'}</tbody>
          </table>
        </div>
      </td>
    </tr>`;

    html += `
      <tr class="stat-summary-row" style="cursor:pointer;" onclick="
        const el = document.getElementById('${rowId}');
        const isOpen = el.style.display !== 'none';
        el.style.display = isOpen ? 'none' : 'table-row';
        this.style.background = isOpen ? '' : 'var(--card)';
      " title="Clicca per vedere tutti i prodotti con i link">
        <td><strong>${g.title}</strong> <span style="font-size:10px;color:var(--text-muted);">▼</span></td>
        <td style="text-align:center;">${prices.length}</td>
        <td style="text-align:right;">${minPxStr}</td>
        <td style="text-align:center;">${zScoreStr}</td>
        <td style="text-align:center;">${spreadStr}</td>
        <td style="text-align:center;">${emaStr}</td>
        <td style="text-align:center;">${slopeStr}</td>
        <td style="text-align:center;">${pOutStr}</td>
        <td style="text-align:center;">${pearsonStr}</td>
        <td style="text-align:center;">${clusterStr}</td>
        <td style="text-align:center;">${premStr}</td>
        <td style="text-align:center;">${ciStr}</td>
        <td style="text-align:center;">${kStr}</td>
      </tr>
      ${expandPanel}
    `;
  });

  html += '</tbody></table></div>';
  container.innerHTML = html;
}

function toggleSort(key) {
  currentSort = currentSort.key === key
    ? { key, dir: currentSort.dir * -1 }
    : { key, dir: 1 };
  applyFilters();
}

async function _toggleBaseModel(productId, currentValue, starEl) {
  const newValue = !currentValue;
  try {
    const res = await API.fetchJson('/api/db/set-base', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: productId, value: newValue }),
    });
    const data = res.body || {};
    if (data.ok) {
      // Aggiorna localmente
      const prod = DB_PRODUCTS.find(p => p.id === productId);
      if (prod) prod.is_base_model = newValue ? 1 : 0;
      // Aggiorna in BASE_MODELS
      if (newValue) {
        const toAdd = DB_PRODUCTS.find(p => p.id === productId);
        if (toAdd && !BASE_MODELS.find(p => p.id === productId)) BASE_MODELS.push(toAdd);
      } else {
        BASE_MODELS = BASE_MODELS.filter(p => p.id !== productId);
      }
      starEl.textContent = newValue ? '★' : '☆';
      starEl.classList.toggle('star-active', newValue);
      starEl.title = newValue ? 'Rimuovi dai modelli base' : 'Aggiungi ai modelli base';
      // Ricarica home se visibile
      if (document.getElementById('view-home').classList.contains('active')) {
        renderHome();
      }
      _syncState();
    }
  } catch (e) {
    console.error('Errore set-base:', e);
  }
}

function _renderTable(products) {
  const tbody    = document.getElementById('table-body');
  const emptyMsg = document.getElementById('empty-msg');
  const table    = document.getElementById('products-table');
  tbody.innerHTML = '';

  const hasData = DB_PRODUCTS.length || ALL_PRODUCTS.length;
  if (!hasData) {
    table.style.display = 'none'; emptyMsg.style.display = ''; return;
  }
  table.style.display = ''; emptyMsg.style.display = 'none';

  if (!products.length) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:40px;color:var(--text-muted);">Nessun risultato</td></tr>';
    return;
  }

  for (const p of products) {
    const tr = document.createElement('tr');
    const isAvail = p.last_available || p.available;
    const isBase  = !!p.is_base_model;
    const price   = p.last_price ?? p.price;

    // Colonna ID + stella
    const tdId = document.createElement('td');
    tdId.className = 'td-id';
    if (p.id) {
      const star = document.createElement('button');
      star.className   = 'star-btn' + (isBase ? ' star-active' : '');
      star.textContent = isBase ? '★' : '☆';
      star.title       = isBase ? 'Rimuovi dai modelli base' : 'Aggiungi ai modelli base';
      star.dataset.productId = p.id;
      star.dataset.isBase    = isBase ? '1' : '0';
      const idLabel = document.createElement('span');
      idLabel.className   = 'id-label';
      idLabel.textContent = p.display_id ?? p.id;
      tdId.appendChild(star);
      tdId.appendChild(idLabel);
    }
    tr.appendChild(tdId);

    // Nome
    const tdName = document.createElement('td');
    const a = document.createElement('a');
    a.href = p.url || '#'; a.target = '_blank'; a.rel = 'noopener noreferrer';
    a.className = 'prod-name'; a.textContent = p.name;
    tdName.appendChild(a); tr.appendChild(tdName);

    // Nuove colonne griglia
    const tdFamily = document.createElement('td');
    tdFamily.textContent = p.parsed_family || p.console_family || '—';
    tr.appendChild(tdFamily);

    const tdSegment = document.createElement('td');
    tdSegment.textContent = p.parsed_segment || p.model_segment || '—';
    tr.appendChild(tdSegment);

    const tdEdition = document.createElement('td');
    tdEdition.textContent = p.parsed_edition || '—';
    tr.appendChild(tdEdition);

    const tdKinect = document.createElement('td');
    tdKinect.textContent = p.parsed_kinect ? '✓' : '—';
    if (p.parsed_kinect) {
      tdKinect.style.color = 'var(--text)';
      tdKinect.style.fontWeight = 'bold';
    } else {
      tdKinect.style.color = 'var(--text-muted)';
    }
    tdKinect.style.textAlign = 'center';
    tr.appendChild(tdKinect);

    // Storage
    const tdStorage = document.createElement('td');
    if (p.storage_label) {
      const sb = document.createElement('span');
      sb.className = 'storage-badge'; sb.textContent = p.storage_label;
      tdStorage.appendChild(sb);
    } else {
      tdStorage.textContent = '—'; tdStorage.style.color = 'var(--text-muted)';
    }
    tr.appendChild(tdStorage);

    // Condizione
    const tdCond = document.createElement('td');
    const badge  = document.createElement('span');
    badge.className   = 'badge ' + (p.condition === 'Nuovo' ? 'badge-nuovo' : p.condition === 'Usato' ? 'badge-usato' : 'badge-nd');
    badge.textContent = p.condition || '—';
    tdCond.appendChild(badge); tr.appendChild(tdCond);

    // Imballo
    const tdPack = document.createElement('td');
    const packBadge = document.createElement('span');
    packBadge.className = 'badge-pack';
    packBadge.textContent = p.packaging_state || 'Imballata';
    tdPack.appendChild(packBadge);
    tr.appendChild(tdPack);

    // Fonte
    const tdSrc = document.createElement('td');
    const sb    = document.createElement('span');
    sb.className = 'badge-source'; sb.textContent = _storeLabel(p.source);
    tdSrc.appendChild(sb); tr.appendChild(tdSrc);

    // Prezzo
    const tdPrice = document.createElement('td');
    tdPrice.className   = 'td-price';
    tdPrice.textContent = price != null ? '€ ' + price.toFixed(2) : '—';
    tr.appendChild(tdPrice);

    // Disponibilità
    const tdAvail = document.createElement('td');
    if (isAvail !== undefined && isAvail !== null) {
      const av = document.createElement('span');
      av.className   = 'avail-badge ' + (isAvail ? 'ok' : 'ko');
      av.textContent = isAvail ? 'Disponibile' : 'Esaurito';
      tdAvail.appendChild(av);
    } else {
      tdAvail.textContent = '—'; tdAvail.style.color = 'var(--text-muted)';
    }
    tr.appendChild(tdAvail);

    tbody.appendChild(tr);
  }
}

// Delegated click handler per star buttons nella tabella Catalogo (un solo listener)
document.getElementById('products-table')?.addEventListener('click', e => {
  const star = e.target.closest('.star-btn[data-product-id]');
  if (!star) return;
  const productId = parseInt(star.dataset.productId, 10);
  const isBase = star.dataset.isBase === '1';
  _toggleBaseModel(productId, isBase, star);
});

// ============================================================
// NOMI STANDARD VIEW
// ============================================================
function renderStandardGroups() {
  const listEl = document.getElementById('standard-list');
  if (!listEl) return;

  const q = (document.getElementById('standard-search')?.value || '').toLowerCase().trim();
  let groups = STANDARD_GROUPS.slice();
  if (q) {
    groups = groups.filter(g =>
      (g.standard_name || '').toLowerCase().includes(q) ||
      (g.standard_key || '').toLowerCase().includes(q) ||
      (g.items || []).some(i => (i.name || '').toLowerCase().includes(q))
    );
  }

  const countEl = document.getElementById('standard-count');
  if (countEl) countEl.textContent = `${groups.length} nomi standard`;

  listEl.innerHTML = '';
  if (!groups.length) {
    listEl.innerHTML = '<div class="empty-shop">Nessun gruppo standard disponibile.</div>';
    return;
  }

  for (const group of groups) {
    const card = document.createElement('div');
    card.className = 'standard-group-card';

    const head = document.createElement('div');
    head.className = 'standard-group-head';
    head.innerHTML = `
      <div class="standard-title">${group.standard_name || 'N/D'}</div>
      <div class="standard-meta">${group.total_products || 0} prodotti · ${((group.sources || []).join(', ')) || 'n/d'}</div>
    `;
    card.appendChild(head);

    const key = document.createElement('div');
    key.className = 'standard-key';
    key.textContent = group.standard_key || '';
    card.appendChild(key);

    const tags = document.createElement('div');
    tags.className = 'standard-tags';
    const packaging = (group.packaging_states || []).join(' / ') || 'Imballata';
    const conditions = (group.conditions || []).join(' / ') || 'N/D';
    tags.innerHTML = `
      <span class="standard-tag">Imballo: ${packaging}</span>
      <span class="standard-tag">Condizioni: ${conditions}</span>
    `;
    card.appendChild(tags);

    const table = document.createElement('table');
    table.className = 'standard-items-table';
    table.innerHTML = `
      <thead>
        <tr>
          <th>Fonte</th>
          <th>Nome Originale</th>
          <th>Imballo</th>
          <th>Cond.</th>
        </tr>
      </thead>
    `;
    const tbody = document.createElement('tbody');
    for (const item of group.items || []) {
      const tr = document.createElement('tr');

      const tdSource = document.createElement('td');
      tdSource.textContent = _storeLabel(item.source || '');
      tr.appendChild(tdSource);

      const tdName = document.createElement('td');
      tdName.textContent = item.name || '—';
      tr.appendChild(tdName);

      const tdPack = document.createElement('td');
      tdPack.textContent = item.packaging_state || 'Imballata';
      tr.appendChild(tdPack);

      const tdCond = document.createElement('td');
      tdCond.textContent = item.condition || '—';
      tr.appendChild(tdCond);

      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    card.appendChild(table);

    listEl.appendChild(card);
  }
}

// ============================================================
// SUBITO VIEW
// ============================================================

function _populateRegionFilter() {
  const sel = document.getElementById('subito-filter-region');
  if (!sel) return;
  const current = sel.value;
  // Raccoglie regioni uniche dagli annunci disponibili, ordinate alfabeticamente
  const regions = [...new Set(
    SUBITO_ADS
      .filter(a => a.region)
      .map(a => a.region)
  )].sort((a, b) => a.localeCompare(b, 'it'));

  sel.innerHTML = '<option value="">Tutte le regioni</option>';
  for (const r of regions) {
    const opt = document.createElement('option');
    opt.value = r; opt.textContent = r;
    sel.appendChild(opt);
  }
  // Ripristina selezione precedente se ancora valida
  if (regions.includes(current)) sel.value = current;
}

async function loadSubitoData() {
  try {
    const _fb = { ok: false, body: null };
    const [rAds, rStats, rOpp] = _settled(
      await Promise.allSettled([
        API.fetchJson('/api/subito/ads'),
        API.fetchJson('/api/subito/stats'),
        API.fetchJson('/api/valuation/subito-opportunities?limit=500'),
      ]),
      [_fb, _fb, _fb],
    );
    if (rAds.ok)   SUBITO_ADS = _sanitizeRows(rAds.body);
    if (rStats.ok) {
      const s   = _sanitizeRow(rStats.body);
      const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
      set('ss-total', s.total   ?? '—');
      set('ss-avail', s.available ?? '—');
      set('ss-min',   s.min_price != null ? '€ ' + s.min_price.toFixed(2) : '—');
      set('ss-avg',   s.avg_price != null ? '€ ' + Math.round(s.avg_price) : '—');
    }
    if (rOpp.ok) {
      const payload = _sanitizeRow(rOpp.body);
      SUBITO_OPPS = _sanitizeRows(payload.items || []);
    } else {
      SUBITO_OPPS = [];
    }
    _populateRegionFilter();
    renderSubito();
    _syncState();
  } catch (e) {
    console.warn('Subito load:', e.message);
  }
}

function _fmtSubitoDate(raw) {
  if (!raw) return '—';
  // "2026-03-18 08:21:53" → "18/03 08:21"
  const m = raw.match(/(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})/);
  return m ? `${m[3]}/${m[2]} ${m[4]}:${m[5]}` : raw;
}

// ============================================================
// DEAL SCORE helpers
// ============================================================

function _computeFamilyAvgs(ads) {
  /** Calcola prezzo medio per famiglia, solo annunci disponibili con prezzo >= €35.
   *  Il floor esclude giochi/accessori economici che distorcerebbero la media. */
  const PRICE_FLOOR = 35;
  const groups = {};
  for (const ad of ads) {
    if (!ad.last_available || !(ad.last_price >= PRICE_FLOOR)) continue;
    const fk = ad.console_family || 'other';
    if (!groups[fk]) groups[fk] = [];
    groups[fk].push(ad.last_price);
  }
  const avgs = {};
  for (const [fk, prices] of Object.entries(groups)) {
    avgs[fk] = prices.reduce((a, b) => a + b, 0) / prices.length;
  }
  return avgs;
}

function _dealBadge(price, avg) {
  if (!(price > 0) || !(avg > 0)) return '';
  const score = (avg - price) / avg * 100;
  // Score > 60%: troppo lontano dalla media → quasi certamente non è una console
  if (score > 60) return '';
  if (score >= 30) return `<span class="deal-badge deal-fire">🔥 -${score.toFixed(0)}%</span>`;
  if (score >= 20) return `<span class="deal-badge deal-great">-${score.toFixed(0)}%</span>`;
  if (score >= 10) return `<span class="deal-badge deal-ok">-${score.toFixed(0)}%</span>`;
  if (score <= -15) return `<span class="deal-badge deal-over">+${Math.abs(score).toFixed(0)}%</span>`;
  return '';
}

function _buildOppMap(items) {
  const map = {};
  for (const item of items || []) {
    if (item.urn_id) map[item.urn_id] = item;
  }
  return map;
}

function _renderOpportunityBadge(ad, opp, famAvgs) {
  if (!opp) {
    return _dealBadge(ad.last_price, famAvgs[ad.console_family || 'other']);
  }

  const delta = Number(opp.delta_pct || 0);
  const quality = Number(opp.quality_score || 0);

  let valueBadge = '';
  if (delta >= 30) valueBadge = `<span class="deal-badge deal-fire">🔥 -${delta.toFixed(0)}%</span>`;
  else if (delta >= 20) valueBadge = `<span class="deal-badge deal-great">-${delta.toFixed(0)}%</span>`;
  else if (delta >= 10) valueBadge = `<span class="deal-badge deal-ok">-${delta.toFixed(0)}%</span>`;
  else if (delta <= -15) valueBadge = `<span class="deal-badge deal-over">+${Math.abs(delta).toFixed(0)}%</span>`;

  const qColor = quality >= 75 ? '#2e7d32' : quality >= 55 ? '#ef6c00' : '#b71c1c';
  const qBadge = `<span class="deal-badge" style="border-color:${qColor};color:${qColor};">Q${quality.toFixed(0)}</span>`;
  return `${valueBadge}${qBadge}`;
}

function renderSubito() {
  const grid = document.getElementById('subito-grid');
  if (!grid) return;
  grid.innerHTML = '';

  if (!SUBITO_ADS.length) {
    grid.innerHTML = '<div class="empty-shop">Nessun dato — esegui <code>python3 run.py --source subito</code></div>';
    return;
  }

  const q         = (document.getElementById('subito-search')?.value  || '').toLowerCase().trim();
  const aiFilter  = document.getElementById('subito-filter-ai')?.value || 'approved';
  const famFilter = document.getElementById('subito-filter-family')?.value  || '';
  const selFilter = document.getElementById('subito-filter-seller')?.value  || '';
  const segFilter = document.getElementById('subito-filter-segment')?.value || '';
  const edtFilter = document.getElementById('subito-filter-edition')?.value || '';
  const regFilter = document.getElementById('subito-filter-region')?.value  || '';
  const onlyAvail = document.getElementById('subito-filter-avail')?.checked || false;

  let filtered = SUBITO_ADS.slice();
  if (aiFilter !== 'all') {
    filtered = filtered.filter(a => (a.ai_status || 'pending') === aiFilter);
  }
  if (q)         filtered = filtered.filter(a => a.name.toLowerCase().includes(q));
  if (famFilter) filtered = filtered.filter(a => a.console_family === famFilter);
  if (selFilter) filtered = filtered.filter(a => a.seller_type === selFilter);
  if (segFilter) filtered = filtered.filter(a => (a.model_segment || 'unknown') === segFilter);
  if (edtFilter) filtered = filtered.filter(a => (a.edition_class || 'standard') === edtFilter);
  if (regFilter) filtered = filtered.filter(a => a.region === regFilter);
  if (onlyAvail) filtered = filtered.filter(a => !!a.last_available);

  const countEl = document.getElementById('subito-count');
  if (countEl) countEl.textContent = filtered.length + ' annunci';

  // Raggruppa per famiglia console
  // Medie per famiglia (su TUTTI gli annunci disponibili, non solo filtrati — riferimento stabile)
  const _famAvgs = _computeFamilyAvgs(SUBITO_ADS);
  const _oppByUrn = _buildOppMap(SUBITO_OPPS);

  const byFamily = {};
  for (const ad of filtered) {
    const fk = ad.console_family || 'other';
    if (!byFamily[fk]) byFamily[fk] = [];
    byFamily[fk].push(ad);
  }

  const familyOrder = [...CONSOLE_FAMILIES.map(f => f.key), 'other'];
  const thSt = 'padding:6px 10px; font-size:9.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.7px; color:var(--text-muted);';

  for (const fk of familyOrder) {
    const ads = byFamily[fk];
    if (!ads?.length) continue;

    // Ordina per prezzo crescente (disponibili prima)
    ads.sort((a, b) => {
      const da = a.last_available ? 0 : 1, db = b.last_available ? 0 : 1;
      if (da !== db) return da - db;
      return (a.last_price ?? Infinity) - (b.last_price ?? Infinity);
    });

    const availN  = ads.filter(a => a.last_available).length;
    const minPx   = ads.filter(a => a.last_available && a.last_price != null)
                       .reduce((m, a) => Math.min(m, a.last_price), Infinity);
    const famLabel = FAMILY_LABELS[fk] || fk;

    // Card
    const card = document.createElement('div');
    card.className = 'store-card';
    card.style.setProperty('--store-accent', '#ff6600');

    // Header card
    const header = document.createElement('div');
    header.className = 'store-card-header';
    header.innerHTML = `
      <span class="store-name" style="color:#ff6600;">${famLabel}</span>
      <span class="store-header-right">
        <span class="store-count">${availN} disponibili · ${ads.length} totali</span>
        ${minPx !== Infinity ? `<span class="card-min-price" style="margin-left:10px;font-size:12px;color:var(--text-muted);">da € ${minPx.toFixed(2)}</span>` : ''}
      </span>
    `;
    card.appendChild(header);

    // Tabella
    const table = document.createElement('table');
    table.className = 'console-table';

    const thead = document.createElement('thead');
    thead.innerHTML = `
      <tr style="background:var(--bg); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
        <th style="${thSt} padding-left:18px;">Annuncio</th>
        <th style="${thSt}">Città</th>
        <th style="${thSt} text-align:center;">Tipo</th>
        <th style="${thSt}">Pubblicato</th>
        <th style="${thSt} text-align:right;">Prezzo</th>
        <th style="${thSt} text-align:center;">Score <span class="info-icon" tabindex="0" data-tip="Sconto % vs media CEX (🔥≥30%, 🟢≥20%, 🟡≥10%). Q = qualità annuncio 0-100 (foto, città, tipo venditore).">ℹ️</span></th>
        <th style="${thSt} text-align:center; width:32px;"></th>
        <th style="${thSt} text-align:center;">Disp.</th>
      </tr>`;
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (const ad of ads) {
      const isAvail = !!ad.last_available;
      const isPro   = ad.seller_type === 'professionale';

      const tr = document.createElement('tr');
      tr.className = 'product-row' + (isAvail ? '' : ' product-row-esaurito');

      // Annuncio (link + badge AI)
      const tdName = document.createElement('td');
      tdName.className = 'prod-name-cell';
      tdName.style.position = 'relative';
      let titleHtml = '';
      if (ad.url) {
        titleHtml = `<a href="${ad.url}" target="_blank" rel="noopener noreferrer" class="prod-link" title="${ad.name}">${ad.name}</a>`;
      } else {
        titleHtml = `<span class="prod-link">${ad.name}</span>`;
      }
      
      // Aggiungi score AI se disponibile
      let aiBadge = '';
      if (ad.ai_confidence != null) {
        const conf = ad.ai_confidence;
        let c = conf >= 75 ? '#2e7d32' : conf <= 25 ? '#b71c1c' : '#ef6c00';
        aiBadge = `<span style="font-size:9px; font-weight:bold; color:${c}; margin-left:5px; border:1px solid ${c}; border-radius:3px; padding:0 3px;">AI:${conf}%</span>`;
      }
      
      // Aggiungi pulsanti per revisione (delegation via data-* attributes)
      let reviewActions = '';
      if (aiFilter === 'pending' || ad.ai_status === 'pending') {
         reviewActions = `
           <div style="margin-top:4px;">
             <button data-ai-action="approved" data-ad-id="${ad.id}" style="font-size:10px; cursor:pointer; background:#2e7d32; color:white; border:none; border-radius:3px; margin-right:4px;">Approva</button>
             <button data-ai-action="rejected" data-ad-id="${ad.id}" style="font-size:10px; cursor:pointer; background:#b71c1c; color:white; border:none; border-radius:3px;">Scarta</button>
           </div>
         `;
      }
      
      tdName.innerHTML = titleHtml + aiBadge + reviewActions;
      tr.appendChild(tdName);

      // Città
      const tdCity = document.createElement('td');
      tdCity.style.cssText = 'padding:6px 10px; font-size:12px; color:var(--text-muted); white-space:nowrap;';
      tdCity.textContent = ad.city || '—';
      tr.appendChild(tdCity);

      // Tipo venditore
      const tdSeller = document.createElement('td');
      tdSeller.style.cssText = 'padding:6px 10px; text-align:center;';
      const selBadge = document.createElement('span');
      selBadge.style.cssText = `
        font-size:9px; font-weight:700; text-transform:uppercase;
        padding:2px 5px; border-radius:3px; letter-spacing:0.5px;
        background:${isPro ? 'rgba(255,102,0,0.15)' : 'rgba(120,120,120,0.12)'};
        color:${isPro ? '#ff6600' : 'var(--text-muted)'};
      `;
      selBadge.textContent = isPro ? 'PRO' : 'PRV';
      selBadge.title = isPro ? 'Venditore professionale' : 'Privato';
      tdSeller.appendChild(selBadge);
      tr.appendChild(tdSeller);

      // Data pubblicazione
      const tdDate = document.createElement('td');
      tdDate.style.cssText = 'padding:6px 10px; font-size:11px; color:var(--text-muted); white-space:nowrap;';
      tdDate.textContent = _fmtSubitoDate(ad.published_at);
      tr.appendChild(tdDate);

      // Prezzo
      const tdPrice = document.createElement('td');
      tdPrice.className = 'prod-price-cell';
      tdPrice.textContent = ad.last_price != null ? '€ ' + ad.last_price.toFixed(2) : '—';
      tr.appendChild(tdPrice);

      // Deal Score
      const tdScore = document.createElement('td');
      tdScore.style.cssText = 'padding:6px 10px; text-align:center; white-space:nowrap;';
      tdScore.innerHTML = _renderOpportunityBadge(ad, _oppByUrn[ad.urn_id], _famAvgs);
      tr.appendChild(tdScore);

      // Chart button
      const tdChart = document.createElement('td');
      tdChart.style.cssText = 'padding:4px 6px; text-align:center;';
      const chartBtn = document.createElement('button');
      chartBtn.className = 'chart-btn';
      chartBtn.textContent = '📈';
      chartBtn.title = 'Mostra storico prezzi';
      chartBtn.dataset.chartUrn  = ad.urn_id;
      chartBtn.dataset.chartName = ad.name;
      tdChart.appendChild(chartBtn);
      tr.appendChild(tdChart);

      // Disponibilità
      const tdAvail = document.createElement('td');
      tdAvail.className = 'prod-avail-cell';
      const dot = document.createElement('span');
      dot.className = 'avail-dot ' + (isAvail ? 'ok' : 'ko');
      dot.title = isAvail ? 'Disponibile' : 'Venduto / Scaduto';
      tdAvail.appendChild(dot);
      tr.appendChild(tdAvail);

      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    const tableWrapper = document.createElement('div');
    tableWrapper.className = 'console-table-wrapper';
    tableWrapper.appendChild(table);
    card.appendChild(tableWrapper);
    grid.appendChild(card);
  }
}

// Delegated click handler per Subito: AI buttons + chart buttons (un solo listener)
document.getElementById('subito-grid')?.addEventListener('click', e => {
  // AI review buttons
  const aiBtn = e.target.closest('[data-ai-action]');
  if (aiBtn) {
    const adId  = parseInt(aiBtn.dataset.adId, 10);
    const action = aiBtn.dataset.aiAction;
    if (adId && action) updateAiStatus(adId, action);
    return;
  }
  // Chart buttons
  const chartBtn = e.target.closest('.chart-btn[data-chart-urn]');
  if (chartBtn) {
    e.preventDefault();
    e.stopPropagation();
    _showHistoryModal(chartBtn.dataset.chartUrn, chartBtn.dataset.chartName);
    return;
  }
});

// ============================================================
// SUBITO — VENDUTI
// ============================================================
let SUBITO_SOLD      = [];
let SUBITO_SOLD_STATS = {};

async function loadSubitoSoldData() {
  const btn = document.getElementById('btn-load-sold');
  if (btn) btn.disabled = true;
  try {
    const _fb = { ok: false, body: null };
    const [rSold, rStats] = _settled(
      await Promise.allSettled([
        API.fetchJson('/api/subito/sold'),
        API.fetchJson('/api/subito/sold-stats'),
      ]),
      [_fb, _fb],
    );
    if (rSold.ok)  SUBITO_SOLD       = _sanitizeRows(rSold.body);
    if (rStats.ok) SUBITO_SOLD_STATS = _sanitizeRow(rStats.body);
    _renderSubitoSoldStats();
    renderSubitoSold();
  } catch (e) {
    console.warn('Subito sold load:', e.message);
  } finally {
    if (btn) btn.disabled = false;
  }
}

function _renderSubitoSoldStats() {
  const cont = document.getElementById('subito-sold-stats');
  if (!cont) return;
  const byFam = (SUBITO_SOLD_STATS.by_family || []);
  const global = SUBITO_SOLD_STATS.global || {};
  const byWeekday = SUBITO_SOLD_STATS.by_weekday || [];
  const byHour = SUBITO_SOLD_STATS.by_hour || [];
  const byPeriod = SUBITO_SOLD_STATS.by_period || [];
  if (!byFam.length && !global.total_sold) { cont.innerHTML = ''; return; }

  const weekdayMap = ['Dom', 'Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab'];
  const periodMap = {
    night: 'notte',
    morning: 'mattina',
    afternoon: 'pomeriggio',
    evening: 'sera',
  };

  const peakDay = byWeekday.length ? byWeekday[0] : null;
  const peakHour = byHour.length ? byHour[0] : null;
  const peakPeriod = byPeriod.length ? byPeriod[0] : null;

  const globalCards = [];
  if (global.total_sold) {
    const avgGlobal = global.avg_price != null ? '€ ' + Math.round(global.avg_price) : '—';
    const activeGlobal = global.avg_hours_active != null
      ? (global.avg_hours_active < 48
          ? Math.round(global.avg_hours_active) + ' ore'
          : Math.round(global.avg_hours_active / 24) + ' giorni')
      : '—';
    const windowGlobal = global.avg_sold_window_hours != null
      ? (global.avg_sold_window_hours < 48
          ? Math.round(global.avg_sold_window_hours) + ' ore'
          : Math.round(global.avg_sold_window_hours / 24) + ' giorni')
      : '—';
    const dayText = peakDay ? `${weekdayMap[peakDay.weekday_idx] || peakDay.weekday_idx} (${peakDay.count})` : '—';
    const hourText = peakHour ? `${String(peakHour.hour).padStart(2, '0')}:00 (${peakHour.count})` : '—';
    const periodText = peakPeriod ? `${periodMap[peakPeriod.period] || peakPeriod.period} (${peakPeriod.count})` : '—';

    globalCards.push(`
      <div style="background:var(--bg-card); border:1px solid var(--border); border-radius:10px; padding:14px 16px;">
        <div style="font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:0.6px; color:#ff6600; margin-bottom:6px;">Panoramica Vendite</div>
        <div style="font-size:20px; font-weight:800; color:var(--text);">${global.total_sold} venduti</div>
        <div style="font-size:11px; color:var(--text-muted); margin-top:2px;">prezzo medio ${avgGlobal}</div>
        <div style="font-size:11px; color:var(--text-muted);">tempo attivo medio ${activeGlobal}</div>
        <div style="font-size:11px; color:var(--text-muted);">finestra stima vendita ${windowGlobal}</div>
        <div style="font-size:11px; color:var(--text-muted); margin-top:4px; border-top:1px solid var(--border); padding-top:4px;">picco: giorno ${dayText} · ora ${hourText} · fascia ${periodText}</div>
      </div>
    `);
  }

  const familyCards = byFam.map(r => {
    const fk    = r.console_family || 'other';
    const label = FAMILY_LABELS[fk] || fk;
    const avg   = r.avg_price != null ? '€ ' + Math.round(r.avg_price) : '—';
    const range = (r.min_price != null && r.max_price != null)
      ? `€ ${Math.round(r.min_price)} – € ${Math.round(r.max_price)}`
      : '—';
    const hours = r.avg_hours_active != null
      ? (r.avg_hours_active < 48
          ? Math.round(r.avg_hours_active) + ' ore'
          : Math.round(r.avg_hours_active / 24) + ' giorni')
      : '—';
    const soldWindow = r.avg_sold_window_hours != null
      ? (r.avg_sold_window_hours < 48
          ? Math.round(r.avg_sold_window_hours) + ' ore'
          : Math.round(r.avg_sold_window_hours / 24) + ' giorni')
      : '—';
    return `
      <div style="background:var(--bg-card); border:1px solid var(--border); border-radius:10px; padding:14px 16px;">
        <div style="font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:0.6px; color:#ff6600; margin-bottom:6px;">${label}</div>
        <div style="font-size:22px; font-weight:800; color:var(--text);">${avg}</div>
        <div style="font-size:11px; color:var(--text-muted); margin-top:2px;">media · ${r.count} venduti</div>
        <div style="font-size:11px; color:var(--text-muted);">range ${range}</div>
        <div style="font-size:11px; color:var(--text-muted); margin-top:4px; border-top:1px solid var(--border); padding-top:4px;">⏱ attivo medio ${hours}</div>
        <div style="font-size:11px; color:var(--text-muted);">⏳ finestra vendita ${soldWindow}</div>
      </div>`;
  });

  cont.innerHTML = [...globalCards, ...familyCards].join('');
}

function renderSubitoSold() {
  const grid    = document.getElementById('subito-sold-grid');
  const countEl = document.getElementById('subito-sold-count');
  if (!grid) return;

  if (!SUBITO_SOLD.length) {
    grid.innerHTML = '<div style="color:var(--text-muted); font-size:13px; padding:12px 0;">Nessun venduto rilevato — esegui <code>python3 verify_sold.py</code></div>';
    if (countEl) countEl.textContent = '';
    return;
  }

  const q      = (document.getElementById('subito-sold-search')?.value || '').toLowerCase().trim();
  const famF   = document.getElementById('subito-sold-family')?.value || '';

  let rows = SUBITO_SOLD.slice();
  if (q)    rows = rows.filter(a => a.name.toLowerCase().includes(q));
  if (famF) rows = rows.filter(a => a.console_family === famF);

  if (countEl) countEl.textContent = rows.length + ' venduti';

  const thSt = 'padding:6px 10px; font-size:9.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.7px; color:var(--text-muted);';
  const table = document.createElement('table');
  table.className = 'console-table';
  table.innerHTML = `<thead><tr style="background:var(--bg); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
    <th style="${thSt} padding-left:18px;">Annuncio</th>
    <th style="${thSt}">Famiglia</th>
    <th style="${thSt}">Città</th>
    <th style="${thSt}">Venduto il</th>
    <th style="${thSt}">Attivo per</th>
    <th style="${thSt}">Finestra</th>
    <th style="${thSt} text-align:right;">Prezzo</th>
  </tr></thead>`;

  const tbody = document.createElement('tbody');
  for (const ad of rows) {
    const tr = document.createElement('tr');
    tr.className = 'product-row product-row-esaurito';

    // Calcola tempo attivo: usa published_at (data pubblicazione reale su Subito)
    // con fallback a first_seen (quando lo scraper lo ha trovato per la prima volta)
    let activeStr = '—';
    let soldWindowStr = '—';
    const startDate = ad.published_at || ad.first_seen;
    const soldRefRaw = ad.sold_at_estimated || ad.sold_at;
    if (startDate && soldRefRaw) {
      const ms  = new Date(soldRefRaw) - new Date(startDate);
      const hrs = Math.round(ms / 3600000);
      activeStr = hrs < 48 ? hrs + ' ore' : Math.round(hrs / 24) + ' giorni';
    }
    if (ad.sold_window_hours != null) {
      const wh = Math.round(Number(ad.sold_window_hours));
      soldWindowStr = wh < 48 ? wh + ' ore' : Math.round(wh / 24) + ' giorni';
    }

    const famLabel = FAMILY_LABELS[ad.console_family || 'other'] || ad.console_family || '—';
    const soldDate = soldRefRaw ? _fmtSubitoDate(String(soldRefRaw).replace('T', ' ').substring(0, 16)) : '—';
    const soldPrefix = ad.sold_at_estimated ? '~' : '';

    tr.innerHTML = `
      <td class="prod-name-cell" style="max-width:320px;">
        ${ad.url ? `<a href="${ad.url}" target="_blank" rel="noopener noreferrer" class="prod-link" title="${ad.name}">${ad.name}</a>` : `<span>${ad.name}</span>`}
      </td>
      <td style="padding:6px 10px; font-size:12px; color:#ff6600; font-weight:600; white-space:nowrap;">${famLabel}</td>
      <td style="padding:6px 10px; font-size:12px; color:var(--text-muted); white-space:nowrap;">${ad.city || '—'}</td>
      <td style="padding:6px 10px; font-size:11px; color:var(--text-muted); white-space:nowrap;">${soldPrefix}${soldDate}</td>
      <td style="padding:6px 10px; font-size:11px; color:var(--text-muted); white-space:nowrap;">${activeStr}</td>
      <td style="padding:6px 10px; font-size:11px; color:var(--text-muted); white-space:nowrap;">${soldWindowStr}</td>
      <td class="prod-price-cell">${ad.last_price != null ? '€ ' + ad.last_price.toFixed(2) : '—'}</td>`;
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  grid.innerHTML = '';
  grid.appendChild(table);
}

// ============================================================
// ============================================================
// Subito — Scrape dal browser
// ============================================================
let _scrapePoller = null;

async function startSubitoScrape() {
  const btn  = document.getElementById('btn-scrape-subito');
  const bar  = document.getElementById('subito-scrape-bar');
  const prog = document.getElementById('subito-scrape-progress');
  const msg  = document.getElementById('subito-scrape-msg');

  btn.disabled = true;
  btn.textContent = '⏳ In corso…';
  bar.style.display = 'block';
  prog.className = 'running';
  msg.textContent = 'Avvio scraper Subito.it…';

  try {
    const res = await API.fetchJson('/api/scrape/run', { method: 'POST' });
    if (!res.ok && res.error === 'already-running') {
      msg.textContent = '⚠️ Scraper già in esecuzione.';
      _pollScrapeStatus();
      return;
    }
    if (!res.ok) throw new Error(res.error || 'Errore avvio');
    _pollScrapeStatus();
  } catch(e) {
    prog.className = 'done-err';
    msg.textContent = '❌ ' + e.message;
    btn.disabled = false;
    btn.textContent = '▶ Scrapa';
  }
}

function _pollScrapeStatus() {
  if (_scrapePoller) clearInterval(_scrapePoller);
  _scrapePoller = setInterval(async () => {
    try {
      const s    = await API.fetchJson('/api/scrape/status');
      const bar  = document.getElementById('subito-scrape-bar');
      const prog = document.getElementById('subito-scrape-progress');
      const msg  = document.getElementById('subito-scrape-msg');
      const btn  = document.getElementById('btn-scrape-subito');

      // Mostra ultima riga di log significativa
      const lines = (s.lines || []).filter(l => l.includes('[INFO]') || l.includes('[WARNING]') || l.includes('[ERROR]'));
      const last  = lines[lines.length - 1] || '';
      const clean = last.replace(/^\d{2}:\d{2}:\d{2} \[.*?\] /, '').trim();
      if (clean) msg.textContent = clean;

      if (s.done || !s.running) {
        clearInterval(_scrapePoller);
        _scrapePoller = null;
        if (s.ok) {
          prog.className = 'done-ok';
          msg.textContent = '✅ Scrape completato! Ricarico i dati…';
          btn.textContent = '▶ Scrapa';
          btn.disabled = false;
          setTimeout(async () => {
            await loadSubitoData();
            msg.textContent = '✅ Dati aggiornati.';
            setTimeout(() => { bar.style.display = 'none'; prog.className = ''; }, 3000);
          }, 1000);
        } else {
          prog.className = 'done-err';
          msg.textContent = '❌ Errore: ' + (s.error || 'scraper terminato con errore');
          btn.textContent = '▶ Scrapa';
          btn.disabled = false;
        }
      }
    } catch(e) { /* ignora errori di rete transitori */ }
  }, 2000);
}

// ============================================================
// Subito — aggiornamento stato AI (chiamata dai bottoni Approva/Scarta)
// ============================================================
async function updateAiStatus(adId, status) {
  try {
    const res = await API.fetchJson('/api/subito/update-ai', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: adId, status }),
    });
    if (!res.ok) {
      console.warn('updateAiStatus fallito:', res.status, res.body);
      alert('Errore aggiornamento stato: ' + (res.body?.error || res.status));
      return;
    }
    // Aggiorna localmente senza ricaricare tutto
    const ad = SUBITO_ADS.find(a => a.id === adId);
    if (ad) ad.ai_status = status;
    renderSubito();
  } catch (e) {
    console.error('updateAiStatus eccezione:', e);
  }
}

// ============================================================
// Tooltip globale (non viene tagliato da overflow dei container)
// ============================================================
(function _initGlobalTooltip() {
  const tip = document.createElement('div');
  tip.id = 'global-tooltip';
  document.body.appendChild(tip);

  let _visible = false;

  document.addEventListener('mouseover', e => {
    const icon = e.target.closest('.info-icon[data-tip]');
    if (icon) {
      tip.textContent = icon.dataset.tip;
      tip.style.display = 'block';
      _visible = true;
    }
  });
  document.addEventListener('mouseout', e => {
    if (e.target.closest('.info-icon[data-tip]')) {
      tip.style.display = 'none';
      _visible = false;
    }
  });
  document.addEventListener('mousemove', e => {
    if (!_visible) return;
    const x = e.clientX + 14;
    const y = e.clientY - 10;
    const tw = tip.offsetWidth;
    const th = tip.offsetHeight;
    // Evita uscita dallo schermo
    tip.style.left = (x + tw > window.innerWidth  ? x - tw - 28 : x) + 'px';
    tip.style.top  = (y + th > window.innerHeight ? y - th - 4  : y) + 'px';
  });
})();

// ============================================================
// Caricamento dati
// ============================================================
document.getElementById('btn-refresh').addEventListener('click', () => {
  tryAutoLoad();
  // Se il tab Subito è attivo, ricarica anche i suoi dati
  const activeTab = document.querySelector('.tab.active');
  if (activeTab && activeTab.dataset.tab === 'subito') loadSubitoData();
  if (activeTab && activeTab.dataset.tab === 'ebay')   loadEbayData();
});

async function tryAutoLoad() {
  try {
    const _fb = { ok: false, body: null };
    const [rSrc, rComb, rDbProds, rBase, rStorage, rStandard, rHistory] = _settled(
      await Promise.allSettled([
        API.fetchJson('/api/sources'),
        API.fetchJson('/api/combined/latest'),
        API.fetchJson('/api/db/products'),
        API.fetchJson('/api/db/base-models'),
        API.fetchJson('/api/db/storage-sizes'),
        API.fetchJson('/api/db/standard-groups'),
        API.fetchJson('/api/db/price-history'),
      ]),
      [_fb, _fb, _fb, _fb, _fb, _fb, _fb],
    );

    if (rSrc.ok) {
      SOURCES_META = _sanitizeRows(rSrc.body);
      _populateSourceFilter();
    }
    
    if (rHistory && rHistory.ok) {
      PRICE_HISTORY = rHistory.body || [];
    }

    if (rComb.ok) {
      const data = _sanitizeRow(rComb.body);
      ALL_PRODUCTS = _sanitizeRows(data.products ?? []);

      ALL_DATA = {};
      for (const src of SOURCES_META) {
        ALL_DATA[src.id] = {
          scraped_at: SAN.sanitizeText(src.last_scraped),
          products:   ALL_PRODUCTS.filter(p => p.source === src.id),
        };
      }

      const latestTs = [...SOURCES_META].map(s => s.last_scraped).filter(Boolean).sort().pop();
      document.getElementById('last-update').textContent = latestTs
        ? 'Agg. ' + _fmtDate(latestTs) : '—';

      renderShop();
    }

    if (rDbProds.ok) {
      DB_PRODUCTS = _sanitizeRows(rDbProds.body);
    }

    if (rBase.ok) {
      BASE_MODELS = _sanitizeRows(rBase.body);
    }

    if (rStorage.ok) {
      STORAGE_SIZES = _sanitizeRows(rStorage.body);
      _populateStorageFilter();
    }

    if (rStandard.ok) {
      STANDARD_GROUPS = _sanitizeRows(rStandard.body);
    }

    _populateCatalogoFilters();
    renderHome();
    applyFilters();
    renderStandardGroups();
    _syncState();

  } catch (e) {
    console.warn('Auto-load:', e.message);
  }
}

// Bootstrap token dal server (senza query string), poi carica i dati
(async () => {
  if (API.bootstrapToken) await API.bootstrapToken();
  tryAutoLoad();
})();

// ============================================================
// EBAY VIEW (Feature 6)
// ============================================================

const PARTS_RE = /ricamb[io]|per pezzi|for parts|non funzionante|rott[ao]|difettosa|malfunzionante|cannibalizz/i;

async function loadEbayData() {
  try {
    const _fb = { ok: false, body: null };
    const [rSold, rStats] = _settled(
      await Promise.allSettled([
        API.fetchJson('/api/ebay/sold'),
        API.fetchJson('/api/ebay/stats'),
      ]),
      [_fb, _fb],
    );
    if (rSold.ok) {
      let items = _sanitizeRows(rSold.body);
      const excluded = items.filter(item => PARTS_RE.test(item.name || ''));
      items = items.filter(item => !PARTS_RE.test(item.name || ''));
      EBAY_SOLD = items;
      const countEl = document.getElementById('ebay-count');
      if (countEl && excluded.length > 0) {
        countEl.dataset.excludedParts = excluded.length;
      }
    }
    if (rStats.ok) {
      const s = _sanitizeRow(rStats.body);
      const o = s.overall || {};
      const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
      set('es-total', o.total  ?? '—');
      set('es-avg',   o.avg_price != null ? '€ ' + Math.round(o.avg_price) : '—');
      set('es-min',   o.min_price != null ? '€ ' + o.min_price.toFixed(0)  : '—');
      set('es-max',   o.max_price != null ? '€ ' + o.max_price.toFixed(0)  : '—');

      // Riepilogo per famiglia
      _renderEbaySummary(s.by_family || []);
    }
    renderEbay();
    _syncState();
  } catch (e) {
    console.warn('eBay load:', e.message);
  }
}

function _renderEbaySummary(byFamily) {
  const cont = document.getElementById('ebay-summary');
  if (!cont || !byFamily.length) return;

  const famOrder = [...CONSOLE_FAMILIES.map(f => f.key), 'other'];
  const sorted = byFamily
    .filter(r => r.count > 0)
    .sort((a, b) => famOrder.indexOf(a.console_family) - famOrder.indexOf(b.console_family));

  let html = '<div class="ebay-summary-grid">';
  for (const r of sorted) {
    const fk    = r.console_family || 'other';
    const label = FAMILY_LABELS[fk] || fk;
    const familyKey = fk.replace(/[^a-z0-9]/gi, '-').toLowerCase();
    html += `
      <div class="ebay-summary-card" style="cursor:pointer" onclick="(function(){const el=document.getElementById('ebay-section-${familyKey}');if(el){el.scrollIntoView({behavior:'smooth',block:'start'});el.classList.add('ebay-flash');setTimeout(()=>el.classList.remove('ebay-flash'),1000);}})()">
        <div class="ebay-sum-family">${label}</div>
        <div class="ebay-sum-avg">€ ${r.avg_price != null ? Math.round(r.avg_price) : '—'}</div>
        <div class="ebay-sum-meta">media · ${r.count} venduti</div>
        <div class="ebay-sum-range">
          min €${r.min_price != null ? r.min_price.toFixed(0) : '—'} &nbsp;—&nbsp;
          max €${r.max_price != null ? r.max_price.toFixed(0) : '—'}
        </div>
      </div>`;
  }
  html += '</div>';
  cont.innerHTML = html;
}

function renderEbay() {
  const grid = document.getElementById('ebay-grid');
  if (!grid) return;
  grid.innerHTML = '';

  if (!EBAY_SOLD.length) {
    grid.innerHTML = '<div class="empty-shop">Nessun dato — esegui <code>python3 run.py --source ebay</code></div>';
    return;
  }

  const q         = (document.getElementById('ebay-search')?.value  || '').toLowerCase().trim();
  const famFilter = document.getElementById('ebay-filter-family')?.value || '';

  let filtered = EBAY_SOLD.slice();
  if (q)         filtered = filtered.filter(i => i.name.toLowerCase().includes(q));
  if (famFilter) filtered = filtered.filter(i => i.console_family === famFilter);

  const countEl = document.getElementById('ebay-count');
  if (countEl) {
    const excludedParts = parseInt(countEl.dataset.excludedParts || '0', 10);
    let countText = filtered.length + ' lotti';
    if (excludedParts > 0) countText += ` · ${excludedParts} esclusi (ricambi)`;
    countEl.textContent = countText;
  }

  // Raggruppa per famiglia
  const byFamily = {};
  for (const item of filtered) {
    const fk = item.console_family || 'other';
    if (!byFamily[fk]) byFamily[fk] = [];
    byFamily[fk].push(item);
  }

  const familyOrder = [...CONSOLE_FAMILIES.map(f => f.key), 'other'];
  const thSt = 'padding:6px 10px; font-size:9.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.7px; color:var(--text-muted);';

  for (const fk of familyOrder) {
    const items = byFamily[fk];
    if (!items?.length) continue;

    // Ordina per data vendita decrescente, poi per prezzo
    items.sort((a, b) => {
      const da = a.sold_date || '', db = b.sold_date || '';
      if (da !== db) return da < db ? 1 : -1;
      return (a.sold_price ?? 0) - (b.sold_price ?? 0);
    });

    const prices   = items.map(i => i.sold_price).filter(v => v != null && v > 0);
    const avgPrice = prices.length ? prices.reduce((s, v) => s + v, 0) / prices.length : null;
    const minPrice = prices.length ? Math.min(...prices) : null;
    const famLabel = FAMILY_LABELS[fk] || fk;

    const card = document.createElement('div');
    card.className = 'store-card';
    card.id = 'ebay-section-' + fk.replace(/[^a-z0-9]/gi, '-').toLowerCase();
    card.style.setProperty('--store-accent', '#e53935');

    const header = document.createElement('div');
    header.className = 'store-card-header';
    header.innerHTML = `
      <span class="store-name" style="color:#e53935;">${famLabel}</span>
      <span class="store-header-right">
        <span class="store-count">${items.length} venduti</span>
        ${avgPrice != null ? `<span class="card-min-price" style="margin-left:10px;font-size:12px;color:var(--text-muted);">media € ${Math.round(avgPrice)}</span>` : ''}
      </span>`;
    card.appendChild(header);

    const table = document.createElement('table');
    table.className = 'console-table';

    const thead = document.createElement('thead');
    thead.innerHTML = `
      <tr style="background:var(--bg); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
        <th style="${thSt} padding-left:18px;">Titolo</th>
        <th style="${thSt}">Venduto il</th>
        <th style="${thSt} text-align:right;">Prezzo</th>
      </tr>`;
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (const item of items) {
      const tr = document.createElement('tr');
      tr.className = 'product-row';

      const tdName = document.createElement('td');
      tdName.className = 'prod-name-cell';
      if (item.url) {
        const a = document.createElement('a');
        a.href = item.url; a.target = '_blank'; a.rel = 'noopener noreferrer';
        a.className = 'prod-link'; a.textContent = item.name; a.title = item.name;
        tdName.appendChild(a);
      } else {
        const span = document.createElement('span');
        span.className = 'prod-link'; span.textContent = item.name;
        tdName.appendChild(span);
      }
      tr.appendChild(tdName);

      const tdDate = document.createElement('td');
      tdDate.style.cssText = 'padding:6px 10px; font-size:11px; color:var(--text-muted); white-space:nowrap;';
      tdDate.textContent = item.sold_date || '—';
      tr.appendChild(tdDate);

      const tdPrice = document.createElement('td');
      tdPrice.className = 'prod-price-cell';
      tdPrice.style.cssText += 'color:#e53935;';
      tdPrice.textContent = item.sold_price != null ? '€ ' + item.sold_price.toFixed(0) : '—';
      tr.appendChild(tdPrice);

      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    const tableWrapper = document.createElement('div');
    tableWrapper.className = 'console-table-wrapper';
    tableWrapper.appendChild(table);
    card.appendChild(tableWrapper);
    grid.appendChild(card);
  }
}

// ============================================================
// PRICE HISTORY CHART (Feature 4)
// ============================================================

function _buildChartSVG(points) {
  const W = 500, H = 200;
  const PAD = { top: 28, right: 20, bottom: 36, left: 52 };
  const cW = W - PAD.left - PAD.right;
  const cH = H - PAD.top - PAD.bottom;

  const prices = points.map(p => p.price);
  const minP = Math.min(...prices);
  const maxP = Math.max(...prices);
  const pRange = maxP - minP || maxP * 0.2 || 20;
  const pLo = minP - pRange * 0.15;
  const pHi = maxP + pRange * 0.15;

  const times = points.map(p => p.ts.getTime());
  const minT  = Math.min(...times);
  const maxT  = Math.max(...times);
  const tRange = maxT - minT || 86_400_000;

  const xOf = t => PAD.left + (t - minT) / tRange * cW;
  const yOf = p => PAD.top  + cH - (p - pLo) / (pHi - pLo) * cH;

  let svg = `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" style="display:block;overflow:visible">`;

  // Background
  svg += `<rect x="${PAD.left}" y="${PAD.top}" width="${cW}" height="${cH}" fill="#fafaf8" rx="2"/>`;

  // Grid lines (4 horizontal)
  for (let i = 0; i <= 4; i++) {
    const pVal = pLo + (pHi - pLo) * i / 4;
    const y = yOf(pVal);
    svg += `<line x1="${PAD.left}" y1="${y.toFixed(1)}" x2="${PAD.left + cW}" y2="${y.toFixed(1)}" stroke="#e8e8e4" stroke-width="1"/>`;
    svg += `<text x="${PAD.left - 6}" y="${(y + 4).toFixed(1)}" text-anchor="end" font-size="10" fill="#bbb" font-family="system-ui,sans-serif">€${pVal.toFixed(0)}</text>`;
  }

  // Area fill
  const first = points[0], last = points[points.length - 1];
  const areaPts = [
    `${xOf(first.ts.getTime()).toFixed(1)},${(PAD.top + cH).toFixed(1)}`,
    ...points.map(p => `${xOf(p.ts.getTime()).toFixed(1)},${yOf(p.price).toFixed(1)}`),
    `${xOf(last.ts.getTime()).toFixed(1)},${(PAD.top + cH).toFixed(1)}`,
  ].join(' ');
  svg += `<polygon points="${areaPts}" fill="rgba(255,102,0,0.07)"/>`;

  // Line
  const linePts = points.map(p => `${xOf(p.ts.getTime()).toFixed(1)},${yOf(p.price).toFixed(1)}`).join(' ');
  svg += `<polyline points="${linePts}" fill="none" stroke="#ff6600" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>`;

  // Dots + labels
  for (let i = 0; i < points.length; i++) {
    const p = points[i];
    const x = xOf(p.ts.getTime()), y = yOf(p.price);
    const isFirst = i === 0, isLast = i === points.length - 1;

    // X-axis date label (first + last + if few points)
    if (isFirst || isLast || points.length <= 5) {
      const d = p.ts.toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit' });
      svg += `<text x="${x.toFixed(1)}" y="${(PAD.top + cH + 16).toFixed(1)}" text-anchor="middle" font-size="9" fill="#bbb" font-family="system-ui,sans-serif">${d}</text>`;
    }

    // Dot colour: green if cheaper than first, red if more expensive
    const dotColor = p.type === 'new' ? '#888'
      : p.price < prices[0] ? '#00a040'
      : p.price > prices[0] ? '#d44'
      : '#ff6600';
    svg += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="4.5" fill="${dotColor}" stroke="#fff" stroke-width="1.5"/>`;

    // Price label (above dot, or below if too close to top)
    const lblY = y - 12 > PAD.top ? y - 9 : y + 17;
    svg += `<text x="${x.toFixed(1)}" y="${lblY.toFixed(1)}" text-anchor="middle" font-size="10" fill="#444" font-weight="600" font-family="system-ui,sans-serif">€${p.price.toFixed(0)}</text>`;
  }

  svg += '</svg>';
  return svg;
}

function _showHistoryModal(urnId, adName) {
  // Cleanup: rimuovi overlay precedente e i suoi listener
  const old = document.getElementById('price-history-modal');
  if (old) old.remove();

  const safeUrnId = SAN.sanitizeText(urnId);
  const safeAdName = SAN.sanitizeText(adName);

  const overlay = document.createElement('div');
  overlay.id = 'price-history-modal';
  overlay.className = 'history-overlay';
  // Un solo handler delegato: click su sfondo o su bottone close
  overlay.addEventListener('click', e => {
    if (e.target === overlay || e.target.closest('.history-close')) {
      overlay.remove();
    }
  });

  const card = document.createElement('div');
  card.className = 'history-card';

  card.innerHTML = `
    <div class="history-header">
      <div class="history-title-group">
        <div class="history-title">${safeAdName}</div>
        <div class="history-urn">${safeUrnId}</div>
      </div>
      <button class="history-close" aria-label="Chiudi">×</button>
    </div>
    <div id="history-body" class="history-body">
      <div class="history-loading">Caricamento…</div>
    </div>
  `;

  overlay.appendChild(card);
  document.body.appendChild(overlay);

  API.fetchJson(`/api/subito/ad-history?urn_id=${encodeURIComponent(urnId)}`)
    .then((res) => _sanitizeRow(res.body))
    .then(data => {
      const changes = data.changes || [];
      const body = document.getElementById('history-body');

      // Build price timeline: only entries with a price value
      const points = [];
      for (const c of changes) {
        if (['new', 'price', 'both'].includes(c.change_type) && c.price_new != null) {
          points.push({ ts: new Date(c.changed_at), price: c.price_new, type: c.change_type });
        }
      }

      let html = '';

      if (!points.length) {
        html = '<div class="history-empty">Nessun dato storico disponibile per questo annuncio.</div>';
      } else if (points.length === 1) {
        html = `
          <div class="history-single">
            <div class="history-price-big">€ ${points[0].price.toFixed(0)}</div>
            <div class="history-price-note">Prezzo attuale · nessuna variazione rilevata</div>
          </div>`;
      } else {
        html += `<div class="history-chart">${_buildChartSVG(points)}</div>`;
      }

      // Change log (price changes only)
      const priceChanges = changes.filter(c =>
        (c.change_type === 'price' || c.change_type === 'both') &&
        c.price_old != null && c.price_new != null
      );
      if (priceChanges.length) {
        html += '<div class="history-log">';
        html += '<div class="history-log-title">Variazioni prezzo</div>';
        for (const c of priceChanges) {
          const diff  = c.price_new - c.price_old;
          const arrow = diff < 0 ? '↓' : '↑';
          const cls   = diff < 0 ? 'log-down' : 'log-up';
          const date  = new Date(c.changed_at).toLocaleDateString('it-IT', { day:'2-digit', month:'2-digit', year:'2-digit' });
          html += `<div class="history-log-row">
            <span class="log-date">${date}</span>
            <span class="log-prices">€${c.price_old.toFixed(0)} → <strong>€${c.price_new.toFixed(0)}</strong></span>
            <span class="log-diff ${cls}">${arrow} €${Math.abs(diff).toFixed(0)}</span>
          </div>`;
        }
        html += '</div>';
      }

      // First seen / last seen
      const ad = data.ad || {};
      if (ad.first_seen) {
        const fs = new Date(ad.first_seen).toLocaleDateString('it-IT', { day:'2-digit', month:'2-digit', year:'2-digit' });
        const ls = new Date(ad.last_seen).toLocaleDateString('it-IT', { day:'2-digit', month:'2-digit', year:'2-digit' });
        html += `<div class="history-meta">Prima vista: <strong>${fs}</strong> &nbsp;·&nbsp; Ultimo aggiornamento: <strong>${ls}</strong></div>`;
      }

      body.innerHTML = html;
    })
    .catch(err => {
      const safeErr = SAN.sanitizeText(err.message || String(err));
      document.getElementById('history-body').innerHTML =
        `<div class="history-empty" style="color:#c44">Errore: ${safeErr}</div>`;
    });
}

// ============================================================
// TREND VIEW — Grafici storici prezzi
// ============================================================

function _buildTrendSvg(series, { width = 520, height = 200, padding = { top: 20, right: 20, bottom: 30, left: 50 } } = {}) {
  if (!series.length) return '<div class="history-empty">Nessun dato storico</div>';

  const w = width - padding.left - padding.right;
  const h = height - padding.top - padding.bottom;

  const prices = series.map(s => s.price);
  const minP = Math.min(...prices);
  const maxP = Math.max(...prices);
  const range = maxP - minP || 1;

  // Parse dates
  const dates = series.map(s => new Date(s.date));
  const minT = dates[0].getTime();
  const maxT = dates[dates.length - 1].getTime();
  const spanT = maxT - minT || 1;

  const x = (d) => padding.left + ((d.getTime() - minT) / spanT) * w;
  const y = (p) => padding.top + h - ((p - minP) / range) * h;

  // Build polyline
  const points = series.map((s, i) => `${x(dates[i]).toFixed(1)},${y(s.price).toFixed(1)}`).join(' ');

  // Area fill
  const areaPoints = points + ` ${x(dates[dates.length - 1]).toFixed(1)},${(padding.top + h).toFixed(1)} ${x(dates[0]).toFixed(1)},${(padding.top + h).toFixed(1)}`;

  // Y-axis ticks (5 ticks)
  const yTicks = [];
  for (let i = 0; i <= 4; i++) {
    const val = minP + (range * i / 4);
    yTicks.push({ val, cy: y(val) });
  }

  // X-axis ticks (max 5 date labels)
  const xTicks = [];
  const step = Math.max(1, Math.floor(series.length / 5));
  for (let i = 0; i < series.length; i += step) {
    xTicks.push({ label: series[i].date.slice(5), cx: x(dates[i]) });
  }
  // Always add last
  if (xTicks.length && xTicks[xTicks.length - 1].label !== series[series.length - 1].date.slice(5)) {
    xTicks.push({ label: series[series.length - 1].date.slice(5), cx: x(dates[dates.length - 1]) });
  }

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const gridColor = isDark ? '#333' : '#eee';
  const textColor = isDark ? '#888' : '#999';
  const lineColor = '#4a90d9';
  const fillColor = isDark ? 'rgba(74,144,217,0.12)' : 'rgba(74,144,217,0.08)';
  const dotColor = lineColor;

  // Price delta
  const first = prices[0];
  const last = prices[prices.length - 1];
  const delta = last - first;
  const deltaPct = ((delta / first) * 100).toFixed(1);
  const deltaColor = delta < 0 ? '#00a040' : delta > 0 ? '#c44' : textColor;
  const deltaSign = delta > 0 ? '+' : '';

  return `<svg viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg" style="width:100%; height:auto;">
    <!-- Grid lines -->
    ${yTicks.map(t => `<line x1="${padding.left}" y1="${t.cy.toFixed(1)}" x2="${width - padding.right}" y2="${t.cy.toFixed(1)}" stroke="${gridColor}" stroke-width="0.5"/>`).join('')}
    <!-- Y labels -->
    ${yTicks.map(t => `<text x="${padding.left - 6}" y="${(t.cy + 3).toFixed(1)}" text-anchor="end" font-size="9" fill="${textColor}">€${Math.round(t.val)}</text>`).join('')}
    <!-- X labels -->
    ${xTicks.map(t => `<text x="${t.cx.toFixed(1)}" y="${height - 6}" text-anchor="middle" font-size="9" fill="${textColor}">${t.label}</text>`).join('')}
    <!-- Area -->
    <polygon points="${areaPoints}" fill="${fillColor}"/>
    <!-- Line -->
    <polyline points="${points}" fill="none" stroke="${lineColor}" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>
    <!-- Dots start/end -->
    <circle cx="${x(dates[0]).toFixed(1)}" cy="${y(first).toFixed(1)}" r="3" fill="${dotColor}"/>
    <circle cx="${x(dates[dates.length - 1]).toFixed(1)}" cy="${y(last).toFixed(1)}" r="3" fill="${dotColor}"/>
    <!-- Delta label -->
    <text x="${width - padding.right}" y="${padding.top - 4}" text-anchor="end" font-size="11" font-weight="600" fill="${deltaColor}">${deltaSign}€${Math.round(delta)} (${deltaSign}${deltaPct}%)</text>
  </svg>`;
}

function renderTrend() {
  const grid  = document.getElementById('trend-grid');
  const empty = document.getElementById('trend-empty');
  const count = document.getElementById('trend-count');
  grid.innerHTML = '';

  if (!BASE_MODELS.length || !PRICE_HISTORY.length) {
    empty.style.display = 'block';
    grid.style.display  = 'none';
    count.textContent   = '';
    return;
  }

  const filterFamily = document.getElementById('trend-filter-family').value;
  const periodDays   = parseInt(document.getElementById('trend-period').value, 10);

  // Cutoff date
  let cutoff = null;
  if (periodDays > 0) {
    const d = new Date();
    d.setDate(d.getDate() - periodDays);
    cutoff = d.toISOString().split('T')[0];
  }

  const uniqueGroups = getUniqueBaseModels();

  const cards = [];
  for (const group of uniqueGroups) {
    const rep = group.items[0];
    const fk = rep.console_family || _familyKey(rep.name);

    // Filter by family
    if (filterFamily && fk !== filterFamily) continue;

    // Get product IDs for this group
    const productIds = group.items.map(p => p.id).filter(Boolean);
    if (!productIds.length) continue;

    // Get daily series using StatEngine
    let series = StatEngine.getDailyHistory(productIds);
    if (!series.length) continue;

    // Filter by period
    if (cutoff) {
      series = series.filter(s => s.date >= cutoff);
    }
    if (series.length < 2) continue;

    const title = _comboTitle({ rep });
    const storageLabel = rep.storage_label || '';
    const condBadge = rep.condition === 'Nuovo'
      ? '<span class="badge badge-nuovo">Nuovo</span>'
      : rep.condition === 'Usato'
        ? '<span class="badge badge-usato">Usato</span>'
        : '';

    const currentPrice = series[series.length - 1].price;
    const minPrice = Math.min(...series.map(s => s.price));
    const maxPrice = Math.max(...series.map(s => s.price));

    cards.push({
      fk,
      title,
      html: `
        <div class="trend-card">
          <div class="trend-card-header">
            <div class="trend-card-title">${SAN.sanitizeText(title)}</div>
            <div class="trend-card-meta">
              ${condBadge}
              ${storageLabel ? `<span class="storage-badge">${SAN.sanitizeText(storageLabel)}</span>` : ''}
            </div>
          </div>
          <div class="trend-card-chart">${_buildTrendSvg(series)}</div>
          <div class="trend-card-footer">
            <span>Attuale: <strong>€${Math.round(currentPrice)}</strong></span>
            <span>Min: €${Math.round(minPrice)}</span>
            <span>Max: €${Math.round(maxPrice)}</span>
            <span>${series.length} giorni</span>
          </div>
        </div>
      `
    });
  }

  if (!cards.length) {
    empty.style.display = 'block';
    grid.style.display  = 'none';
    count.textContent   = '';
    return;
  }

  empty.style.display = 'none';
  grid.style.display  = '';
  count.textContent   = `${cards.length} grafici`;

  // Sort by family order, then title
  const familyIdx = {};
  CONSOLE_FAMILIES.forEach((f, i) => { familyIdx[f.key] = i; });
  cards.sort((a, b) => (familyIdx[a.fk] ?? 99) - (familyIdx[b.fk] ?? 99) || a.title.localeCompare(b.title, 'it'));

  grid.innerHTML = cards.map(c => c.html).join('');
}

// ============================================================
// RICERCA VIEW
// ============================================================

const _SUBMODELS = {
  '':         ['Base','E','Slim','Elite','X','S'],
  'Original': ['Base'],
  '360':      ['Base','E','Slim','Elite'],
  'One':      ['Base','S','X'],
  'Series':   ['X','S'],
};

let _ricercaInited = false;

function initRicerca() {
  if (_ricercaInited) return;
  _ricercaInited = true;

  // Popola storage dal DB
  ViewerApi.fetchJson('/api/db/storage-sizes').then(sizes => {
    const sel = document.getElementById('rf-storage');
    (sizes || []).forEach(s => {
      const opt = document.createElement('option');
      opt.value = s.label;
      opt.textContent = s.label;
      sel.appendChild(opt);
    });
  }).catch(() => {});

  // Toggle button groups
  document.querySelectorAll('.rbtn-group').forEach(group => {
    group.addEventListener('click', e => {
      const btn = e.target.closest('.rbtn');
      if (!btn) return;
      group.querySelectorAll('.rbtn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      // Aggiorna sotto-modelli se è il gruppo famiglia
      if (group.id === 'rf-family') _updateSubmodels(btn.dataset.val);
    });
  });

  _updateSubmodels('');
}

function _updateSubmodels(family) {
  const group   = document.getElementById('rf-submodel');
  const options = _SUBMODELS[family] || _SUBMODELS[''];
  group.innerHTML = '<button class="rbtn active" data-val="">Tutti</button>';
  options.forEach(sm => {
    const btn = document.createElement('button');
    btn.className = 'rbtn';
    btn.dataset.val = sm;
    btn.textContent = sm;
    group.appendChild(btn);
    btn.addEventListener('click', () => {
      group.querySelectorAll('.rbtn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
    });
  });
  // Abilita/disabilita campo sotto-modello
  const field = document.getElementById('rfield-submodel');
  field.style.opacity = options.length <= 1 ? '0.4' : '1';
  field.style.pointerEvents = options.length <= 1 ? 'none' : '';
}

function _getRicercaParams() {
  const get = id => {
    const el = document.getElementById(id);
    if (!el) return '';
    if (el.tagName === 'SELECT') return el.value;
    const active = el.querySelector('.rbtn.active');
    return active ? active.dataset.val : '';
  };
  return {
    base_family:    get('rf-family'),
    sub_model:      get('rf-submodel'),
    edition_name:   document.getElementById('rf-edition').value,
    storage_label:  document.getElementById('rf-storage').value,
    color:          document.getElementById('rf-color').value,
    has_kinect:     get('rf-kinect'),
    available_only: document.getElementById('rf-available').checked ? '1' : '',
  };
}

function runRicerca() {
  const params = _getRicercaParams();
  const qs = Object.entries(params)
    .filter(([, v]) => v !== '' && v !== null && v !== undefined)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');

  const status  = document.getElementById('ricerca-status');
  const results = document.getElementById('ricerca-results');
  status.textContent  = 'Ricerca in corso…';
  results.innerHTML   = '';

  ViewerApi.fetchJson('/api/db/search' + (qs ? '?' + qs : ''))
    .then(data => _renderRicercaResults(data.body, params))
    .catch(err => {
      status.textContent = 'Errore: ' + SAN.sanitizeText(err.message || String(err));
    });
}

function _renderRicercaResults(rows, params) {
  const status  = document.getElementById('ricerca-status');
  const container = document.getElementById('ricerca-results');

  if (!rows || rows.length === 0) {
    status.textContent = 'Nessun risultato trovato.';
    container.innerHTML = '';
    return;
  }

  // Raggruppa per standard_key → standard_name
  const groups = new Map();
  rows.forEach(p => {
    const key = p.standard_key || p.standard_name || p.name || '?';
    if (!groups.has(key)) {
      groups.set(key, { name: p.standard_name || p.name || '?', items: [] });
    }
    groups.get(key).items.push(p);
  });

  status.textContent = `${rows.length} prodotti trovati in ${groups.size} configurazioni`;

  const html = [];
  groups.forEach((group, key) => {
    const items = group.items.sort((a, b) => (a.last_price || 0) - (b.last_price || 0));

    const rows_html = items.map(p => {
      const price    = p.last_price != null ? p.last_price.toFixed(2) + ' €' : '—';
      const avail    = p.last_available ? '<span class="badge-avail">Disp.</span>' : '<span class="badge-unavail">Esaurito</span>';
      const storeAcc = _storeAccent(p.source);
      const storeLbl = _storeLabel(p.source);
      const cond     = SAN.sanitizeText(p.condition || '');
      const pack     = SAN.sanitizeText(p.packaging_state || '');
      const url      = SAN.sanitizeUrl(p.url || '');
      const nameLink = url
        ? `<a href="${url}" target="_blank" rel="noopener noreferrer">${storeLbl}</a>`
        : storeLbl;
      return `<tr>
        <td><span class="source-badge" style="background:${storeAcc}">${nameLink}</span></td>
        <td>${cond}</td>
        <td>${pack}</td>
        <td class="price-cell">${price}</td>
        <td>${avail}</td>
      </tr>`;
    }).join('');

    const stdName = SAN.sanitizeText(group.name);
    html.push(`
      <div class="ricerca-group">
        <div class="ricerca-group-head">
          <span class="ricerca-group-name">${stdName}</span>
          <span class="ricerca-group-count">${items.length} offerte</span>
        </div>
        <table class="ricerca-table">
          <thead><tr>
            <th>Store</th><th>Condizione</th><th>Packaging</th>
            <th>Prezzo</th><th>Stato</th>
          </tr></thead>
          <tbody>${rows_html}</tbody>
        </table>
      </div>
    `);
  });

  container.innerHTML = html.join('');
}
