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

function enhanceProduct(p) {
  if (p._enhanced) return p;

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

  let ed = specialEd || p.edition_class || 'standard';
  ed = ed.charAt(0).toUpperCase() + ed.slice(1);
  const parsed_edition = parsed_color ? parsed_color : ed;

  p.parsed_family = pf;
  p.parsed_segment = ps;
  p.parsed_kinect = pk;
  p.parsed_color = parsed_color;
  p.parsed_edition = parsed_edition;

  const storage = p.storage_label || '—';
  const condition = p.condition || '—';
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
    if (btn.dataset.tab === 'ricerca') initRicerca();
  });
});

// ============================================================
// HOME VIEW
// ============================================================
function renderHome() {
  const grid      = document.getElementById('home-grid');
  const emptyCard = document.getElementById('home-empty');
  grid.innerHTML  = '';

  if (!BASE_MODELS.length) {
    emptyCard.style.display = '';
    grid.style.display      = 'none';
    return;
  }

  emptyCard.style.display = 'none';
  grid.style.display      = '';

  BASE_MODELS.forEach(p => enhanceProduct(p));
  const baseKeys = [...new Set(BASE_MODELS.map(p => p.combo_key))];

  const unifiedPool = [];
  const sourceProducts = DB_PRODUCTS.length ? DB_PRODUCTS : ALL_PRODUCTS;
  sourceProducts.forEach(p => {
    enhanceProduct(p);
    unifiedPool.push(p);
  });

  const byFamily = {};
  for (const k of baseKeys) {
    const matching = unifiedPool.filter(p => p.combo_key === k);
    if (!matching.length) continue;

    const rep = matching[0];
    const fk = rep.console_family || _familyKey(rep.name);
    if (!byFamily[fk]) byFamily[fk] = [];
    byFamily[fk].push({ key: k, rep, matching });
  }

  const familyOrder = [...CONSOLE_FAMILIES.map(f => f.key), 'other'];
  for (const fk of familyOrder) {
    const combos = byFamily[fk];
    if (!combos?.length) continue;

    const section = document.createElement('div');
    section.className = 'home-section';
    const title = document.createElement('h2');
    title.className   = 'home-section-title';
    title.textContent = FAMILY_LABELS[fk] || fk;
    section.appendChild(title);

    const container = document.createElement('div');
    container.className = 'home-combos-container';

    combos.forEach(combo => {
      const { rep, matching } = combo;

      const avail = matching.filter(p => p.last_available || p.available);
      const prices = avail.map(p => p.last_price ?? p.price).filter(v => v != null);
      const minPx = prices.length ? Math.min(...prices) : null;
      const maxPx = prices.length ? Math.max(...prices) : null;

      const row = document.createElement('div');
      row.className = 'home-combo-row';

      const kinectStr = rep.parsed_kinect ? ' + Kinect' : '';
      const titleStr = `Xbox ${rep.parsed_family} ${rep.parsed_segment} ${rep.parsed_edition}${kinectStr}`;

      const header = document.createElement('div');
      header.className = 'home-combo-header';
      header.onclick = () => row.classList.toggle('open');

      header.innerHTML = `
        <div>
          <div class="home-combo-title">${titleStr}</div>
          <div class="home-combo-tags">
            <span class="badge ${rep.condition === 'Nuovo' ? 'badge-nuovo' : rep.condition === 'Usato' ? 'badge-usato' : 'badge-nd'}">${rep.condition}</span>
            <span class="storage-badge">${rep.storage_label || '—'}</span>
            <span>Imballo: ${rep.packaging_state || 'N/D'}</span>
          </div>
        </div>
        <div class="home-combo-stats">
          <div style="color:var(--text);">${avail.length} store disponibili</div>
          <div style="color:var(--text-muted); font-size: 12px; margin-top:2px;">
            ${prices.length ? (prices.length > 1 ? `da €${minPx.toFixed(0)} a €${maxPx.toFixed(0)}` : `€${minPx.toFixed(0)}`) : 'esaurito tutti gli store'}
          </div>
        </div>
      `;

      const details = document.createElement('div');
      details.className = 'home-combo-details';

      const table = document.createElement('table');
      table.className = 'home-table';
      table.innerHTML = `
        <thead>
          <tr>
            <th>Fonte</th>
            <th>Nome Originale</th>
            <th style="text-align:right;">Prezzo</th>
            <th style="text-align:center;">Disp.</th>
          </tr>
        </thead>
      `;
      const tbody = document.createElement('tbody');

      matching.sort((a,b) => {
        const da = (a.last_available || a.available) ? 0 : 1;
        const db = (b.last_available || b.available) ? 0 : 1;
        if (da !== db) return da - db;
        const pa = a.last_price ?? a.price ?? Infinity;
        const pb = b.last_price ?? b.price ?? Infinity;
        return pa - pb;
      });

      for (const p of matching) {
        const tr = document.createElement('tr');
        const isA = p.last_available || p.available;
        const px = p.last_price ?? p.price;
        tr.innerHTML = `
          <td><span class="badge-source" style="border-color:${_storeAccent(p.source)}40; color:var(--text)">${_storeLabel(p.source)}</span></td>
          <td>
            ${p.url 
              ? `<a href="${p.url}" target="_blank" rel="noopener" class="prod-link" title="${p.name}">${p.name}</a>` 
              : `<span class="prod-link" title="${p.name}">${p.name}</span>`
            }
          </td>
          <td style="text-align:right; font-weight:600;">${px != null ? '€ ' + px.toFixed(2) : '—'}</td>
          <td style="text-align:center;"><span class="avail-dot ${isA ? 'ok': 'ko'}"></span></td>
        `;
        tbody.appendChild(tr);
      }

      table.appendChild(tbody);
      details.appendChild(table);

      row.appendChild(header);
      row.appendChild(details);
      container.appendChild(row);
    });
    section.appendChild(container);
    grid.appendChild(section);
  }
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
    card.appendChild(table);
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
  set('gs-stores', SOURCES_META.filter(s => ALL_DATA[s.id]).length);
  set('gs-avg-u',  uPrices.length ? '€ ' + (_avg(uPrices) ?? 0).toFixed(0) : '—');
  set('gs-avg-n',  nPrices.length ? '€ ' + (_avg(nPrices) ?? 0).toFixed(0) : '—');
  set('gs-min',    allPx.length   ? '€ ' + Math.min(...allPx).toFixed(2)   : '—');
}

function _renderConsoleSummary() {
  const sources = SOURCES_META.filter(s => ALL_DATA[s.id]).map(s => s.id);
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

function applyFilters() {
  const q           = document.getElementById('search').value.toLowerCase().trim();
  const srcFilter   = document.getElementById('filter-source').value;
  const condFilter  = document.getElementById('filter-condition').value;
  const familyFilter= document.getElementById('filter-family').value;
  const storageFilter= document.getElementById('filter-storage').value;
  const segmentFilter = document.getElementById('filter-segment').value;
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
  if (srcFilter)     filtered = filtered.filter(p => p.source === srcFilter);
  if (condFilter)    filtered = filtered.filter(p => p.condition === condFilter);
  if (familyFilter)  filtered = filtered.filter(p => p.parsed_family === familyFilter);
  if (storageFilter) filtered = filtered.filter(p => p.storage_label === storageFilter);
  if (segmentFilter) filtered = filtered.filter(p => p.parsed_segment === segmentFilter);
  if (editionFilter) filtered = filtered.filter(p => (p.edition_class || 'standard').toLowerCase() === editionFilter.toLowerCase() || p.parsed_edition.toLowerCase() === editionFilter.toLowerCase());
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

  document.getElementById('count-label').textContent = filtered.length + ' prodotti';
  _renderTable(filtered);
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
      star.addEventListener('click', () => _toggleBaseModel(p.id, isBase, star));
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
    const [rAds, rStats, rOpp] = await Promise.all([
      API.fetchJson('/api/subito/ads'),
      API.fetchJson('/api/subito/stats'),
      API.fetchJson('/api/valuation/subito-opportunities?limit=500'),
    ]);
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

  // Filtri
  const q         = (document.getElementById('subito-search')?.value  || '').toLowerCase().trim();
  const famFilter = document.getElementById('subito-filter-family')?.value  || '';
  const selFilter = document.getElementById('subito-filter-seller')?.value  || '';
  const segFilter = document.getElementById('subito-filter-segment')?.value || '';
  const edtFilter = document.getElementById('subito-filter-edition')?.value || '';
  const regFilter = document.getElementById('subito-filter-region')?.value  || '';
  const onlyAvail = document.getElementById('subito-filter-avail')?.checked || false;

  let filtered = SUBITO_ADS.slice();
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
        <th style="${thSt} text-align:center;">Score</th>
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

      // Annuncio (link)
      const tdName = document.createElement('td');
      tdName.className = 'prod-name-cell';
      if (ad.url) {
        const a = document.createElement('a');
        a.href = ad.url; a.target = '_blank'; a.rel = 'noopener noreferrer';
        a.className = 'prod-link'; a.textContent = ad.name; a.title = ad.name;
        tdName.appendChild(a);
      } else {
        const span = document.createElement('span');
        span.className = 'prod-link'; span.textContent = ad.name;
        tdName.appendChild(span);
      }
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
      chartBtn.title = 'Storico prezzi';
      chartBtn.addEventListener('click', e => {
        e.preventDefault();
        e.stopPropagation();
        _showHistoryModal(ad.urn_id, ad.name);
      });
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
    card.appendChild(table);
    grid.appendChild(card);
  }
}

// ============================================================
// Caricamento dati
// ============================================================
document.getElementById('btn-refresh').addEventListener('click', tryAutoLoad);

async function tryAutoLoad() {
  try {
    const [rSrc, rComb, rDbProds, rBase, rStorage, rStandard] = await Promise.all([
      API.fetchJson('/api/sources'),
      API.fetchJson('/api/combined/latest'),
      API.fetchJson('/api/db/products'),
      API.fetchJson('/api/db/base-models'),
      API.fetchJson('/api/db/storage-sizes'),
      API.fetchJson('/api/db/standard-groups'),
    ]);

    if (rSrc.ok) {
      SOURCES_META = _sanitizeRows(rSrc.body);
      _populateSourceFilter();
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

    renderHome();
    applyFilters();
    renderStandardGroups();
    _syncState();

  } catch (e) {
    console.warn('Auto-load:', e.message);
  }
}

tryAutoLoad();

// ============================================================
// EBAY VIEW (Feature 6)
// ============================================================

async function loadEbayData() {
  try {
    const [rSold, rStats] = await Promise.all([
      API.fetchJson('/api/ebay/sold'),
      API.fetchJson('/api/ebay/stats'),
    ]);
    if (rSold.ok)  EBAY_SOLD = _sanitizeRows(rSold.body);
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
    html += `
      <div class="ebay-summary-card">
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
  if (countEl) countEl.textContent = filtered.length + ' lotti';

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
    card.appendChild(table);
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
  document.getElementById('price-history-modal')?.remove();
  const safeUrnId = SAN.sanitizeText(urnId);
  const safeAdName = SAN.sanitizeText(adName);

  const overlay = document.createElement('div');
  overlay.id = 'price-history-modal';
  overlay.className = 'history-overlay';
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });

  const card = document.createElement('div');
  card.className = 'history-card';

  card.innerHTML = `
    <div class="history-header">
      <div class="history-title-group">
        <div class="history-title">${safeAdName}</div>
        <div class="history-urn">${safeUrnId}</div>
      </div>
      <button class="history-close" onclick="document.getElementById('price-history-modal').remove()">×</button>
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
