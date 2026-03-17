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
};

const FAMILY_LABELS = {
  'series-x': 'Xbox Series X',
  'series-s': 'Xbox Series S',
  'one-x':    'Xbox One X',
  'one-s':    'Xbox One S',
  'one':      'Xbox One',
  '360':      'Xbox 360',
  'original': 'Xbox Original',
  'other':    'Altro',
};

// ============================================================
// Stato globale
// ============================================================
let SOURCES_META   = [];
let ALL_PRODUCTS   = [];   // da snapshot JSON (usato in "Tutto")
let ALL_DATA       = {};   // { source_id: { scraped_at, products[] } }
let DB_PRODUCTS    = [];   // da DB SQLite (usato in "Catalogo")
let BASE_MODELS    = [];   // DB prodotti con is_base_model=1 (usato in "Home")
let STORAGE_SIZES  = [];   // dimensioni archiviazione dal DB
let currentSort    = { key: 'last_price', dir: 1 };

// ============================================================
// Utilità
// ============================================================
function _familyKey(name) {
  const n = name.toLowerCase();
  for (const f of CONSOLE_FAMILIES) {
    if (f.re.test(n)) return f.key;
  }
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

// ============================================================
// Navigazione tab
// ============================================================
document.querySelectorAll('.tab').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('view-' + btn.dataset.tab).classList.add('active');
    if (btn.dataset.tab === 'home') renderHome();
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

  // Raggruppa base models per famiglia
  const byFamily = {};
  for (const p of BASE_MODELS) {
    const fk = p.console_family || _familyKey(p.name);
    if (!byFamily[fk]) byFamily[fk] = [];
    byFamily[fk].push(p);
  }

  const familyOrder = [...CONSOLE_FAMILIES.map(f => f.key), 'other'];
  for (const fk of familyOrder) {
    const items = byFamily[fk];
    if (!items?.length) continue;

    const section = document.createElement('div');
    section.className = 'home-section';

    const title = document.createElement('h2');
    title.className   = 'home-section-title';
    title.textContent = FAMILY_LABELS[fk] || fk;
    section.appendChild(title);

    const table = document.createElement('table');
    table.className = 'home-table';
    table.innerHTML = `
      <thead>
        <tr>
          <th>Prodotto</th>
          <th style="text-align:center;">Cond.</th>
          <th>Storage</th>
          <th>Store</th>
          <th style="text-align:right;">Prezzo</th>
          <th style="text-align:center;">Disp.</th>
        </tr>
      </thead>
    `;
    const tbody = document.createElement('tbody');

    // Ordina: disponibili prima, poi per prezzo
    const sorted = [...items].sort((a, b) => {
      const da = a.last_available ? 0 : 1;
      const db = b.last_available ? 0 : 1;
      if (da !== db) return da - db;
      return (a.last_price ?? Infinity) - (b.last_price ?? Infinity);
    });

    for (const p of sorted) {
      const isAvail = !!p.last_available;
      const condClass  = p.condition === 'Nuovo' ? 'badge-nuovo' : p.condition === 'Usato' ? 'badge-usato' : 'badge-nd';
      const condLetter = p.condition === 'Nuovo' ? 'N' : p.condition === 'Usato' ? 'U' : '?';

      const tr = document.createElement('tr');
      tr.className = 'home-row' + (isAvail ? '' : ' home-row-esaurito');

      const fk2      = p.console_family || _familyKey(p.name);
      const shortName = fk2 !== 'other' ? _shortName(p.name, fk2) : p.name;
      const storeAcc  = _storeAccent(p.source);

      tr.innerHTML = `
        <td class="home-prod-name">
          ${p.url
            ? `<a href="${p.url}" target="_blank" rel="noopener noreferrer" class="prod-link" title="${p.name}">${shortName || p.name}</a>`
            : `<span class="prod-link" title="${p.name}">${shortName || p.name}</span>`
          }
        </td>
        <td style="text-align:center; padding:8px 10px;">
          <span class="badge ${condClass}" title="${p.condition}">${condLetter}</span>
        </td>
        <td class="home-storage">
          ${p.storage_label
            ? `<span class="storage-badge">${p.storage_label}</span>`
            : '<span style="color:var(--text-muted)">—</span>'
          }
        </td>
        <td class="home-store">
          <span class="badge-source" style="border-color:${storeAcc}40; color:var(--text)">
            ${_storeLabel(p.source)}
          </span>
        </td>
        <td style="text-align:right; font-weight:600; font-size:14px; padding:8px 18px; white-space:nowrap;">
          ${_fmtPrice(p.last_price)}
        </td>
        <td style="text-align:center; padding:8px 14px;">
          <span class="avail-dot ${isAvail ? 'ok' : 'ko'}" title="${isAvail ? 'Disponibile' : 'Esaurito'}"></span>
        </td>
      `;
      tbody.appendChild(tr);
    }

    table.appendChild(tbody);
    section.appendChild(table);
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
    thead.innerHTML = `
      <tr style="background:var(--bg); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
        <th style="padding:6px 18px; font-size:9.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.7px; color:var(--text-muted);">Prodotto</th>
        <th style="padding:6px 10px; font-size:9.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.7px; color:var(--text-muted); text-align:center;">Cond.</th>
        <th style="padding:6px 18px; font-size:9.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.7px; color:var(--text-muted); text-align:right;">Prezzo</th>
        <th style="padding:6px 14px; font-size:9.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.7px; color:var(--text-muted); text-align:center;">Disp.</th>
      </tr>
    `;
    table.appendChild(thead);

    const tbody     = document.createElement('tbody');
    let firstFamily = true;

    const familyOrder = [...CONSOLE_FAMILIES.map(f => f.key), 'other'];
    for (const fk of familyOrder) {
      const items = byFamily[fk];
      if (!items?.length) continue;

      const familyLabel = fk === 'other'
        ? 'Altro'
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
  const onlyAvail   = document.getElementById('filter-available').checked;
  const onlyBase    = document.getElementById('filter-base').checked;

  // Usa DB_PRODUCTS se disponibili, fallback su ALL_PRODUCTS
  const source = DB_PRODUCTS.length ? DB_PRODUCTS : ALL_PRODUCTS.map(p => ({
    ...p, last_price: p.price, last_available: p.available ? 1 : 0,
    console_family: _familyKey(p.name),
  }));

  let filtered = source.slice();
  if (q)             filtered = filtered.filter(p => p.name.toLowerCase().includes(q));
  if (srcFilter)     filtered = filtered.filter(p => p.source === srcFilter);
  if (condFilter)    filtered = filtered.filter(p => p.condition === condFilter);
  if (familyFilter)  filtered = filtered.filter(p => p.console_family === familyFilter);
  if (storageFilter) filtered = filtered.filter(p => p.storage_label === storageFilter);
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
    const res = await fetch('/api/db/set-base', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: productId, value: newValue }),
    });
    const data = await res.json();
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
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:40px;color:var(--text-muted);">Nessun risultato</td></tr>';
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
      idLabel.textContent = p.id;
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
// Caricamento dati
// ============================================================
document.getElementById('btn-refresh').addEventListener('click', tryAutoLoad);

async function tryAutoLoad() {
  try {
    const [rSrc, rComb, rDbProds, rBase, rStorage] = await Promise.all([
      fetch('/api/sources'),
      fetch('/api/combined/latest'),
      fetch('/api/db/products'),
      fetch('/api/db/base-models'),
      fetch('/api/db/storage-sizes'),
    ]);

    if (rSrc.ok) {
      SOURCES_META = await rSrc.json();
      _populateSourceFilter();
    }

    if (rComb.ok) {
      const data = await rComb.json();
      ALL_PRODUCTS = data.products ?? [];

      ALL_DATA = {};
      for (const src of SOURCES_META) {
        ALL_DATA[src.id] = {
          scraped_at: src.last_scraped,
          products:   ALL_PRODUCTS.filter(p => p.source === src.id),
        };
      }

      const latestTs = [...SOURCES_META].map(s => s.last_scraped).filter(Boolean).sort().pop();
      document.getElementById('last-update').textContent = latestTs
        ? 'Agg. ' + _fmtDate(latestTs) : '—';

      renderShop();
    }

    if (rDbProds.ok) {
      DB_PRODUCTS = await rDbProds.json();
    }

    if (rBase.ok) {
      BASE_MODELS = await rBase.json();
    }

    if (rStorage.ok) {
      STORAGE_SIZES = await rStorage.json();
      _populateStorageFilter();
    }

    renderHome();
    applyFilters();

  } catch (e) {
    console.warn('Auto-load:', e.message);
  }
}

tryAutoLoad();
