'use strict';

(function initViewerApi(globalObj) {
  // Modalità statica: GitHub Pages non ha backend Python.
  // I dati vengono letti da file JSON pre-generati in viewer/data/.
  const IS_STATIC = typeof window !== 'undefined' &&
    window.location.hostname.endsWith('github.io');

  // Mappa endpoint API → file statico relativo a viewer/
  const STATIC_MAP = {
    '/api/sources':                        './data/sources.json',
    '/api/combined/latest':                './data/combined-latest.json',
    '/api/db/products':                    './data/db-products.json',
    '/api/db/base-models':                 './data/db-base-models.json',
    '/api/db/storage-sizes':               './data/db-storage-sizes.json',
    '/api/db/standard-groups':             './data/db-standard-groups.json',
    '/api/db/price-history':               './data/db-price-history.json',
    '/api/subito/ads':                     './data/subito-ads.json',
    '/api/subito/stats':                   './data/subito-stats.json',
    '/api/subito/sold':                    './data/subito-sold.json',
    '/api/subito/sold-stats':              './data/subito-sold-stats.json',
    '/api/subito/pending-reviews':          './data/subito-pending-reviews.json',
    '/api/ebay/sold':                      './data/ebay-sold.json',
    '/api/ebay/stats':                     './data/ebay-stats.json',
    '/api/valuation/subito-opportunities': './data/valuation-opportunities.json',
  };

  // Token risolto in ordine: sessionStorage (già acquisito) → bootstrap API
  // Il token NON viene mai letto dalla query string per evitare esposizione
  // in browser history, referer headers e log del server.
  let apiToken = '';

  function _loadCachedToken() {
    try {
      return window.sessionStorage.getItem('trader_api_token') || '';
    } catch {
      return '';
    }
  }

  function _cacheToken(token) {
    try {
      window.sessionStorage.setItem('trader_api_token', token);
    } catch {
      // no-op (private browsing / blocked storage)
    }
  }

  // Migrazione: se c'è un token in localStorage (vecchio sistema), migralo
  // a sessionStorage e rimuovilo da localStorage.
  try {
    const legacyToken = window.localStorage.getItem('trader_api_token');
    if (legacyToken) {
      _cacheToken(legacyToken);
      window.localStorage.removeItem('trader_api_token');
    }
  } catch {
    // no-op
  }

  // Pulisci la query string se contiene un token residuo (vecchi bookmark)
  try {
    const params = new URLSearchParams(window.location.search);
    if (params.has('token')) {
      params.delete('token');
      const clean = params.toString();
      const newUrl = window.location.pathname + (clean ? '?' + clean : '');
      window.history.replaceState({}, '', newUrl);
    }
  } catch {
    // no-op
  }

  apiToken = _loadCachedToken();

  async function bootstrapToken() {
    if (IS_STATIC) return; // Nessun auth necessario su GitHub Pages
    // Acquisisce il token dal server (endpoint senza auth, accessibile solo da localhost)
    // Forza sempre il refresh: il server genera un nuovo token ad ogni riavvio,
    // quindi un token in sessionStorage potrebbe essere stale.
    try {
      const res = await fetch('/api/token');
      if (res.ok) {
        const data = await res.json();
        if (data && data.token) {
          apiToken = data.token;
          _cacheToken(apiToken);
        }
      }
    } catch {
      // Server non raggiungibile — le chiamate API falliranno con 401
    }
  }

  function withAuthHeaders(headers = {}) {
    if (!apiToken) return headers;
    return { ...headers, Authorization: `Bearer ${apiToken}` };
  }

  async function fetchJson(url, options = {}) {
    if (IS_STATIC && url.startsWith('/api/')) {
      // Operazioni di scrittura non disponibili in modalità statica
      const method = (options.method || 'GET').toUpperCase();
      if (method !== 'GET') {
        return { ok: false, status: 405, body: { error: 'Non disponibile in modalità statica' } };
      }
      const apiPath = url.split('?')[0];
      const staticFile = STATIC_MAP[apiPath];
      if (!staticFile) {
        return { ok: false, status: 404, body: null };
      }
      try {
        const res = await fetch(staticFile);
        let body = null;
        try { body = await res.json(); } catch { body = null; }
        return { ok: res.ok, status: res.status, body };
      } catch {
        return { ok: false, status: 503, body: null };
      }
    }
    const opts = { ...options };
    opts.headers = withAuthHeaders(opts.headers || {});
    const response = await fetch(url, opts);
    let body = null;
    try {
      body = await response.json();
    } catch {
      body = null;
    }
    return { ok: response.ok, status: response.status, body };
  }

  globalObj.ViewerApi = {
    get apiToken() { return apiToken; },
    fetchJson,
    withAuthHeaders,
    bootstrapToken,
  };
})(window);
