'use strict';

(function initViewerApi(globalObj) {
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
