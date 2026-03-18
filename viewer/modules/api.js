'use strict';

(function initViewerApi(globalObj) {
  function _resolveToken() {
    const fromQuery = new URLSearchParams(window.location.search).get('token');
    if (fromQuery) {
      try {
        window.localStorage.setItem('trader_api_token', fromQuery);
      } catch {
        // no-op (private browsing / blocked storage)
      }
      return fromQuery;
    }
    try {
      return window.localStorage.getItem('trader_api_token') || '';
    } catch {
      return '';
    }
  }

  const apiToken = _resolveToken();

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
    apiToken,
    fetchJson,
    withAuthHeaders,
  };
})(window);
