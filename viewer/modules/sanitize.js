'use strict';

(function initViewerSanitize(globalObj) {
  function sanitizeText(value) {
    if (value == null) return '';
    return String(value)
      .replace(/[<>"'`]/g, ' ')
      .replace(/[\u0000-\u001F\u007F]/g, ' ')
      .replace(/\s{2,}/g, ' ')
      .trim();
  }

  function sanitizeUrl(value) {
    const raw = sanitizeText(value);
    if (!raw) return '';
    try {
      const parsed = new URL(raw, window.location.origin);
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return '';
      return parsed.href;
    } catch {
      return '';
    }
  }

  function sanitizeValue(key, value) {
    if (value == null) return value;
    if (Array.isArray(value)) return value.map((item) => sanitizeValue('', item));
    if (typeof value === 'object') return sanitizeRecord(value);
    if (typeof value === 'string') {
      const lowerKey = String(key || '').toLowerCase();
      if (lowerKey.includes('url')) return sanitizeUrl(value);
      return sanitizeText(value);
    }
    return value;
  }

  function sanitizeRecord(record) {
    if (!record || typeof record !== 'object') return {};
    const out = {};
    for (const [key, value] of Object.entries(record)) {
      out[key] = sanitizeValue(key, value);
    }
    return out;
  }

  function sanitizeCollection(items) {
    if (!Array.isArray(items)) return [];
    return items.map((item) => sanitizeRecord(item));
  }

  globalObj.ViewerSanitize = {
    sanitizeText,
    sanitizeUrl,
    sanitizeRecord,
    sanitizeCollection,
  };
})(window);
