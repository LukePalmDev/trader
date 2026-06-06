// Navigazione a due livelli (domini → sotto-tab). Fonte unica della IA.
export interface SubTab { id: string; label: string; }
export interface Domain { id: string; label: string; subs: SubTab[]; }

export const NAV: Domain[] = [
  { id: "negozi", label: "Negozi", subs: [
    { id: "base", label: "Base" },
    { id: "special", label: "Special" },
    { id: "riepilogo", label: "Riepilogo" },
    { id: "catalogo", label: "Catalogo" },
  ]},
  { id: "subito", label: "Subito", subs: [
    { id: "acquistabili", label: "Acquistabili" },
    { id: "da-rivedere", label: "Da rivedere" },
    { id: "review-ai", label: "Review AI" },
    { id: "venduti", label: "Venduti" },
    { id: "dashboard", label: "Dashboard" },
  ]},
  { id: "ebay", label: "eBay", subs: [
    { id: "venduti", label: "Venduti" },
  ]},
  { id: "analisi", label: "Analisi", subs: [
    { id: "statistiche", label: "Statistiche" },
    { id: "trend", label: "Trend" },
  ]},
  { id: "ricerca", label: "Ricerca", subs: [
    { id: "ricerca", label: "Ricerca" },
  ]},
];

export const DEFAULT_ROUTE = { domain: "negozi", sub: "base" };

export function domainOf(id: string): Domain | undefined {
  return NAV.find((d) => d.id === id);
}
