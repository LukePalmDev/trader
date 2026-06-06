// Routing a hash: #/dominio/sotto-tab → signal reattivo.
import { signal } from "@preact/signals";
import { DEFAULT_ROUTE, domainOf } from "./nav";

export interface Route { domain: string; sub: string; }

function parse(): Route {
  const raw = (location.hash || "").replace(/^#\/?/, "");
  const [domain, sub] = raw.split("/");
  const dom = domainOf(domain);
  if (!dom) return { ...DEFAULT_ROUTE };
  const valid = dom.subs.find((s) => s.id === sub);
  return { domain: dom.id, sub: valid ? sub : dom.subs[0].id };
}

export const route = signal<Route>(parse());

export function navigate(domain: string, sub: string): void {
  location.hash = `#/${domain}/${sub}`;
}

window.addEventListener("hashchange", () => { route.value = parse(); });

// Normalizza l'URL all'avvio (es. apertura senza hash → default).
if (!location.hash) {
  location.replace(`#/${DEFAULT_ROUTE.domain}/${DEFAULT_ROUTE.sub}`);
}
