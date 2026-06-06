import { NAV, domainOf } from "./nav";
import { route, navigate } from "./router";
import { theme, toggleTheme } from "./theme";
import { loadAll, loading, lastUpdate } from "./store/data";
import css from "./app.module.css";

function View() {
  const r = route.value;
  // Le viste reali verranno montate qui nelle prossime migliorie (parità col viewer attuale).
  return (
    <div class={css.placeholder}>
      Vista <strong>{r.domain} / {r.sub}</strong> — in costruzione.
    </div>
  );
}

export function App() {
  const r = route.value;
  const dom = domainOf(r.domain) ?? NAV[0];
  return (
    <>
      <header class={css.header}>
        <div class={css.row1}>
          <div class={css.logo}>
            <img src="/assets/trader-icon.svg" alt="" aria-hidden="true" /> Xbox Tracker
          </div>
          <nav class={css.domains}>
            {NAV.map((d) => (
              <button
                class={`${css.domain} ${d.id === r.domain ? css.domainActive : ""}`}
                onClick={() => navigate(d.id, d.subs[0].id)}
              >
                {d.label}
              </button>
            ))}
          </nav>
          <div class={css.right}>
            <span style="font-size:11.5px;color:var(--text-muted)">
              {lastUpdate.value ? "Agg. " + new Date(lastUpdate.value).toLocaleString("it-IT") : ""}
            </span>
            <button class={css.iconbtn} title="Tema chiaro/scuro" onClick={toggleTheme}>
              {theme.value === "dark" ? "🌙" : "☀️"}
            </button>
            <button class={css.iconbtn} title="Aggiorna dati" disabled={loading.value} onClick={() => void loadAll()}>↻</button>
            <a class={css.iconbtn} href="/log" target="_blank" rel="noopener" title="Stato log">🩺</a>
          </div>
        </div>
        <div class={css.subs}>
          {dom.subs.map((s) => (
            <button
              class={`${css.sub} ${s.id === r.sub ? css.subActive : ""}`}
              onClick={() => navigate(dom.id, s.id)}
            >
              {s.label}
            </button>
          ))}
        </div>
      </header>
      <main class={css.main}>
        <View />
      </main>
    </>
  );
}
