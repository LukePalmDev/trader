// Tema light/dark coerente con l'attuale (localStorage xbox-tracker-theme, default dark).
import { signal } from "@preact/signals";

const KEY = "xbox-tracker-theme";

export const theme = signal<"light" | "dark">(
  localStorage.getItem(KEY) === "light" ? "light" : "dark",
);

function apply(t: "light" | "dark"): void {
  if (t === "dark") document.documentElement.setAttribute("data-theme", "dark");
  else document.documentElement.removeAttribute("data-theme");
}

export function toggleTheme(): void {
  const next = theme.value === "dark" ? "light" : "dark";
  theme.value = next;
  localStorage.setItem(KEY, next);
  apply(next);
}

apply(theme.value);
