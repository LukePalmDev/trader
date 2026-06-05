# Routine Server

Dal 3 giugno 2026 le routine operative non sono più eseguite da GitHub Actions.
I vecchi workflow sono archiviati in `STORICI3GIUGNO/github-workflows/`.

## Timer systemd

| Routine | Unità server | Scopo |
|---------|--------------|-------|
| Scrape fonti shop | `trader-scrape-fonti.timer` / `trader-scrape-fonti.service` | Aggiorna CEX, ReBuy, GameLife, GameShock, GamePeople e fonti shop. |
| Scrape Subito | `trader-scrape-subito.timer` / `trader-scrape-subito.service` | Aggiorna annunci Subito e storico prezzi/disponibilità. |
| Scrape eBay venduti | `trader-scrape-ebay.timer` / `trader-scrape-ebay.service` | Aggiorna lotti venduti eBay. |
| AI classify compat | `trader-ai-classify.timer` / `trader-ai-classify.service` | Alias operativo del cascade GPT, mantenuto per timer già installati. |
| AI cascade classify | `trader-ai-cascade.timer` / `trader-ai-cascade.service` | Classifica nuovi annunci Subito con cascata OpenAI Bibbia-first e coda review. |
| Verify sold | `trader-verify-sold.timer` / `trader-verify-sold.service` | Verifica annunci Subito non più disponibili e marca venduti. |
| Backup DB | `trader-backup.timer` / `trader-backup.service` | Copia il DB operativo in archivio backup. |
| Viewer/API | `trader-viewer.service` | Espone viewer e API locali dietro Nginx. |

## Percorsi attesi sul server

- App: `/opt/trader/app`
- DB runtime: `/var/lib/trader/tracker.db`
- Dati generati: `/var/lib/trader/data`
- Log: `/var/log/trader`
- Backup: `/var/backups/trader`

## OpenRouter

La cascata AI usa OpenRouter come endpoint OpenAI-compatible di default. In
`/etc/trader/trader.env` deve essere compilata solo la chiave:

```bash
OPENAI_API_KEY=sk-or-v1-...
```

`OPENAI_BASE_URL` e `OPENAI_CASCADE_MODELS` sono già valorizzati in
`deploy/trader.env.example` per usare OpenRouter.

## Comandi utili

```bash
systemctl list-timers 'trader-*'
systemctl status trader-viewer.service
journalctl -u trader-scrape-subito.service -n 80 --no-pager
journalctl -u trader-ai-cascade.service -n 80 --no-pager
```

Per applicare nuove modifiche: commit locale, deploy sul server, migrazioni DB, export statico,
restart del viewer e controllo dei timer. GitHub resta solo come repository di codice, non come
motore di esecuzione periodica.
