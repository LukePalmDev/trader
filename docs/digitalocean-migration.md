# Migrazione Trader su DigitalOcean

Questa procedura porta il progetto da Mac locale a VPS Linux sempre accesa.
La prima versione e' volutamente conservativa:

- codice su GitHub
- dati runtime sul server in `/var/lib/trader`
- viewer/API su `127.0.0.1` dietro reverse proxy Nginx per `trader.byluke.org`
- automazioni con `systemd timers`, non dipendenti dal Mac
- niente token o `.env` committati

## Scelta server

Per questo progetto partire con:

- Ubuntu 24.04 LTS
- Droplet DigitalOcean Basic
- 2 GB RAM consigliati per Playwright
- 50 GB disco se disponibile nel piano scelto
- autenticazione SSH key, non password

Il piano da 1 GB puo' bastare per test, ma Subito/eBay + Playwright sono piu'
stabili con 2 GB.

## Stato progetto preparato

Il codice supporta queste variabili server:

```bash
TRADER_DB_PATH=/var/lib/trader/tracker.db
TRADER_OUTPUT_DIR=/var/lib/trader/data
TRADER_VIEWER_HOST=127.0.0.1
TRADER_VIEWER_PORT=8080
TRADER_VIEWER_OPEN_BROWSER=false
TRADER_PLAYWRIGHT_CHANNEL=chromium
```

Quindi il DB non deve piu' vivere dentro al checkout Git.

## Passi minimi

### 1. Crea il Droplet

In DigitalOcean:

1. Create Droplet
2. Ubuntu 24.04 LTS
3. datacenter vicino all'Italia, per esempio Frankfurt
4. piano Basic 2 GB RAM
5. aggiungi la tua SSH key
6. crea il server

Annota l'IP pubblico.

### 2. Primo accesso

Dal Mac:

```bash
ssh root@IP_DEL_SERVER
```

Aggiorna il server:

```bash
apt-get update && apt-get upgrade -y
```

### 3. Se il repo e' privato, prepara una deploy key

Sul server:

```bash
adduser --system --create-home --shell /bin/bash trader
install -d -m 700 -o trader -g trader /home/trader/.ssh
sudo -u trader ssh-keygen -t ed25519 -N "" -C "trader-digitalocean" -f /home/trader/.ssh/github_deploy_key
cat >/home/trader/.ssh/config <<'EOF'
Host github.com
  IdentityFile /home/trader/.ssh/github_deploy_key
  IdentitiesOnly yes
EOF
chown trader:trader /home/trader/.ssh/config
chmod 600 /home/trader/.ssh/config
cat /home/trader/.ssh/github_deploy_key.pub
```

Poi su GitHub:

```text
Repo > Settings > Deploy keys > Add deploy key
Title: trader-digitalocean
Key: incolla la chiave pubblica
Allow write access: OFF
```

Se il repo e' pubblico, puoi saltare questo passo.

### 4. Bootstrap server

Repo pubblico:

```bash
curl -fsSL https://raw.githubusercontent.com/LukePalmDev/trader/main/deploy/bootstrap_ubuntu.sh -o /tmp/bootstrap_ubuntu.sh
bash /tmp/bootstrap_ubuntu.sh https://github.com/LukePalmDev/trader.git
```

Repo privato con deploy key:

```bash
git clone git@github.com:LukePalmDev/trader.git /opt/trader/app
bash /opt/trader/app/deploy/bootstrap_ubuntu.sh git@github.com:LukePalmDev/trader.git
```

Lo script installa:

- Python 3.11 + virtualenv
- dipendenze Python da `requirements.lock`
- Chromium Playwright/Patchright
- service viewer
- timer scraping/classificazione/verifica
- backup SQLite giornaliero
- file env in `/etc/trader/trader.env`

### 5. Copia il DB attuale dal Mac

Dal Mac, non dal server:

```bash
rsync -avz tracker.db root@IP_DEL_SERVER:/var/lib/trader/tracker.db
ssh root@IP_DEL_SERVER 'chown trader:trader /var/lib/trader/tracker.db'
```

Se vuoi portare anche gli snapshot:

```bash
rsync -avz data/ root@IP_DEL_SERVER:/var/lib/trader/data/
ssh root@IP_DEL_SERVER 'chown -R trader:trader /var/lib/trader/data'
```

### 6. Configura i segreti

Sul server:

```bash
nano /etc/trader/trader.env
```

Compila solo cio' che usi:

```bash
TRADER_TELEGRAM_ENABLED=true
TRADER_TELEGRAM_TOKEN=...
TRADER_TELEGRAM_CHAT_ID=...
OPENAI_API_KEY=sk-or-v1-...
OPENAI_BASE_URL=https://openrouter.ai/api/v1
OPENAI_CASCADE_MODELS=openai/gpt-4o-mini,openai/gpt-4.1-mini,openai/gpt-5-mini
```

Poi:

```bash
systemctl restart trader-viewer.service
```

### 7. Verifica servizi

Sul server:

```bash
systemctl status trader-viewer.service
systemctl list-timers 'trader-*'
journalctl -u trader-viewer.service -n 80 --no-pager
```

Esegui un job manuale:

```bash
sudo -u trader /opt/trader/app/deploy/server_job.sh scrape-fonti
```

### 8. Apri il viewer

Con dominio configurato:

```text
https://trader.byluke.org/
```

Per debug locale via tunnel SSH:

```bash
ssh -L 8080:127.0.0.1:8080 root@IP_DEL_SERVER
```

Poi apri:

```text
http://127.0.0.1:8080/
```

## GitHub dopo la migrazione

Dal 3 giugno 2026 gli schedule GitHub sono stati rimossi dall’operatività e archiviati in
`STORICI3GIUGNO/github-workflows/`:

- `Subito.it`
- `Scraper Fonti`
- `eBay`
- `AI Classify`
- `Verify Sold`
- `Quality`

I test/lint si eseguono localmente prima del commit. Le routine periodiche girano sul server
con systemd, documentate in `docs/server-routines.md`.

Il flusso consigliato diventa:

```text
Mac/dev -> commit/push codice su GitHub -> deploy server -> dati restano sul server
```

Per aggiornare codice server:

```bash
sudo -u trader git -C /opt/trader/app pull --rebase
systemctl restart trader-viewer.service
```

## Backup

Il bootstrap installa gia' `trader-backup.timer`, che crea backup in:

```bash
/var/backups/trader
```

Verifica:

```bash
systemctl list-timers trader-backup.timer
sudo -u trader /opt/trader/app/deploy/backup_db.sh
ls -lh /var/backups/trader
```

Questo non sostituisce uno snapshot DigitalOcean, ma copre gli errori applicativi
piu' probabili.

## Rollback

Se qualcosa non va:

```bash
systemctl stop 'trader-*'
```

Il tuo progetto locale resta invariato. Il DB copiato sul server e' una copia del
file locale, quindi puoi sempre tornare a usare il Mac.
