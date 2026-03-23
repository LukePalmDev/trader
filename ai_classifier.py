import asyncio
import json
import logging
import os
import sqlite3
from pathlib import Path

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("ai_classifier")

import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_subito import DB_PATH, _connect

try:
    from anthropic import AsyncAnthropic
except ImportError:
    log.error("Il modulo 'anthropic' non è installato. Esegui: pip install anthropic")
    sys.exit(1)

# Lista modelli da provare in ordine di costo (dal più economico)
HAIKU_MODELS = [
    "claude-haiku-4-5-20251001",   # Haiku attuale (più economico)
    "claude-3-5-haiku-20241022",   # Haiku legacy fallback
    "claude-sonnet-4-6",           # Sonnet attuale (fallback finale)
]

SELECTED_MODEL = os.environ.get("ANTHROPIC_MODEL")

BATCH_SIZE = 50
MAX_CONCURRENT_BATCHES = 5  # batch API in parallelo (riduce tempo da ~30min a ~6min)

SYSTEM_PROMPT = """Classifica se l'annuncio vende una console Xbox hardware (qualsiasi modello).
Score: 100 (console), 0 (solo giochi/accessori), 50 (ambiguo).
Rispondi ESCLUSIVAMENTE con un array JSON di oggetti: [{"id": int, "console_confidence": int}].
Niente testo aggiuntivo, preamboli o spiegazioni. Solo il JSON."""


async def classify_batch(client: AsyncAnthropic, batch: list) -> list[dict]:
    """Invia un batch di annunci a Claude e restituisce le valutazioni."""
    ads_text = []
    for row in batch:
        ads_text.append(f"ID: {row['id']} | Titolo: {row['name']} | Prezzo: {row['last_price']} | URL: {row['url']}")
    
    user_message = "Valuta i seguenti annunci:\n" + "\n".join(ads_text)

    try:
        response = await client.messages.create(
            model=SELECTED_MODEL,
            max_tokens=2048,
            temperature=0.0,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}]
        )
        
        reply_text = response.content[0].text.strip()
        # Clean any markdown formatting if present
        if "```json" in reply_text:
            reply_text = reply_text.split("```json")[1].split("```")[0].strip()
        elif "```" in reply_text:
            reply_text = reply_text.split("```")[1].split("```")[0].strip()
        try:
            data = json.loads(reply_text)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "ads" in data:
                return data["ads"]
            return []
        except Exception as e:
            # Fallback robusto con Regex per superare virgole finali, key non quotate o backtick non chiusi
            import re
            matches = re.finditer(r'["\']?id["\']?\s*:\s*(\d+).*?["\']?console_confidence["\']?\s*:\s*(\d+)', reply_text, re.IGNORECASE | re.DOTALL)
            results = []
            for m in matches:
                results.append({"id": int(m.group(1)), "console_confidence": int(m.group(2))})
            
            if results:
                log.info("Recuperati %d risultati dal testo malformato tramite fallback regex.", len(results))
                return results

            log.error("Errore classificazione AI e fallback fallito: %s\nText: %s", e, reply_text[:200])
            return []
            
    except Exception as e:
        log.error("Errore di rete/API durante la classificazione: %s", e)
        return []

async def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("La variabile d'ambiente ANTHROPIC_API_KEY non è impostata.")
        log.info("Esegui: export ANTHROPIC_API_KEY='la-tua-chiave'")
        return

    client = AsyncAnthropic(api_key=api_key)
    
    global SELECTED_MODEL
    if not SELECTED_MODEL:
        log.info("Rilevamento modello Haiku disponibile...")
        for model in HAIKU_MODELS:
            try:
                # Test veloce con 1 solo token per verificare disponibilità modello
                await client.messages.create(
                    model=model,
                    max_tokens=1,
                    messages=[{"role": "user", "content": "ping"}]
                )
                SELECTED_MODEL = model
                log.info("Modello rilevato e selezionato: %s", SELECTED_MODEL)
                break
            except Exception:
                continue
        
        if not SELECTED_MODEL:
            log.error("Nessun modello disponibile (Haiku 4.5, Haiku legacy, Sonnet 4.6 tutti falliti).")
            log.warning("Verifica il tuo Tier e le API Key su https://console.anthropic.com/settings/plans")
            return

    conn = _connect(DB_PATH)
    
    # Seleziona annunci in pending e senza classificazione AI
    rows = conn.execute(
        "SELECT id, urn_id, name, last_price, url FROM ads WHERE ai_status = 'pending' AND ai_confidence IS NULL"
    ).fetchall()
    
    if not rows:
        log.info("Nessun annuncio da classificare. Il DB è aggiornato!")
        return
        
    log.info("Trovati %d annunci pending da classificare con %s.", len(rows), SELECTED_MODEL)

    total_updated = 0
    sem = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
    tot_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE

    async def _process_one_batch(batch_idx: int, batch: list) -> list[dict]:
        """Processa un singolo batch con semaphore per limitare la concorrenza."""
        num_batch = batch_idx + 1
        async with sem:
            log.info(
                "Elaborazione batch %d/%d (annunci %d-%d)...",
                num_batch, tot_batches,
                batch_idx * BATCH_SIZE + 1,
                min((batch_idx + 1) * BATCH_SIZE, len(rows)),
            )
            results = await classify_batch(client, batch)
            if not results:
                log.warning(
                    "Nessun risultato valido restituito dall'API per il batch %d. "
                    "Riproverà al prossimo avvio.", num_batch,
                )
            return results or []

    # Lancia tutti i batch in parallelo (il semaphore limita a MAX_CONCURRENT_BATCHES)
    batches = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
    all_results = await asyncio.gather(
        *[_process_one_batch(idx, batch) for idx, batch in enumerate(batches)]
    )

    # Applica i risultati al DB (sequenziale — SQLite non supporta write concorrenti)
    for batch_results in all_results:
        for res in batch_results:
            ad_id = res.get("id")
            conf = res.get("console_confidence")

            if ad_id is None or conf is None:
                continue

            try:
                conf = int(conf)
            except ValueError:
                continue

            # Determina lo status
            if conf >= 75:
                status = "approved"
            elif conf <= 25:
                status = "rejected"
            else:
                status = "pending"

            try:
                conn.execute(
                    "UPDATE ads SET ai_confidence = ?, ai_status = ? WHERE id = ?",
                    (conf, status, ad_id),
                )
                total_updated += 1
            except Exception as e:
                log.error("Errore aggiornamento db per id %s: %s", ad_id, e)

    try:
        conn.commit()
    except sqlite3.OperationalError:
        pass  # auto-commit potrebbe essere già attivo

    log.info(
        "Classificazione completata. %d annunci analizzati e aggiornati "
        "(concorrenza: %d batch paralleli).",
        total_updated, MAX_CONCURRENT_BATCHES,
    )

if __name__ == "__main__":
    asyncio.run(main())
