# 🏗️ Trader Safeguards - System Architecture

## Overview Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                     TRADER SCRAPING SYSTEM                       │
│                     (WITH SAFEGUARDS)                            │
└──────────────────────────────────────────────────────────────────┘

                    ┌─────────────────────────┐
                    │   SCRAPE EXECUTION      │
                    │   (scrape_with_        │
                    │    safeguards.py)       │
                    └──────────┬──────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         │                     │                     │
         ▼                     ▼                     ▼
    ┌────────────┐         ┌────────────┐      ┌──────────────┐
    │   STAGE 1  │         │   STAGE 3  │      │   STAGE 5    │
    │PRE-HEALTH  │         │  EXECUTE   │      │  VERIFY      │
    │   CHECK    │         │   SCRAPE   │      │ CHANGES      │
    └──────┬─────┘         └──────┬─────┘      └──────┬───────┘
           │                      │                    │
           ▼                      ▼                    ▼
    ┌────────────────────────────────────────────────────────┐
    │        DATABASE HEALTH CHECK MODULE                    │
    │           (db_safeguards.py)                           │
    ├────────────────────────────────────────────────────────┤
    │ • Integrity Check (PRAGMA integrity_check)             │
    │ • File Size Monitoring                                 │
    │ • Table & Record Counts                                │
    │ • WAL File Status                                      │
    │ • Connection Testing                                   │
    │ • Record Verification                                  │
    └─────────────┬───────────────────────────────┬──────────┘
                  │                               │
         ┌────────▼────────┐           ┌──────────▼─────────┐
         │  STAGE 2        │           │  STAGE 4           │
         │  CREATE BACKUP  │           │  POST-HEALTH CHECK │
         │  (Auto-version) │           │  & VERIFICATION    │
         └────────┬────────┘           └──────────┬─────────┘
                  │                               │
                  ▼                               ▼
         ┌──────────────────────────────────────────────┐
         │    BACKUP MANAGER                            │
         │  (DatabaseBackupManager)                     │
         ├──────────────────────────────────────────────┤
         │ • Automatic versioning (keep 5)             │
         │ • Pre-scrape backup creation                │
         │ • Backup restoration capability             │
         │ • Integrity verification before backup      │
         └────────┬─────────────────────────────────────┘
                  │
                  ▼
         ┌──────────────────────────────┐
         │  /backups directory           │
         │  • tracker_backup_*.db        │
         │  • subito_backup_*.db         │
         │  • trader_backup_*.db         │
         │  • (max 5 versions each)      │
         └──────────────────────────────┘


                    ERROR HANDLING FLOW

         ┌──────────────────────────────────┐
         │   ERROR DETECTED ANYWHERE        │
         └────────────┬─────────────────────┘
                      │
         ┌────────────▼────────────────┐
         │  IOErrorRecovery Module      │
         │  (db_safeguards.py)          │
         ├──────────────────────────────┤
         │ • Log error with context     │
         │ • Capture timestamp          │
         │ • Store in error_history     │
         │ • Attempt auto-recovery      │
         └────────────┬─────────────────┘
                      │
         ┌────────────▼─────────────────┐
         │  Recovery Attempt            │
         │  (DatabaseHealthCheck)       │
         ├──────────────────────────────┤
         │ If recovery succeeds:        │
         │  ✅ Continue operation       │
         │                              │
         │ If recovery fails:           │
         │  📋 Log detailed error       │
         │  💾 Keep backup safe         │
         │  📊 Store for analysis       │
         └──────────────────────────────┘


                 MAINTENANCE OPERATIONS

         ┌──────────────────────────────────┐
         │  cleanup_and_optimize.py         │
         ├──────────────────────────────────┤
         │ 1. VACUUM databases              │
         │ 2. Clean __pycache__, .ruff, etc │
         │ 3. Organize backup inventory     │
         │ 4. Reset WAL journals            │
         │ 5. Update .gitignore             │
         └──────────────────────────────────┘

```

## Component Details

### 1. DatabaseHealthCheck Class

```
Input: database_path
Process:
  ├─ Check file exists
  ├─ Check readable/writable
  ├─ Verify size
  ├─ Run PRAGMA integrity_check
  ├─ Count tables
  ├─ Test connections
  ├─ Check WAL files
  └─ Count records per table
Output: {overall_status, detailed_checks}
```

**8 Different Checks:**
1. File existence
2. File accessibility (read/write)
3. Database size
4. Database integrity
5. Table structures
6. Connection availability
7. WAL file status
8. Record counts per table

### 2. DatabaseBackupManager Class

```
Operations:
  ├─ Create backup
  │  ├─ Verify source integrity
  │  ├─ Copy with timestamp
  │  ├─ Calculate SHA256 hash
  │  └─ Clean old backups (keep 5)
  │
  ├─ Restore backup
  │  ├─ Verify backup integrity
  │  ├─ Backup current (corrupted) DB
  │  └─ Restore from backup
  │
  └─ Manage versions
     ├─ Sort by date
     └─ Keep last 5 only
```

**Features:**
- Automatic versioning
- SHA256 hash verification
- Retention policy (5 versions)
- Quick restoration

### 3. IOErrorRecovery Class

```
Responsibilities:
  ├─ Log errors with:
  │  ├─ Timestamp
  │  ├─ Error type
  │  ├─ Details
  │  └─ Context dict
  │
  ├─ Maintain error history:
  │  ├─ Persistent JSON file
  │  ├─ Last 100 errors
  │  └─ Queryable by timeframe
  │
  └─ Attempt recovery:
     ├─ Run health check
     ├─ If healthy → success
     └─ If unhealthy → log failure
```

**Error Logging:**
- File: `{db_path}.errors.json`
- Format: JSON array
- Max entries: 100 (circular)
- Timestamp: ISO 8601

### 4. ScrapeWithSafeguards Wrapper

```
5-Stage Protection:

STAGE 1: PRE-HEALTH CHECK
  ├─ Verify all databases
  ├─ Check accessibility
  └─ Abort if unhealthy

STAGE 2: CREATE BACKUP
  ├─ Backup tracker.db
  ├─ Verify backup
  └─ Report backup location

STAGE 3: EXECUTE SCRAPE
  ├─ Run: python run.py --source subito
  ├─ Capture output
  ├─ Handle timeout (1 hour max)
  └─ Report exit code

STAGE 4: POST-HEALTH CHECK
  ├─ Verify all databases again
  ├─ Check integrity
  └─ Compare with pre-scrape

STAGE 5: VERIFY CHANGES
  ├─ Check DB size changed
  ├─ Verify records added
  └─ Generate report
```

**Output:** `logs/scrape_safeguards_{source}_{timestamp}.json`

### 5. Cleanup & Optimize

```
Operations in sequence:

1. VACUUM DATABASES
   ├─ tracker.db
   ├─ subito.db
   ├─ trader.db
   └─ ebay.db
   (Reclaims unused space)

2. CLEANUP TEMPORARY FILES
   ├─ __pycache__/
   ├─ .pytest_cache/
   ├─ .ruff_cache/
   ├─ *.pyc files
   ├─ .fuse_hidden*
   └─ *.tmp files

3. ORGANIZE BACKUPS
   ├─ Inventory backup directory
   ├─ List by database
   ├─ Calculate total size
   └─ Report statistics

4. RESET WAL JOURNALS
   ├─ Check WAL files
   ├─ Monitor sizes
   └─ Identify unrecovered logs

5. UPDATE .gitignore
   ├─ Add backup patterns
   ├─ Add cache patterns
   ├─ Add database patterns
   └─ Prevent accidental commits
```

## Data Flow Diagram

```
USER
  │
  ├─ Calls: python scrape_with_safeguards.py --source subito
  │
  ▼
SAFEGUARD WRAPPER
  │
  ├─► Health Check (pre)    ──────────┐
  │                                   │
  │   ◀──────────────────────────────┘
  │   (if unhealthy: abort)
  │
  ├─► Create Backup        ──────────┐
  │                                  │
  │   ◀──────────────────────────────┘
  │   (backup created in backups/)
  │
  ├─► Run Scrape (run.py) ──────────┐
  │                                 │
  │   ◀─────────────────────────────┘
  │   (capture logs & output)
  │
  ├─► Health Check (post)  ──────────┐
  │                                   │
  │   ◀──────────────────────────────┘
  │   (compare with pre-check)
  │
  ├─► Verify Changes      ──────────┐
  │                                  │
  │   ◀──────────────────────────────┘
  │   (check DB size, records)
  │
  └─► Generate Report
      └─ JSON file to logs/
         └─ Return exit code
```

## File Organization

```
trader/
│
├── Core Database Modules
│   ├── db.py               (main database layer)
│   ├── db_subito.py        (subito-specific)
│   ├── db_ebay.py          (ebay-specific)
│   └── migrations.py       (schema migrations)
│
├── NEW: Safeguard Modules
│   ├── db_safeguards.py    ⭐ (health checks, backup, recovery)
│   ├── scrape_with_safeguards.py ⭐ (wrapper with 5-stage protection)
│   └── cleanup_and_optimize.py ⭐ (maintenance automation)
│
├── Documentation
│   ├── SAFEGUARDS_IMPLEMENTATION.md (technical guide)
│   ├── README_SAFEGUARDS.md (quick start)
│   ├── SYSTEM_ARCHITECTURE.md (this file)
│   └── README.md (original docs)
│
├── Data & Backups
│   ├── tracker.db          (main database - 45 MB)
│   ├── subito.db           (subito data - 0.9 MB)
│   ├── trader.db           (trader data - 0.5 MB)
│   ├── backups/            ⭐ (auto-managed)
│   │   ├── tracker_backup_*.db
│   │   ├── subito_backup_*.db
│   │   └── trader_backup_*.db
│   │
│   └── logs/               (execution logs)
│       ├── safeguards_*.log (detailed logs)
│       ├── scrape_safeguards_*.json (reports)
│       ├── cleanup_report_*.json (maintenance)
│       └── health_check_*.json (diagnostics)
│
└── Scraping Modules
    ├── run.py              (main scraper)
    ├── scrapers/           (source-specific)
    │   ├── subito.py
    │   └── ebay.py
    └── alerts.py           (alert system)
```

## Security & Recovery Strategy

```
TIER 1: PREVENTION
  ├─ Pre-scrape health checks (catch issues before they happen)
  ├─ Automatic backups (always have a safe copy)
  └─ Connection testing (verify before scraping)

TIER 2: DETECTION
  ├─ Post-scrape health checks (detect problems immediately)
  ├─ Error logging with context (understand what happened)
  └─ Record verification (confirm data integrity)

TIER 3: RECOVERY
  ├─ Automatic backup restoration (recover from data loss)
  ├─ Error history analysis (learn from failures)
  ├─ Connection retry logic (handle transient issues)
  └─ Detailed logging (debug if needed)

TIER 4: MONITORING
  ├─ Health check reports (regular status)
  ├─ Error trend analysis (spot patterns)
  ├─ Backup inventory (ensure coverage)
  └─ Performance metrics (track efficiency)
```

## Performance Characteristics

```
Operation           | Time  | Impact | Frequency
───────────────────────────────────────────────
Pre-health check    | ~1s   | Minimal| Per scrape
Create backup       | ~2s   | Minimal| Per scrape
Scrape execution    | ~10m  | Normal | Per scrape
Post-health check   | ~1s   | Minimal| Per scrape
Cleanup & optimize  | ~30s  | Minimal| Weekly
Health check report | ~2s   | Minimal| On-demand

Total overhead per scrape: ~4 seconds (0.7%)
```

## Integration Points

### With GitHub Actions
```yaml
# Update workflow to use:
run: python3.11 scrape_with_safeguards.py --source subito

# Provides:
# - Pre-scrape verification
# - Automatic backup
# - Post-scrape verification
# - Detailed JSON reports
# - Error tracking & recovery
```

### With Monitoring
```bash
# Daily health check (cron):
0 9 * * * cd ~/trader && python db_safeguards.py > logs/daily_health.log

# Weekly cleanup:
0 10 * * 0 cd ~/trader && python cleanup_and_optimize.py
```

### With Alerting
```python
# Can integrate with Telegram/Slack:
from db_safeguards import create_diagnostic_report

report = create_diagnostic_report()
if report['databases']['tracker.db']['overall_status'] != 'healthy':
    send_alert("Database health issue detected")
```

---

**Last Updated:** 2026-04-07
**Version:** 1.0 - Production Ready
