"""
Database Cleanup & Optimization
- VACUUM databases
- Clean up temporary files
- Organize backups
- Reset WAL journals
"""

import sqlite3
import os
import shutil
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("cleanup")


def vacuum_database(db_path: str) -> bool:
    """Execute VACUUM on database"""
    if not os.path.exists(db_path):
        logger.warning(f"Database not found: {db_path}")
        return False

    try:
        logger.info(f"Vacuuming {db_path}...")

        size_before = os.path.getsize(db_path) / (1024 * 1024)
        logger.info(f"   Size before: {size_before:.2f} MB")

        conn = sqlite3.connect(db_path)
        conn.execute("VACUUM")
        conn.close()

        size_after = os.path.getsize(db_path) / (1024 * 1024)
        logger.info(f"   Size after:  {size_after:.2f} MB")
        logger.info(f"   Saved:       {size_before - size_after:.2f} MB")

        return True
    except Exception as e:
        logger.error(f"VACUUM failed: {str(e)}")
        return False


def cleanup_temporary_files() -> Dict:
    """Clean up temporary and cache files"""
    logger.info("\nCleaning temporary files...")

    patterns = [
        ("__pycache__", "directory"),
        ("*.pyc", "file"),
        (".pytest_cache", "directory"),
        (".ruff_cache", "directory"),
        (".fuse_hidden*", "file"),
        ("*.tmp", "file"),
        ("/tmp/*trader*", "file"),
        ("/tmp/*subito*", "file"),
    ]

    result = {"cleaned": {}, "errors": []}

    for pattern, file_type in patterns:
        try:
            if "*" in pattern:
                # Use glob pattern
                for path in Path(".").glob(pattern.replace(".", "")):
                    if file_type == "directory":
                        shutil.rmtree(path, ignore_errors=True)
                        result["cleaned"][str(path)] = "directory"
                        logger.info(f"   ✅ Removed: {path}")
                    elif file_type == "file":
                        try:
                            os.remove(path)
                            result["cleaned"][str(path)] = "file"
                        except PermissionError:
                            result["errors"].append(f"Permission denied: {path}")
            else:
                # Direct path
                if os.path.isdir(pattern):
                    shutil.rmtree(pattern, ignore_errors=True)
                    result["cleaned"][pattern] = "directory"
                    logger.info(f"   ✅ Removed: {pattern}")
                elif os.path.isfile(pattern):
                    os.remove(pattern)
                    result["cleaned"][pattern] = "file"
                    logger.info(f"   ✅ Removed: {pattern}")
        except Exception as e:
            result["errors"].append(f"{pattern}: {str(e)}")

    logger.info(f"   Total cleaned: {len(result['cleaned'])} items")
    if result["errors"]:
        logger.warning(f"   Errors: {len(result['errors'])}")

    return result


def organize_backups(backup_dir: str = "backups") -> Dict:
    """Organize and inventory backups"""
    logger.info(f"\nOrganizing backups ({backup_dir})...")

    Path(backup_dir).mkdir(exist_ok=True)

    result = {
        "total_backups": 0,
        "total_size_mb": 0,
        "by_database": {}
    }

    for file in sorted(os.listdir(backup_dir)):
        file_path = os.path.join(backup_dir, file)

        if os.path.isfile(file_path):
            size_mb = os.path.getsize(file_path) / (1024 * 1024)

            # Extract database name
            db_name = file.split("_backup_")[0] + ".db"

            if db_name not in result["by_database"]:
                result["by_database"][db_name] = {
                    "backups": [],
                    "total_size_mb": 0
                }

            result["by_database"][db_name]["backups"].append({
                "file": file,
                "size_mb": round(size_mb, 2)
            })
            result["by_database"][db_name]["total_size_mb"] += size_mb

            result["total_backups"] += 1
            result["total_size_mb"] += size_mb

    # Log summary
    logger.info(f"   Total backups: {result['total_backups']}")
    logger.info(f"   Total size: {result['total_size_mb']:.2f} MB")

    for db_name, info in result["by_database"].items():
        logger.info(f"   {db_name}: {len(info['backups'])} backups ({info['total_size_mb']:.2f} MB)")

    return result


def reset_wal_journals() -> Dict:
    """Reset WAL (Write-Ahead Log) journals"""
    logger.info("\nResetting WAL journals...")

    databases = ["tracker.db", "subito.db", "trader.db", "ebay.db"]
    result = {"reset": {}, "errors": []}

    for db in databases:
        if not os.path.exists(db):
            continue

        try:
            # Try to open and close cleanly
            conn = sqlite3.connect(db, timeout=5)
            conn.close()

            # Check for WAL files
            for ext in ["-shm", "-wal"]:
                wal_path = f"{db}{ext}"
                if os.path.exists(wal_path):
                    size = os.path.getsize(wal_path)
                    if size == 0:
                        result["reset"][wal_path] = "empty"
                    else:
                        result["reset"][wal_path] = f"{size} bytes"
                        logger.info(f"   {wal_path}: {size} bytes (will be auto-recovered)")

        except Exception as e:
            result["errors"].append(f"{db}: {str(e)}")
            logger.error(f"   ⚠️  {db}: {str(e)}")

    if result["reset"]:
        logger.info(f"   ✅ Checked {len(result['reset'])} WAL files")
    if result["errors"]:
        logger.warning(f"   ⚠️  {len(result['errors'])} errors")

    return result


def create_gitignore_update() -> str:
    """Create entries for .gitignore to prevent committing backup files"""
    logger.info("\nUpdating .gitignore...")

    entries = [
        "# Database backups and temporary files",
        "backups/",
        "*.db-shm",
        "*.db-wal",
        "*.db-journal",
        "__pycache__/",
        "*.pyc",
        ".pytest_cache/",
        ".ruff_cache/",
        ".fuse_hidden*",
        "*.tmp",
        ".DS_Store",
        "logs/*.log",
        "logs/*_corrupted*",
        "*.corrupted_*",
    ]

    gitignore_path = ".gitignore"
    existing = set()

    if os.path.exists(gitignore_path):
        with open(gitignore_path, 'r') as f:
            existing = set(line.strip() for line in f if line.strip())

    new_entries = [e for e in entries if e not in existing]

    if new_entries:
        with open(gitignore_path, 'a') as f:
            f.write("\n" + "\n".join(new_entries) + "\n")
        logger.info(f"   ✅ Added {len(new_entries)} entries to .gitignore")
    else:
        logger.info("   ℹ️  .gitignore already up to date")

    return gitignore_path


def main():
    """Run all cleanup operations"""
    logger.info("=" * 70)
    logger.info("DATABASE CLEANUP & OPTIMIZATION")
    logger.info("=" * 70)

    report = {
        "timestamp": datetime.now().isoformat(),
        "operations": {}
    }

    # 1. VACUUM databases
    logger.info("\n[1/5] VACUUM Databases")
    logger.info("-" * 70)
    for db in ["tracker.db", "subito.db", "trader.db", "ebay.db"]:
        if os.path.exists(db):
            vacuum_database(db)

    # 2. Cleanup temporary files
    logger.info("\n[2/5] Cleanup Temporary Files")
    logger.info("-" * 70)
    report["operations"]["cleanup"] = cleanup_temporary_files()

    # 3. Organize backups
    logger.info("\n[3/5] Organize Backups")
    logger.info("-" * 70)
    report["operations"]["backups"] = organize_backups()

    # 4. Reset WAL journals
    logger.info("\n[4/5] Reset WAL Journals")
    logger.info("-" * 70)
    report["operations"]["wal"] = reset_wal_journals()

    # 5. Update .gitignore
    logger.info("\n[5/5] Update .gitignore")
    logger.info("-" * 70)
    create_gitignore_update()

    # Save report
    logger.info("\n" + "=" * 70)
    report_path = f"logs/cleanup_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("✅ CLEANUP COMPLETED")
    logger.info(f"📝 Report saved: {report_path}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
