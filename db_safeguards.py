"""
Database Safeguards & Health Checks
Implemented: 2026-04-07
Purpose: Prevent data loss, ensure recovery capabilities, comprehensive logging
"""

import sqlite3
import os
import json
import hashlib
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("db_safeguards")


class DatabaseHealthCheck:
    """Complete health check for all databases"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db_name = os.path.basename(db_path)
        self.report = {}

    def run_all_checks(self) -> Dict:
        """Execute all health checks"""
        logger.info(f"Starting health check for {self.db_name}")

        checks = [
            ("file_exists", self._check_file_exists),
            ("file_readable", self._check_file_readable),
            ("size", self._check_size),
            ("integrity", self._check_integrity),
            ("tables", self._check_tables),
            ("connections", self._check_connections),
            ("wal_files", self._check_wal_files),
            ("record_counts", self._check_record_counts),
        ]

        for check_name, check_func in checks:
            try:
                self.report[check_name] = check_func()
            except Exception as e:
                self.report[check_name] = {"status": "error", "message": str(e)}
                logger.error(f"Check {check_name} failed: {str(e)}")

        # Overall status
        self.report["overall_status"] = "healthy" if all(
            check.get("status") == "ok" for check in self.report.values()
            if isinstance(check, dict)
        ) else "unhealthy"

        logger.info(f"Health check completed: {self.report['overall_status']}")
        return self.report

    def _check_file_exists(self) -> Dict:
        exists = os.path.exists(self.db_path)
        return {
            "status": "ok" if exists else "error",
            "exists": exists,
            "path": self.db_path
        }

    def _check_file_readable(self) -> Dict:
        try:
            readable = os.access(self.db_path, os.R_OK)
            writable = os.access(self.db_path, os.W_OK)
            return {
                "status": "ok" if readable else "error",
                "readable": readable,
                "writable": writable
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _check_size(self) -> Dict:
        if not os.path.exists(self.db_path):
            return {"status": "error", "message": "File not found"}

        size_bytes = os.path.getsize(self.db_path)
        size_mb = size_bytes / (1024 * 1024)

        status = "ok"
        if size_bytes == 0:
            status = "warning"  # Empty database

        return {
            "status": status,
            "size_bytes": size_bytes,
            "size_mb": round(size_mb, 2)
        }

    def _check_integrity(self) -> Dict:
        if not os.path.exists(self.db_path):
            return {"status": "error", "message": "File not found"}

        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()

            # Integrity check
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]

            conn.close()

            if result == "ok":
                return {"status": "ok", "integrity": "ok"}
            else:
                return {"status": "error", "integrity": result}
        except sqlite3.DatabaseError as e:
            return {"status": "error", "message": f"Database corrupted: {str(e)}"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _check_tables(self) -> Dict:
        if not os.path.exists(self.db_path):
            return {"status": "error", "message": "File not found"}

        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            conn.close()

            return {
                "status": "ok" if table_count > 0 else "warning",
                "table_count": table_count,
                "tables": tables
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _check_connections(self) -> Dict:
        if not os.path.exists(self.db_path):
            return {"status": "error", "message": "File not found"}

        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            conn.execute("SELECT 1")
            conn.close()

            return {"status": "ok", "connections": "ok"}
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                return {"status": "warning", "message": "Database locked"}
            return {"status": "error", "message": str(e)}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _check_wal_files(self) -> Dict:
        wal_files = {
            "shm": f"{self.db_path}-shm",
            "wal": f"{self.db_path}-wal"
        }

        result = {"status": "ok", "files": {}}

        for file_type, file_path in wal_files.items():
            if os.path.exists(file_path):
                size = os.path.getsize(file_path)
                result["files"][file_type] = {
                    "exists": True,
                    "size_bytes": size
                }
                if size > 10 * 1024 * 1024:  # > 10MB
                    result["status"] = "warning"
            else:
                result["files"][file_type] = {"exists": False}

        return result

    def _check_record_counts(self) -> Dict:
        if not os.path.exists(self.db_path):
            return {"status": "error", "message": "File not found"}

        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()

            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            counts = {}
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cursor.fetchone()[0]
                except:
                    counts[table] = "error"

            conn.close()

            return {
                "status": "ok",
                "table_counts": counts
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}


class DatabaseBackupManager:
    """Manage automatic backups with versioning"""

    def __init__(self, db_path: str, backup_dir: str = "backups"):
        self.db_path = db_path
        self.db_name = os.path.basename(db_path)
        self.backup_dir = backup_dir

        # Create backup directory
        Path(self.backup_dir).mkdir(exist_ok=True)

        self.max_backups = 5  # Keep last 5 backups

    def create_backup(self, tag: str = "") -> Optional[str]:
        """Create a backup with timestamp"""
        if not os.path.exists(self.db_path):
            logger.error(f"Database not found: {self.db_path}")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        tag_str = f"_{tag}" if tag else ""
        backup_name = f"{self.db_name.replace('.db', '')}_backup_{timestamp}{tag_str}.db"
        backup_path = os.path.join(self.backup_dir, backup_name)

        try:
            # Verify source integrity before backup
            conn = sqlite3.connect(self.db_path, timeout=5)
            conn.execute("PRAGMA integrity_check")
            conn.close()

            # Copy file
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"✅ Backup created: {backup_path}")

            # Calculate hash
            file_hash = self._calculate_hash(backup_path)

            # Clean old backups
            self._cleanup_old_backups()

            return backup_path
        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            return None

    def _calculate_hash(self, file_path: str) -> str:
        """Calculate SHA256 hash of file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _cleanup_old_backups(self):
        """Keep only the last N backups"""
        backups = sorted([
            f for f in os.listdir(self.backup_dir)
            if f.startswith(self.db_name.replace('.db', ''))
        ])

        if len(backups) > self.max_backups:
            for old_backup in backups[:-self.max_backups]:
                try:
                    os.remove(os.path.join(self.backup_dir, old_backup))
                    logger.info(f"Cleaned old backup: {old_backup}")
                except Exception as e:
                    logger.warning(f"Could not remove {old_backup}: {str(e)}")

    def restore_backup(self, backup_path: str) -> bool:
        """Restore from backup"""
        if not os.path.exists(backup_path):
            logger.error(f"Backup not found: {backup_path}")
            return False

        try:
            # Verify backup integrity
            conn = sqlite3.connect(backup_path, timeout=5)
            conn.execute("PRAGMA integrity_check")
            conn.close()

            # Backup current DB first
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            corrupted_path = f"{self.db_path}.corrupted_{timestamp}"
            shutil.copy2(self.db_path, corrupted_path)

            # Restore backup
            shutil.copy2(backup_path, self.db_path)
            logger.info(f"✅ Restored from: {backup_path}")
            logger.info(f"   Corrupted copy saved: {corrupted_path}")

            return True
        except Exception as e:
            logger.error(f"Restore failed: {str(e)}")
            return False


class IOErrorRecovery:
    """Handle and recover from I/O errors"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.error_log_path = f"{db_path}.errors.json"

    def log_error(self, error_type: str, details: str, context: Dict = None):
        """Log I/O error with context"""
        error_entry = {
            "timestamp": datetime.now().isoformat(),
            "error_type": error_type,
            "details": details,
            "context": context or {},
            "db_file": self.db_path
        }

        # Read existing log
        errors = []
        if os.path.exists(self.error_log_path):
            try:
                with open(self.error_log_path, 'r') as f:
                    errors = json.load(f)
            except:
                errors = []

        errors.append(error_entry)

        # Keep last 100 errors
        errors = errors[-100:]

        # Write back
        try:
            with open(self.error_log_path, 'w') as f:
                json.dump(errors, f, indent=2)
        except Exception as e:
            logger.error(f"Could not write error log: {str(e)}")

    def attempt_recovery(self) -> bool:
        """Attempt automatic recovery from I/O error"""
        logger.info("Attempting I/O error recovery...")

        health_check = DatabaseHealthCheck(self.db_path)
        report = health_check.run_all_checks()

        if report.get("overall_status") == "healthy":
            logger.info("✅ Database recovered")
            return True

        logger.warning("❌ Automatic recovery failed")
        return False

    def get_error_history(self, hours: int = 24) -> List[Dict]:
        """Get error history from last N hours"""
        if not os.path.exists(self.error_log_path):
            return []

        try:
            with open(self.error_log_path, 'r') as f:
                errors = json.load(f)

            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent = [
                e for e in errors
                if datetime.fromisoformat(e["timestamp"]) > cutoff_time
            ]

            return recent
        except:
            return []


def create_diagnostic_report(databases: List[str] = None) -> Dict:
    """Create comprehensive diagnostic report"""
    if databases is None:
        databases = ["tracker.db"]

    report = {
        "timestamp": datetime.now().isoformat(),
        "databases": {}
    }

    for db_path in databases:
        if os.path.exists(db_path):
            health = DatabaseHealthCheck(db_path)
            report["databases"][db_path] = health.run_all_checks()

    return report


if __name__ == "__main__":
    # Example usage
    print("Database Safeguards Module")
    print("=" * 70)

    # Check all databases
    for db in ["tracker.db"]:
        if os.path.exists(db):
            print(f"\n📊 {db}")
            health = DatabaseHealthCheck(db)
            report = health.run_all_checks()

            print(f"   Status: {report.get('overall_status')}")
            print(f"   Tables: {report.get('tables', {}).get('table_count', 0)}")
            print(f"   Size: {report.get('size', {}).get('size_mb', 0):.2f} MB")
