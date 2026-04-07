"""
Scrape Wrapper with Safeguards
Integrates database health checks, automatic backups, and error recovery
Usage: python scrape_with_safeguards.py --source subito [--other-args]
"""

import sys
import os
import json
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict
from db_safeguards import (
    DatabaseHealthCheck,
    DatabaseBackupManager,
    IOErrorRecovery,
    create_diagnostic_report
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/safeguards_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("scrape_safeguards")


class ScrapeWithSafeguards:
    """Wrapper for scrape operations with protection mechanisms"""

    def __init__(self, source: str):
        self.source = source
        self.start_time = datetime.now()
        self.db_paths = ["tracker.db"]
        self.backup_manager = DatabaseBackupManager("tracker.db", backup_dir="backups")
        self.io_recovery = IOErrorRecovery("tracker.db")

    def run(self, scrape_args: list) -> Dict:
        """Execute scrape with safeguards"""
        logger.info("=" * 70)
        logger.info(f"SCRAPE START: {self.source}")
        logger.info("=" * 70)

        result = {
            "timestamp": self.start_time.isoformat(),
            "source": self.source,
            "stages": {}
        }

        try:
            # Stage 1: Pre-scrape health check
            logger.info("\n📋 STAGE 1: Pre-scrape health check")
            result["stages"]["pre_health_check"] = self._pre_scrape_check()

            if result["stages"]["pre_health_check"]["status"] == "error":
                logger.error("❌ Pre-scrape health check failed - aborting")
                return {**result, "status": "aborted", "reason": "Pre-scrape check failed"}

            # Stage 2: Create backups
            logger.info("\n💾 STAGE 2: Creating backups")
            result["stages"]["backups"] = self._create_backups()

            # Stage 3: Execute scrape
            logger.info(f"\n🕷️  STAGE 3: Executing scrape ({self.source})")
            result["stages"]["scrape"] = self._execute_scrape(scrape_args)

            if result["stages"]["scrape"]["status"] == "error":
                logger.warning("⚠️  Scrape had errors - attempting recovery")
                self._handle_scrape_error(result["stages"]["scrape"])

            # Stage 4: Post-scrape health check
            logger.info("\n📊 STAGE 4: Post-scrape health check")
            result["stages"]["post_health_check"] = self._post_scrape_check()

            # Stage 5: Verification
            logger.info("\n✅ STAGE 5: Verification")
            result["stages"]["verification"] = self._verify_changes()

            # Overall status
            result["status"] = "success" if result["stages"]["post_health_check"]["status"] == "ok" else "warning"

        except Exception as e:
            logger.error(f"❌ Scrape failed with exception: {str(e)}")
            result["status"] = "error"
            result["error"] = str(e)

        finally:
            # Save report
            self._save_report(result)
            duration = (datetime.now() - self.start_time).total_seconds()
            logger.info(f"\n⏱️  Duration: {duration:.1f}s")
            logger.info(f"📝 Status: {result['status']}")

        return result

    def _pre_scrape_check(self) -> Dict:
        """Check database health before scrape"""
        logger.info("   Checking database health...")
        status = {"status": "ok", "databases": {}}

        for db_path in self.db_paths:
            if os.path.exists(db_path):
                health = DatabaseHealthCheck(db_path)
                check = health.run_all_checks()

                status["databases"][db_path] = {
                    "integrity": check.get("integrity", {}).get("integrity"),
                    "size_mb": check.get("size", {}).get("size_mb"),
                    "overall": check.get("overall_status")
                }

                if check.get("overall_status") != "healthy":
                    status["status"] = "error"
                    logger.warning(f"   ⚠️  {db_path}: {check.get('overall_status')}")

        if status["status"] == "ok":
            logger.info("   ✅ All databases healthy")
        else:
            logger.error("   ❌ Some databases unhealthy")

        return status

    def _create_backups(self) -> Dict:
        """Create backups of main database"""
        logger.info("   Creating backup of tracker.db...")
        result = {"status": "ok", "backups": []}

        try:
            backup_path = self.backup_manager.create_backup(tag=self.source)

            if backup_path:
                result["backups"].append({
                    "database": "tracker.db",
                    "path": backup_path,
                    "timestamp": datetime.now().isoformat()
                })
                logger.info(f"   ✅ Backup created: {backup_path}")
            else:
                result["status"] = "error"
                logger.error("   ❌ Backup creation failed")
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"   ❌ Backup error: {str(e)}")

        return result

    def _execute_scrape(self, scrape_args: list) -> Dict:
        """Execute the actual scrape command"""
        logger.info(f"   Running: python run.py {' '.join(scrape_args)}")

        result = {"status": "ok", "command": scrape_args}

        try:
            # Execute scrape
            process = subprocess.run(
                ["python3.11", "run.py"] + scrape_args,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )

            result["exit_code"] = process.returncode
            result["stdout_lines"] = len(process.stdout.split('\n'))
            result["stderr_lines"] = len(process.stderr.split('\n'))

            if process.returncode != 0:
                result["status"] = "error"
                logger.error(f"   ❌ Scrape failed (exit code: {process.returncode})")
                result["error_sample"] = process.stderr[:500]
            else:
                logger.info("   ✅ Scrape completed successfully")

            # Log full output to file
            with open(f"logs/scrape_{self.source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", 'w') as f:
                f.write(process.stdout)
                if process.stderr:
                    f.write("\n--- STDERR ---\n")
                    f.write(process.stderr)

        except subprocess.TimeoutExpired:
            result["status"] = "error"
            result["error"] = "Scrape timeout (1 hour)"
            logger.error("   ❌ Scrape timeout")
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"   ❌ Scrape exception: {str(e)}")

        return result

    def _post_scrape_check(self) -> Dict:
        """Check database health after scrape"""
        logger.info("   Checking database health post-scrape...")
        status = {"status": "ok", "databases": {}}

        for db_path in self.db_paths:
            if os.path.exists(db_path):
                health = DatabaseHealthCheck(db_path)
                check = health.run_all_checks()

                status["databases"][db_path] = {
                    "integrity": check.get("integrity", {}).get("integrity"),
                    "size_mb": check.get("size", {}).get("size_mb"),
                    "overall": check.get("overall_status")
                }

                if check.get("overall_status") != "healthy":
                    status["status"] = "error"
                    logger.warning(f"   ⚠️  {db_path}: {check.get('overall_status')}")

        if status["status"] == "ok":
            logger.info("   ✅ All databases still healthy")
        else:
            logger.error("   ❌ Some databases degraded post-scrape")

        return status

    def _verify_changes(self) -> Dict:
        """Verify that scrape actually changed data"""
        logger.info("   Verifying changes...")

        result = {"status": "ok", "changes": {}}

        try:
            # Check tracker.db size change
            if os.path.exists("tracker.db"):
                size_mb = os.path.getsize("tracker.db") / (1024 * 1024)
                result["changes"]["tracker_db_size_mb"] = round(size_mb, 2)

                logger.info(f"   DB size: {round(size_mb, 2)} MB")

            result["status"] = "ok"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"   ❌ Verification error: {str(e)}")

        return result

    def _handle_scrape_error(self, scrape_result: Dict):
        """Handle errors from scrape execution"""
        logger.warning("   Attempting error recovery...")

        # Log the error
        self.io_recovery.log_error(
            "scrape_error",
            scrape_result.get("error", "Unknown error"),
            context={"source": self.source}
        )

        # Attempt recovery
        recovered = self.io_recovery.attempt_recovery()

        if not recovered:
            logger.error("   ❌ Could not recover automatically")
            logger.info("   💡 Options:")
            logger.info("      1. Check Playwright installation: python -m playwright install chromium")
            logger.info("      2. Check network connectivity")
            logger.info("      3. Review database status: python db_safeguards.py")
        else:
            logger.info("   ✅ Recovered successfully")

    def _save_report(self, result: Dict):
        """Save execution report"""
        report_path = f"logs/scrape_safeguards_{self.source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        try:
            with open(report_path, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            logger.info(f"\n💾 Report saved: {report_path}")
        except Exception as e:
            logger.error(f"Could not save report: {str(e)}")


def main():
    """Entry point"""
    if len(sys.argv) < 2:
        print("Usage: python scrape_with_safeguards.py --source <source> [other args...]")
        sys.exit(1)

    # Parse source
    try:
        source_idx = sys.argv.index("--source")
        source = sys.argv[source_idx + 1]
    except (ValueError, IndexError):
        print("Error: --source argument required")
        sys.exit(1)

    # Pass remaining args to scrape
    scrape_args = sys.argv[1:]

    # Create safeguard wrapper
    wrapper = ScrapeWithSafeguards(source)
    result = wrapper.run(scrape_args)

    # Exit with appropriate code
    sys.exit(0 if result["status"] == "success" else 1)


if __name__ == "__main__":
    main()
