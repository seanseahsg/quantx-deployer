"""QuantX Orchestrator — Railway service that manages student trading bots.

Runs as a separate Railway service in the honest-reverence project.
Shares the web-volume (/data) with the main web service.

How it works:
  1. Scans /data/bots/{email}/*.json every SCAN_INTERVAL seconds
  2. Starts bots where enabled=true and status != "running"
  3. Restarts crashed bots (process no longer alive)
  4. Stops bots where enabled=false
  5. Updates each .json with pid / status / last_heartbeat

Start command (Railway):  python orchestrator/main.py
Environment variables:
  DATA_DIR         = /data          (Railway volume mount)
  CENTRAL_API_URL  = https://quantx-deploy.up.railway.app
  ORCHESTRATOR_KEY = quantx-orch-2025
  LONGBRIDGE_LOG_PATH = /data/logs/longbridge
"""

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR        = Path(os.environ.get("DATA_DIR", "/data"))
BOTS_DIR        = DATA_DIR / "bots"
ORCH_LOG_DIR    = DATA_DIR / "orchestrator"
CENTRAL_API_URL = os.environ.get("CENTRAL_API_URL", "")
ORCH_KEY        = os.environ.get("ORCHESTRATOR_KEY", "quantx-orch-2025")
SCAN_INTERVAL   = int(os.environ.get("SCAN_INTERVAL", 30))   # seconds
PYTHON_EXE      = sys.executable

# Ensure directories exist
BOTS_DIR.mkdir(parents=True, exist_ok=True)
ORCH_LOG_DIR.mkdir(parents=True, exist_ok=True)
Path(os.environ.get("LONGBRIDGE_LOG_PATH", str(DATA_DIR / "logs/longbridge"))).mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(str(ORCH_LOG_DIR / "orchestrator.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("quantx-orchestrator")

# ── In-memory process registry ────────────────────────────────────────────────
# Maps strategy_id -> subprocess.Popen
_procs: dict[str, subprocess.Popen] = {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_meta(meta_path: Path) -> dict | None:
    """Read and parse a bot metadata JSON. Returns None on any error."""
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("[META] Failed to read %s: %s", meta_path, e)
        return None


def _write_meta(meta_path: Path, meta: dict) -> None:
    """Write metadata JSON atomically (write to .tmp then rename)."""
    tmp = meta_path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        tmp.replace(meta_path)
    except Exception as e:
        log.warning("[META] Failed to write %s: %s", meta_path, e)


def _is_alive(proc: subprocess.Popen) -> bool:
    return proc is not None and proc.poll() is None


def _start_bot(meta: dict, meta_path: Path) -> subprocess.Popen | None:
    """Launch the bot script as a subprocess. Returns the Popen object or None."""
    script_path = meta.get("script_path", "")
    if not script_path or not Path(script_path).exists():
        log.error("[START] Script not found: %s (strategy_id=%s)", script_path, meta.get("strategy_id"))
        meta["status"] = "error"
        meta["error"]  = f"Script not found: {script_path}"
        _write_meta(meta_path, meta)
        return None

    # Each bot writes to its own log file alongside the script
    log_path = Path(script_path).with_suffix(".log")

    try:
        log_fh = open(str(log_path), "a", encoding="utf-8")
        proc = subprocess.Popen(
            [PYTHON_EXE, script_path],
            stdout=log_fh,
            stderr=log_fh,
            cwd=str(Path(script_path).parent),
            start_new_session=True,   # detach from orchestrator's signal group
        )
        log.info(
            "[START] %s | pid=%d | %s",
            meta.get("strategy_id"), proc.pid, Path(script_path).name,
        )
        meta["status"]         = "running"
        meta["pid"]            = proc.pid
        meta["started_at"]     = _now_iso()
        meta["last_heartbeat"] = _now_iso()
        meta.pop("error", None)
        _write_meta(meta_path, meta)
        return proc

    except Exception as e:
        log.error("[START] Failed to launch %s: %s", meta.get("strategy_id"), e)
        meta["status"] = "error"
        meta["error"]  = str(e)
        _write_meta(meta_path, meta)
        return None


def _stop_bot(strategy_id: str, meta: dict, meta_path: Path) -> None:
    """Terminate a running bot process."""
    proc = _procs.pop(strategy_id, None)
    if proc and _is_alive(proc):
        try:
            proc.terminate()
            proc.wait(timeout=10)
            log.info("[STOP] %s | pid=%d", strategy_id, proc.pid)
        except Exception as e:
            log.warning("[STOP] %s terminate failed: %s — killing", strategy_id, e)
            try:
                proc.kill()
            except Exception:
                pass
    meta["status"] = "stopped"
    meta["pid"]    = None
    _write_meta(meta_path, meta)


def _report_to_central(running_count: int, total_count: int) -> None:
    """POST a heartbeat to the main web service so it knows orchestrator is alive."""
    if not CENTRAL_API_URL:
        return
    try:
        import urllib.request
        payload = json.dumps({
            "key":           ORCH_KEY,
            "running_bots":  running_count,
            "total_bots":    total_count,
            "timestamp":     _now_iso(),
        }).encode()
        req = urllib.request.Request(
            f"{CENTRAL_API_URL}/api/orchestrator/heartbeat",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass   # Central might be redeploying; don't crash orchestrator


# ── Main scan loop ────────────────────────────────────────────────────────────

def scan_once() -> tuple[int, int]:
    """Single scan pass. Returns (running_count, total_count)."""
    running_count = 0
    total_count   = 0

    if not BOTS_DIR.exists():
        return 0, 0

    # Walk every student subdirectory
    for student_dir in sorted(BOTS_DIR.iterdir()):
        if not student_dir.is_dir():
            continue

        for meta_path in sorted(student_dir.glob("*.json")):
            meta = _read_meta(meta_path)
            if meta is None:
                continue

            strategy_id = meta.get("strategy_id", meta_path.stem)
            enabled     = meta.get("enabled", False)
            total_count += 1

            proc = _procs.get(strategy_id)
            alive = _is_alive(proc)

            if not enabled:
                # ── Should be stopped ────────────────────────────────────────
                if alive:
                    log.info("[SCAN] Stopping disabled bot: %s", strategy_id)
                    _stop_bot(strategy_id, meta, meta_path)
                elif meta.get("status") == "running":
                    # Process gone but meta still says running — fix status
                    meta["status"] = "stopped"
                    meta["pid"]    = None
                    _write_meta(meta_path, meta)

            else:
                # ── Should be running ────────────────────────────────────────
                if alive:
                    # All good — update heartbeat timestamp
                    running_count += 1
                    meta["last_heartbeat"] = _now_iso()
                    meta["status"]         = "running"
                    meta["pid"]            = proc.pid
                    _write_meta(meta_path, meta)

                else:
                    # Not running — start it (fresh start or restart after crash)
                    if meta.get("status") == "running":
                        exit_code = proc.poll() if proc else "never started"
                        log.warning(
                            "[SCAN] Bot crashed, restarting: %s (exit=%s)",
                            strategy_id, exit_code,
                        )
                    else:
                        log.info("[SCAN] Starting new bot: %s", strategy_id)

                    new_proc = _start_bot(meta, meta_path)
                    if new_proc:
                        _procs[strategy_id] = new_proc
                        running_count += 1

    return running_count, total_count


def main() -> None:
    log.info("=" * 60)
    log.info("QuantX Orchestrator starting")
    log.info("DATA_DIR      = %s", DATA_DIR)
    log.info("BOTS_DIR      = %s", BOTS_DIR)
    log.info("CENTRAL_URL   = %s", CENTRAL_API_URL or "(not set)")
    log.info("SCAN_INTERVAL = %ds", SCAN_INTERVAL)
    log.info("PYTHON        = %s", PYTHON_EXE)
    log.info("=" * 60)

    heartbeat_counter = 0

    while True:
        try:
            running, total = scan_once()
            log.info("[SCAN] %d/%d bots running", running, total)

            # Report to central every ~5 minutes (every 10 scans at 30s interval)
            heartbeat_counter += 1
            if heartbeat_counter >= 10:
                _report_to_central(running, total)
                heartbeat_counter = 0

        except Exception as e:
            log.exception("[SCAN] Unexpected error in scan loop: %s", e)

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
