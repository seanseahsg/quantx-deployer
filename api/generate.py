"""QuantX Deployer — Unified master bot script generator."""

import json
from pathlib import Path

from .bot_template import BOT_TEMPLATE
from .config import BOTS_DIR, LOGS_DIR


def generate_master_bot(email: str, strategies: list[dict], credentials: dict) -> str:
    """Generate a single master bot script containing ALL strategies.

    Handles standard indicators, custom scripts, AND SYMMETRIC_GRID
    strategies in one unified process.

    Returns:
        Absolute path to the generated master script.
    """
    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    email_safe = email.replace("@", "_at_").replace(".", "_")
    script_path = BOTS_DIR / f"{email_safe}_master.py"
    master_log_name = f"{email_safe}_master.log"

    # Convert strategies to Python-safe format
    strategies_json = json.dumps(strategies, indent=4)
    strategies_json = strategies_json.replace(": true", ": True")
    strategies_json = strategies_json.replace(": false", ": False")
    strategies_json = strategies_json.replace(": null", ": None")

    content = BOT_TEMPLATE.format(
        email=email,
        student_name=credentials.get("name", ""),
        central_api_url=credentials.get("central_api_url", ""),
        app_key=credentials.get("app_key", ""),
        app_secret=credentials.get("app_secret", ""),
        access_token=credentials.get("access_token", ""),
        strategies_json=strategies_json,
        log_dir=str(LOGS_DIR).replace("\\", "/"),
        master_log_name=master_log_name,
    )

    script_path.write_text(content, encoding="utf-8")
    return str(script_path.resolve())
