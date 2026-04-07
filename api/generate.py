"""QuantX Deployer — Unified master bot script generator."""

import json
from pathlib import Path

from .bot_template import BOT_TEMPLATE
from .bot_template_ibkr import IBKR_BOT_TEMPLATE
from .bot_template_simple import SIMPLE_LP_TEMPLATE, SIMPLE_IBKR_TEMPLATE
from .config import BOTS_DIR, LOGS_DIR


def generate_master_bot(email: str, strategies: list[dict], credentials: dict) -> str:
    """Generate a single master bot script containing ALL strategies."""
    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    email_safe = email.replace("@", "_at_").replace(".", "_")
    script_path = BOTS_DIR / f"{email_safe}_master.py"
    master_log_name = f"{email_safe}_master.log"

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


def generate_ibkr_bot(email: str, strategies: list[dict], credentials: dict,
                      ibkr_config: dict) -> str:
    """Generate an IBKR master bot script."""
    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    email_safe = email.replace("@", "_at_").replace(".", "_")
    script_path = BOTS_DIR / f"{email_safe}_ibkr_master.py"
    master_log_name = f"{email_safe}_ibkr_master.log"

    strategies_json = json.dumps(strategies, indent=4)
    strategies_json = strategies_json.replace(": true", ": True")
    strategies_json = strategies_json.replace(": false", ": False")
    strategies_json = strategies_json.replace(": null", ": None")

    content = IBKR_BOT_TEMPLATE.format(
        email=email,
        student_name=credentials.get("name", ""),
        central_api_url=credentials.get("central_api_url", ""),
        ibkr_host=ibkr_config.get("host", "127.0.0.1"),
        ibkr_port=ibkr_config.get("port", 7497),
        ibkr_client_id=ibkr_config.get("client_id", 1),
        strategies_json=strategies_json,
        log_dir=str(LOGS_DIR).replace("\\", "/"),
        master_log_name=master_log_name,
    )

    script_path.write_text(content, encoding="utf-8")
    return str(script_path.resolve())


# ── Simple bot generators (for test orders) ─────────────────────────────────

def generate_simple_lp_bot(email: str, symbol: str, credentials: dict) -> tuple[str, str]:
    """Generate a simple LongPort bot that places one test order.
    Returns (script_path, log_path)."""
    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    email_safe = email.replace("@", "_at_").replace(".", "_")
    script_path = BOTS_DIR / f"{email_safe}_simple_lp.py"
    log_name = f"{email_safe}_simple_lp.log"
    content = SIMPLE_LP_TEMPLATE.format(
        email=email, symbol=symbol,
        central_api_url=credentials.get("central_api_url", ""),
        log_dir=str(LOGS_DIR).replace("\\", "/"),
        master_log_name=log_name,
    )
    script_path.write_text(content, encoding="utf-8")
    return str(script_path.resolve()), str(LOGS_DIR / log_name)


def generate_simple_ibkr_bot(email: str, symbol: str, ibkr_config: dict,
                             credentials: dict) -> tuple[str, str]:
    """Generate a simple IBKR bot that places one test order.
    Returns (script_path, log_path)."""
    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    email_safe = email.replace("@", "_at_").replace(".", "_")
    cid = ibkr_config.get("client_id", 1)
    script_path = BOTS_DIR / f"{email_safe}_simple_ibkr_{cid}.py"
    log_name = f"{email_safe}_simple_ibkr_{cid}.log"
    content = SIMPLE_IBKR_TEMPLATE.format(
        email=email, symbol=symbol,
        ibkr_host=ibkr_config.get("host", "127.0.0.1"),
        ibkr_port=ibkr_config.get("port", 7497),
        ibkr_client_id=cid,
        central_api_url=credentials.get("central_api_url", ""),
        log_dir=str(LOGS_DIR).replace("\\", "/"),
        master_log_name=log_name,
    )
    script_path.write_text(content, encoding="utf-8")
    return str(script_path.resolve()), str(LOGS_DIR / log_name)
