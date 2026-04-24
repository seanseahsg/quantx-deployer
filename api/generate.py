"""QuantX Deployer — Unified master bot script generator."""

import json
import re
from datetime import datetime
from .bot_template_lp_options import LP_OPTIONS_BOT_TEMPLATE
import hashlib
from pathlib import Path

from .bot_template import BOT_TEMPLATE
from .bot_template_ibkr import IBKR_BOT_TEMPLATE
from .bot_template_simple import SIMPLE_LP_TEMPLATE, SIMPLE_IBKR_TEMPLATE
from .bot_template_ibkr_prod import IBKR_PROD_TEMPLATE
from .bot_template_lp_master import LP_MASTER_TEMPLATE
try:
    from .bot_template_options import OPTIONS_BOT_TEMPLATE
except ImportError:
    OPTIONS_BOT_TEMPLATE = None
from .config import BOTS_DIR, LOGS_DIR, TRADES_DIR, STATE_DIR


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
    content = SIMPLE_LP_TEMPLATE
    content = content.replace('__EMAIL__', email)
    content = content.replace('__SYMBOL__', symbol)
    content = content.replace('__CENTRAL_API_URL__', credentials.get("central_api_url", ""))
    content = content.replace('__LOG_DIR__', str(LOGS_DIR).replace("\\", "/"))
    content = content.replace('__LOG_NAME__', log_name)
    script_path.write_text(content, encoding="utf-8")
    # Verify template was filled correctly
    assert email in script_path.read_text(encoding="utf-8"), "Template fill failed for LP bot"
    return str(script_path.resolve()), str(LOGS_DIR / log_name)


def generate_simple_ibkr_bot(email: str, symbol: str, ibkr_config: dict,
                             credentials: dict) -> tuple[str, str]:
    """Generate a simple IBKR bot that places one test order.
    Returns (script_path, log_path)."""
    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    email_safe = email.replace("@", "_at_").replace(".", "_")
    # Auto-assign unique client ID from symbol hash to avoid conflicts
    import hashlib
    cid_hash = int(hashlib.md5((email + symbol).encode()).hexdigest(), 16) % 800 + 100
    cid = ibkr_config.get("client_id", cid_hash) if ibkr_config.get("client_id", 1) == 1 else ibkr_config["client_id"]
    if cid <= 10:
        cid = cid_hash  # override low default IDs
    script_path = BOTS_DIR / f"{email_safe}_simple_ibkr_{cid}.py"
    log_name = f"{email_safe}_simple_ibkr_{cid}.log"
    content = SIMPLE_IBKR_TEMPLATE
    content = content.replace('__EMAIL__', email)
    content = content.replace('__SYMBOL__', symbol)
    content = content.replace('__IBKR_HOST__', ibkr_config.get("host", "127.0.0.1"))
    content = content.replace('__IBKR_PORT__', str(ibkr_config.get("port", 7497)))
    content = content.replace('__IBKR_CLIENT_ID__', str(cid))
    content = content.replace('__CENTRAL_API_URL__', credentials.get("central_api_url", ""))
    content = content.replace('__LOG_DIR__', str(LOGS_DIR).replace("\\", "/"))
    content = content.replace('__LOG_NAME__', log_name)
    script_path.write_text(content, encoding="utf-8")
    assert email in script_path.read_text(encoding="utf-8"), "Template fill failed for IBKR bot"
    return str(script_path.resolve()), str(LOGS_DIR / log_name)


# ── Library → Builder conditions mapping ─────────────────────────────────────

_LIBRARY_CONDITIONS = {
    "EMA_CROSS": {"entry_long":[{"left":{"ind":"EMA","params":{"period":20}},"cond":"crosses_above","right":{"type":"indicator","ind":"EMA","params":{"period":50}}}],"exit_long":[{"left":{"ind":"EMA","params":{"period":20}},"cond":"crosses_below","right":{"type":"indicator","ind":"EMA","params":{"period":50}}}]},
    "RSI": {"entry_long":[{"left":{"ind":"RSI","params":{"period":14}},"cond":"is_less_than","right":{"type":"value","value":30}}],"exit_long":[{"left":{"ind":"RSI","params":{"period":14}},"cond":"is_greater_than","right":{"type":"value","value":70}}]},
    "RSI_MEAN_REVERSION": {"entry_long":[{"left":{"ind":"RSI","params":{"period":14}},"cond":"is_less_than","right":{"type":"value","value":30}}],"exit_long":[{"left":{"ind":"RSI","params":{"period":14}},"cond":"is_greater_than","right":{"type":"value","value":70}}]},
    "MACD": {"entry_long":[{"left":{"ind":"MACD_LINE","params":{"fast":12,"slow":26,"signal":9}},"cond":"crosses_above","right":{"type":"indicator","ind":"MACD_SIGNAL","params":{"fast":12,"slow":26,"signal":9}}}],"exit_long":[{"left":{"ind":"MACD_LINE","params":{"fast":12,"slow":26,"signal":9}},"cond":"crosses_below","right":{"type":"indicator","ind":"MACD_SIGNAL","params":{"fast":12,"slow":26,"signal":9}}}]},
    "MACD_MOMENTUM": {"entry_long":[{"left":{"ind":"MACD_LINE","params":{"fast":12,"slow":26,"signal":9}},"cond":"crosses_above","right":{"type":"indicator","ind":"MACD_SIGNAL","params":{"fast":12,"slow":26,"signal":9}}}],"exit_long":[{"left":{"ind":"MACD_LINE","params":{"fast":12,"slow":26,"signal":9}},"cond":"crosses_below","right":{"type":"indicator","ind":"MACD_SIGNAL","params":{"fast":12,"slow":26,"signal":9}}}]},
    "BB_BREAKOUT": {"entry_long":[{"left":{"ind":"close","params":{}},"cond":"crosses_above","right":{"type":"indicator","ind":"BB_UPPER","params":{"period":20,"std":2.0}}}],"exit_long":[{"left":{"ind":"close","params":{}},"cond":"crosses_below","right":{"type":"indicator","ind":"BB_MID","params":{"period":20,"std":2.0}}}]},
    "BB_GRID": {"entry_long":[{"left":{"ind":"close","params":{}},"cond":"is_below","right":{"type":"indicator","ind":"BB_LOWER","params":{"period":20,"std":2.0}}}],"exit_long":[{"left":{"ind":"close","params":{}},"cond":"is_above","right":{"type":"indicator","ind":"BB_UPPER","params":{"period":20,"std":2.0}}}]},
    "SYMMETRIC_GRID": {"entry_long":[{"left":{"ind":"close","params":{}},"cond":"is_below","right":{"type":"indicator","ind":"BB_LOWER","params":{"period":20,"std":2.0}}}],"exit_long":[{"left":{"ind":"close","params":{}},"cond":"is_above","right":{"type":"indicator","ind":"BB_UPPER","params":{"period":20,"std":2.0}}}]},
    "MOMENTUM_BREAKOUT": {"entry_long":[{"left":{"ind":"RSI","params":{"period":14}},"cond":"is_greater_than","right":{"type":"value","value":55}}],"exit_long":[{"left":{"ind":"RSI","params":{"period":14}},"cond":"is_less_than","right":{"type":"value","value":50}}]},
    "VWAP_REVERSION": {"entry_long":[{"left":{"ind":"close","params":{}},"cond":"is_below","right":{"type":"indicator","ind":"VWAP","params":{}}}],"exit_long":[{"left":{"ind":"close","params":{}},"cond":"is_above","right":{"type":"indicator","ind":"VWAP","params":{}}}]},
    "SUPERTREND": {"entry_long":[{"left":{"ind":"close","params":{}},"cond":"is_above","right":{"type":"indicator","ind":"SUPERTREND","params":{"period":10,"multiplier":3.0}}}],"exit_long":[{"left":{"ind":"close","params":{}},"cond":"is_below","right":{"type":"indicator","ind":"SUPERTREND","params":{"period":10,"multiplier":3.0}}}]},
    "TURTLE": {"entry_long":[{"left":{"ind":"close","params":{}},"cond":"is_above","right":{"type":"indicator","ind":"DONCHIAN_UPPER","params":{"period":20}}}],"exit_long":[{"left":{"ind":"close","params":{}},"cond":"is_below","right":{"type":"indicator","ind":"DONCHIAN_LOWER","params":{"period":10}}}]},
    "TURTLE_TRADER": {"entry_long":[{"left":{"ind":"close","params":{}},"cond":"is_above","right":{"type":"indicator","ind":"DONCHIAN_UPPER","params":{"period":20}}}],"exit_long":[{"left":{"ind":"close","params":{}},"cond":"is_below","right":{"type":"indicator","ind":"DONCHIAN_LOWER","params":{"period":10}}}]},
}

def library_id_to_conditions(library_id: str) -> dict:
    """Convert a library strategy ID to Builder conditions format."""
    base = _LIBRARY_CONDITIONS.get(library_id, {})
    return {
        "entry_long": base.get("entry_long", []),
        "exit_long": base.get("exit_long", []),
        "entry_short": base.get("entry_short", []),
        "exit_short": base.get("exit_short", []),
        "entry_long_logic": base.get("entry_long_logic", "AND"),
        "exit_long_logic": base.get("exit_long_logic", "OR"),
    }


# ── IBKR production bot generator ───────────────────────────────────────────

_IND_CALC = {
    'ema': lambda p: f'calc_ema(close, {p.get("period",20)})',
    'sma': lambda p: f'calc_sma(close, {p.get("period",20)})',
    'wma': lambda p: f'calc_wma(close, {p.get("period",20)})',
    'hma': lambda p: f'calc_hma(close, {p.get("period",20)})',
    'rsi': lambda p: f'calc_rsi(close, {p.get("period",14)})',
    'roc': lambda p: f'calc_roc(close, {p.get("period",10)})',
    'macd_line': lambda p: f'calc_macd(close, {p.get("fast",12)}, {p.get("slow",26)}, {p.get("signal",9)})[0]',
    'macd_signal': lambda p: f'calc_macd(close, {p.get("fast",12)}, {p.get("slow",26)}, {p.get("signal",9)})[1]',
    'macd_hist': lambda p: f'calc_macd(close, {p.get("fast",12)}, {p.get("slow",26)}, {p.get("signal",9)})[2]',
    'stoch_k': lambda p: f'calc_stoch(high, low, close, {p.get("k",14)}, {p.get("d",3)})[0]',
    'stoch_d': lambda p: f'calc_stoch(high, low, close, {p.get("k",14)}, {p.get("d",3)})[1]',
    'bb_upper': lambda p: f'calc_bbands(close, {p.get("period",20)}, {p.get("std",2)})[0]',
    'bb_middle': lambda p: f'calc_bbands(close, {p.get("period",20)}, {p.get("std",2)})[1]',
    'bb_mid': lambda p: f'calc_bbands(close, {p.get("period",20)}, {p.get("std",2)})[1]',
    'bb_lower': lambda p: f'calc_bbands(close, {p.get("period",20)}, {p.get("std",2)})[2]',
    'atr': lambda p: f'calc_atr(high, low, close, {p.get("period",14)})',
    'williams_r': lambda p: f'calc_williams_r(high, low, close, {p.get("period",14)})',
    'zscore': lambda p: f'calc_zscore(close, {p.get("period",20)})',
    'obv': lambda p: f'calc_obv(close, volume)',
    'vwap': lambda p: f'calc_vwap(high, low, close, volume)',
    'donch_upper': lambda p: f'calc_donchian(high, low, {p.get("period",20)})[0]',
    'donchian_upper': lambda p: f'calc_donchian(high, low, {p.get("period",20)})[0]',
    'donch_lower': lambda p: f'calc_donchian(high, low, {p.get("period",20)})[1]',
    'donchian_lower': lambda p: f'calc_donchian(high, low, {p.get("period",20)})[1]',
    'supertrend': lambda p: f'calc_supertrend(high, low, close, {p.get("period",10)}, {p.get("multiplier",3.0)})[0]',
}

def _ind_var(ind_id, params):
    """Generate lowercase variable name: ema_20, rsi_14, bb_upper_20_2"""
    lid = ind_id.lower()
    if lid in ('close', 'open', 'high', 'low', 'volume'):
        return lid
    suffix = '_'.join(str(int(v) if isinstance(v, float) and v == int(v) else v)
                      for v in (params or {}).values())
    return f'{lid}_{suffix}' if suffix else lid

def _ind_calc(ind_id, params):
    """Look up calc_* function call string. Case-insensitive."""
    lid = ind_id.lower()
    fn = _IND_CALC.get(lid)
    if fn:
        return fn(params or {})
    # Not found — return as-is (will cause a NameError in the script, which is intentional)
    return lid

def _cond_code(cond, lv, rv, is_value=False):
    L, Lp = f'{lv}[i]', f'{lv}[i-1]'
    if is_value:
        R, Rp = str(rv), str(rv)
    else:
        R, Rp = f'{rv}[i]', f'{rv}[i-1]'
    if cond == 'crosses_above': return f'({L} > {R}) and ({Lp} <= {Rp})'
    if cond == 'crosses_below': return f'({L} < {R}) and ({Lp} >= {Rp})'
    if cond in ('is_above', 'gt', 'is_greater_than'): return f'{L} > {R}'
    if cond in ('is_below', 'lt', 'is_less_than'): return f'{L} < {R}'
    return 'True'


def generate_signal_code(entry_long, exit_long, entry_short=None, exit_short=None,
                         entry_long_logic='AND', exit_long_logic='OR', has_short=False):
    """Convert builder conditions to compute_signals() Python function."""
    all_conds = list(entry_long or []) + list(exit_long or [])
    if has_short:
        all_conds += list(entry_short or []) + list(exit_short or [])

    # Collect unique indicators
    vars_map = {}
    for c in all_conds:
        for side in [c.get('left', {}), c.get('right', {})]:
            ind_id = side.get('id') or side.get('ind', '')
            if not ind_id or ind_id.lower() in ('close','open','high','low','volume'):
                continue
            if side.get('type') == 'value':
                continue
            params = side.get('p') or side.get('params') or {}
            vn = _ind_var(ind_id, params)
            if vn not in vars_map:
                vars_map[vn] = _ind_calc(ind_id, params)

    lines = [
        "def compute_signals(bars):",
        "    if len(bars) < 60: return [None] * len(bars)",
        "    close  = [b['close'] for b in bars]",
        "    high   = [b['high']  for b in bars]",
        "    low    = [b['low']   for b in bars]",
        "    volume = [b['volume'] for b in bars]",
        "    n = len(bars)",
        "    signals = [None] * n",
        "    in_long = False",
    ]
    if has_short:
        lines.append("    in_short = False")
    lines.append("")

    for vn, calc in vars_map.items():
        lines.append(f"    {vn} = {calc}")
    lines.append("")

    lines.append("    for i in range(1, n):")
    # Null checks
    if vars_map:
        vlist = ', '.join(f'{v}[i]' for v in vars_map)
        vlist_p = ', '.join(f'{v}[i-1]' for v in vars_map)
        lines.append(f"        if any(v is None for v in [{vlist}]): continue")
        lines.append(f"        if any(v is None for v in [{vlist_p}]): continue")
    lines.append("")

    def _build_cond_expr(conditions, logic):
        parts = []
        for c in conditions:
            left = c.get('left', {})
            right = c.get('right', {})
            lid = left.get('id') or left.get('ind', 'close')
            lp = left.get('p') or left.get('params') or {}
            lv = _ind_var(lid, lp)
            cond_type = c.get('cond', 'is_above')
            is_val = right.get('type') == 'value'
            if is_val:
                rv = right.get('value', 0)
                parts.append(_cond_code(cond_type, lv, rv, True))
            else:
                rid = right.get('id') or right.get('ind', 'close')
                rp = right.get('p') or right.get('params') or {}
                rv = _ind_var(rid, rp)
                parts.append(_cond_code(cond_type, lv, rv, False))
        joiner = ' and ' if logic == 'AND' else ' or '
        return joiner.join(parts) if parts else 'False'

    # Entry/exit conditions
    el_expr = _build_cond_expr(entry_long or [], entry_long_logic)
    xl_expr = _build_cond_expr(exit_long or [], exit_long_logic)
    lines.append(f"        cond_entry_long = {el_expr}")
    lines.append(f"        cond_exit_long = {xl_expr}")

    if has_short:
        es_expr = _build_cond_expr(entry_short or [], 'AND')
        xs_expr = _build_cond_expr(exit_short or [], 'OR')
        lines.append(f"        cond_entry_short = {es_expr}")
        lines.append(f"        cond_exit_short = {xs_expr}")

    lines.append("")
    lines.append("        if not in_long and cond_entry_long:")
    lines.append("            signals[i] = 'buy'; in_long = True")
    lines.append("        elif in_long and cond_exit_long:")
    lines.append("            signals[i] = 'sell'; in_long = False")

    if has_short:
        lines.append("        elif not in_short and cond_entry_short:")
        lines.append("            signals[i] = 'short'; in_short = True")
        lines.append("        elif in_short and cond_exit_short:")
        lines.append("            signals[i] = 'cover'; in_short = False")

    lines.append("")
    lines.append("    return signals")
    return "\n".join(lines)


def generate_ibkr_bot_prod(email: str, strategy_config: dict,
                           ibkr_config: dict) -> tuple[str, str, str]:
    """Generate a production IBKR bot script.
    Returns (script_path, log_path, trades_path)."""
    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    TRADES_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    email_safe = email.replace("@", "_at_").replace(".", "_")
    sid = strategy_config.get("strategy_id", "CUSTOM")
    cid = int(hashlib.md5((email + strategy_config.get("symbol", "")).encode()).hexdigest(), 16) % 800 + 100

    script_path = BOTS_DIR / f"{email_safe}_{sid}_ibkr.py"
    log_name = f"{sid}.log"
    trades_path = str(TRADES_DIR / f"trades_{sid}_all.csv")

    # Generate signal code
    signal_code = generate_signal_code(
        entry_long=strategy_config.get("entry_long", []),
        exit_long=strategy_config.get("exit_long", []),
        entry_short=strategy_config.get("entry_short", []),
        exit_short=strategy_config.get("exit_short", []),
        entry_long_logic=strategy_config.get("entry_long_logic", "AND"),
        exit_long_logic=strategy_config.get("exit_long_logic", "OR"),
        has_short=strategy_config.get("has_short", False),
    )

    content = IBKR_PROD_TEMPLATE
    replacements = {
        '__STRATEGY_NAME__': sid,
        '__ACCOUNT_ID__': ibkr_config.get("account_id", ""),
        '__PORT__': str(ibkr_config.get("port", 7497)),
        '__CLIENT_ID__': str(cid),
        '__EMAIL__': email,
        '__CENTRAL_API_URL__': ibkr_config.get("central_api_url", ""),
        '__SYMBOL__': strategy_config.get("symbol", "AAPL"),
        '__SEC_TYPE__': strategy_config.get("sec_type", "STK"),
        '__EXCHANGE__': strategy_config.get("exchange", "SMART"),
        '__CURRENCY__': strategy_config.get("currency", "USD"),
        '__LOT_SIZE__': str(strategy_config.get("lot_size", 1)),
        '__MAX_CAPITAL__': str(strategy_config.get("max_capital", 1000)),
        '__BAR_SIZE__': strategy_config.get("bar_size", "1 min"),
        '__INTERVAL_MINUTES__': str(strategy_config.get("interval_minutes", 1)),
        '__STOP_LOSS_PCT__': str(strategy_config.get("stop_loss_pct", 0.02)),
        '__TAKE_PROFIT_PCT__': str(strategy_config.get("take_profit_pct", 0.05)),
        '__HAS_SHORT__': str(strategy_config.get("has_short", False)),
        '__KILL_SWITCH_PCT__': str(strategy_config.get("kill_switch_pct", 0.02)),
        '__LOG_DIR__': str(LOGS_DIR).replace("\\", "/"),
        '__TRADES_DIR__': str(TRADES_DIR).replace("\\", "/"),
        '__STATE_DIR__': str(STATE_DIR).replace("\\", "/"),
        '__SIGNAL_CODE__': signal_code,
    }

    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)

    script_path.write_text(content, encoding="utf-8")

    # Verify
    written = script_path.read_text(encoding="utf-8")
    assert email in written, f"Template fill failed: email not in script"
    import re
    remaining = re.findall(r'__[A-Z_]{3,}__', written)
    assert not remaining, f"Unfilled placeholders: {remaining}"

    return str(script_path.resolve()), str(LOGS_DIR / log_name), trades_path


# ── LongPort master bot generator (shared connections) ─────────────────────

def generate_lp_master_bot(email: str, strategies: list[dict],
                           lp_credentials: dict,
                           initial_states: dict = None) -> tuple[str, str]:
    """Generate one LP master bot handling multiple strategies with shared connections.
    initial_states: {strategy_id: {position, entry_price, side}} for position preservation.
    Returns (script_path, log_path)."""
    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    TRADES_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    email_safe = email.replace("@", "_at_").replace(".", "_")
    script_path = BOTS_DIR / f"{email_safe}_lp_master.py"
    log_name = f"{email_safe}_lp_master.log"

    # Generate per-strategy compute_signals functions
    signal_functions = []
    strategies_meta = []
    for s in strategies:
        sid = s.get("strategy_id", "CUSTOM")
        conds = s.get("conditions", {})
        # If library strategy, convert to Builder conditions
        if not conds.get("entry_long") and s.get("library_id"):
            conds = library_id_to_conditions(s["library_id"])

        fn_code = generate_signal_code(
            entry_long=conds.get("entry_long", []),
            exit_long=conds.get("exit_long", []),
            entry_short=conds.get("entry_short", []),
            exit_short=conds.get("exit_short", []),
            entry_long_logic=conds.get("entry_long_logic", "AND"),
            exit_long_logic=conds.get("exit_long_logic", "OR"),
            has_short=bool(conds.get("entry_short")),
        )
        # Rename compute_signals -> compute_signals_XXXX
        fn_code = fn_code.replace("def compute_signals(", f"def compute_signals_{sid}(")
        signal_functions.append(f"# Strategy: {sid} ({s.get('symbol', '?')})")
        signal_functions.append(fn_code)
        signal_functions.append("")

        risk = s.get("risk", {})
        from .config import normalize_timeframe
        tf = normalize_timeframe(s.get("timeframe", "1d"))
        state = (initial_states or {}).get(sid, {})
        strategies_meta.append({
            "strategy_id": sid,
            "symbol": s.get("symbol", ""),
            "arena": s.get("arena", "HK"),
            "timeframe": tf,
            "risk": risk,
            "has_short": bool(conds.get("entry_short")),
            "initial_position": int(state.get("position", state.get("current_position", 0))),
            "initial_entry_price": float(state.get("entry_price", 0.0)),
        })

    # Build strategies list as Python literal
    strats_json = json.dumps(strategies_meta, indent=4)
    strats_json = strats_json.replace(": true", ": True")
    strats_json = strats_json.replace(": false", ": False")
    strats_json = strats_json.replace(": null", ": None")

    # Load custom indicators from DB and inject into generated script
    custom_code_blocks = []
    try:
        from .database import get_db, get_custom_indicators
        conn = get_db()
        customs = get_custom_indicators(conn, email)
        conn.close()
        for ind in customs:
            code = ind.get("calc_code", "")
            if code:
                custom_code_blocks.append(f"# Custom indicator: {ind.get('name', ind['indicator_id'])}")
                custom_code_blocks.append(code)
                custom_code_blocks.append("")
    except Exception as e:
        import logging
        logging.getLogger("quantx-generate").warning("Custom indicator load failed: %s", e)

    if custom_code_blocks:
        signal_functions = custom_code_blocks + [""] + signal_functions

    content = LP_MASTER_TEMPLATE
    replacements = {
        '__EMAIL__': email,
        '__CENTRAL_API_URL__': lp_credentials.get("central_api_url", ""),
        '__APP_KEY__': lp_credentials.get("app_key", ""),
        '__APP_SECRET__': lp_credentials.get("app_secret", ""),
        '__ACCESS_TOKEN__': lp_credentials.get("access_token", ""),
        '__LOG_DIR__': str(LOGS_DIR).replace("\\", "/"),
        '__TRADES_DIR__': str(TRADES_DIR).replace("\\", "/"),
        '__STATE_DIR__': str(STATE_DIR).replace("\\", "/"),
        '__LOG_NAME__': log_name,
        '__STRATEGY_COUNT__': str(len(strategies_meta)),
        '__STRATEGIES_LIST__': strats_json,
        '__SIGNAL_FUNCTIONS__': "\n".join(signal_functions),
    }

    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)

    script_path.write_text(content, encoding="utf-8")

    # Verify
    written = script_path.read_text(encoding="utf-8")
    assert email in written, "Template fill failed: email not in script"
    import re
    remaining = re.findall(r'__[A-Z_]{3,}__', written)
    assert not remaining, f"Unfilled placeholders: {remaining}"

    return str(script_path.resolve()), str(LOGS_DIR / log_name)


# ── Options bot generator ────────────────────────────────────────────────────

def generate_options_bot(email: str, options_config: dict,
                         ibkr_config: dict) -> tuple[str, str, str]:
    """Generate a production options bot script (SPX 0DTE etc).
    Returns (script_path, log_path, trades_path)."""
    if OPTIONS_BOT_TEMPLATE is None:
        raise ValueError("Options bot template not available")

    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    TRADES_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    email_safe = email.replace("@", "_at_").replace(".", "_")
    sid = options_config.get("strategy_id", "OPT_CUSTOM")
    cid = int(hashlib.md5((email + sid).encode()).hexdigest(), 16) % 800 + 100

    script_path = BOTS_DIR / f"{email_safe}_{sid}_opt.py"
    log_name = f"{sid}.log"
    trades_path = str(TRADES_DIR / f"trades_{sid}_all.csv")

    content = OPTIONS_BOT_TEMPLATE
    replacements = {
        '__STRATEGY_NAME__': sid,
        '__ACCOUNT_ID__': ibkr_config.get("account_id", ""),
        '__PORT__': str(ibkr_config.get("port", 7497)),
        '__CLIENT_ID__': str(cid),
        '__EMAIL__': email,
        '__CENTRAL_API_URL__': ibkr_config.get("central_api_url", ""),
        '__UNDERLYING__': options_config.get("underlying", "SPX"),
        '__UNDERLYING_SEC_TYPE__': options_config.get("underlying_sec_type", "IND"),
        '__UNDERLYING_EXCHANGE__': options_config.get("underlying_exchange", "CBOE"),
        '__OPTION_TRADING_CLASS__': options_config.get("option_trading_class", "SPXW"),
        '__STRATEGY_TYPE__': options_config.get("strategy_type", "IRON_CONDOR"),
        '__STRIKE_METHOD__': options_config.get("strike_method", "DELTA"),
        '__PUT_DELTA__': str(options_config.get("put_delta", -0.16)),
        '__CALL_DELTA__': str(options_config.get("call_delta", 0.16)),
        '__PUT_PCT_OTM__': str(options_config.get("put_pct_otm", 0.03)),
        '__CALL_PCT_OTM__': str(options_config.get("call_pct_otm", 0.03)),
        '__PUT_POINTS_OTM__': str(options_config.get("put_points_otm", 50)),
        '__CALL_POINTS_OTM__': str(options_config.get("call_points_otm", 50)),
        '__PUT_WING_WIDTH__': str(options_config.get("put_wing_width", 5.0)),
        '__CALL_WING_WIDTH__': str(options_config.get("call_wing_width", 5.0)),
        '__MULTIPLIER__': str(options_config.get("multiplier", 100)),
        '__ENTRY_TIME_ET__': options_config.get("entry_time_et", "13:30"),
        '__EXIT_TIME_ET__': options_config.get("exit_time_et", "15:59"),
        '__RESTART_TIME_ET__': options_config.get("restart_time_et", "09:30"),
        '__SMA_FILTER__': str(options_config.get("sma_filter", False)),
        '__SMA_PERIOD__': str(options_config.get("sma_period", 4)),
        '__SMA_DIRECTION__': options_config.get("sma_direction", "above"),
        '__IV_FILTER__': str(options_config.get("iv_filter", False)),
        '__MIN_VIX__': str(options_config.get("min_vix", 15)),
        '__MAX_VIX__': str(options_config.get("max_vix", 40)),
        '__SKIP_FOMC__': str(options_config.get("skip_fomc", False)),
        '__RISK_PCT__': str(options_config.get("risk_pct", 0.02)),
        '__MIN_CONTRACTS__': str(options_config.get("min_contracts", 1)),
        '__MAX_CONTRACTS__': str(options_config.get("max_contracts", 10)),
        '__ACCOUNT_EQUITY__': str(options_config.get("account_equity", 100000)),
        '__PROFIT_TARGET_PCT__': str(options_config.get("profit_target_pct", 0.50)),
        '__LOSS_LIMIT_MULT__': str(options_config.get("loss_limit_mult", 2.0)),
        '__EOD_EXIT__': str(options_config.get("eod_exit", True)),
        '__MAX_DAILY_LOSS__': str(options_config.get("max_daily_loss", 5000)),
        '__MAX_ORDERS_PER_DAY__': str(options_config.get("max_orders_per_day", 2)),
        '__DRY_RUN__': str(options_config.get("dry_run", True)),
        '__LOG_DIR__': str(LOGS_DIR).replace("\\", "/"),
        '__TRADES_DIR__': str(TRADES_DIR).replace("\\", "/"),
        '__STATE_DIR__': str(STATE_DIR).replace("\\", "/"),
    }

    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)

    script_path.write_text(content, encoding="utf-8")

    # Verify
    written = script_path.read_text(encoding="utf-8")
    assert email in written, "Template fill failed: email not in script"
    import re
    remaining = re.findall(r'__[A-Z_]{3,}__', written)
    assert not remaining, f"Unfilled placeholders: {remaining}"

    return str(script_path.resolve()), str(LOGS_DIR / log_name), trades_path


# ══════════════════════════════════════════════════════════════════════════════
# LongPort Options Bot Generator (Options Studio -> deployable bot)
# ══════════════════════════════════════════════════════════════════════════════

def generate_lp_options_bot(config: dict, student: dict, paths: dict) -> str:
    """
    Generate a LongPort options bot script from Options Studio config.

    config  -- Options Studio backtest config (same keys as run_options_backtest)
    student -- {email, app_key, app_secret, access_token, central_api_url}
    paths   -- {log_dir, trades_dir, state_dir, script_path}
    Returns -- the generated Python script as a string.
    """
    import json
    from pathlib import Path

    template_path = Path(__file__).parent / "bot_template_lp_options.py"
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")

    import importlib.util
    spec = importlib.util.spec_from_file_location("tpl_lp_opt", template_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    template = mod.LP_OPTIONS_BOT_TEMPLATE

    strategy_type = config.get("strategy_type", "SHORT_PUT_SPREAD")
    strike_method = config.get("short_strike_method", "DELTA")

    custom_legs = config.get("custom_legs", [])
    custom_legs_repr = json.dumps(custom_legs, indent=2) if custom_legs else "[]"

    entry_days = config.get("entry_days", ["Mon", "Tue", "Wed", "Thu", "Fri"])
    entry_days_repr = repr(entry_days)

    profit_target_pct = float(config.get("profit_target_pct", 50)) / 100.0
    stop_loss_pct = float(config.get("stop_loss_pct", 200)) / 100.0
    contracts = int(config.get("contracts", 1))
    starting_capital = float(config.get("starting_capital", 10000))
    max_daily_loss = starting_capital * 0.05  # 5% of capital

    symbol_clean = config.get("symbol", "SPY").replace(".", "_")
    strategy_name = (
        f"{symbol_clean}_{strategy_type}_{config.get('target_dte', 7)}DTE"
        .replace("__", "_").upper()
    )

    subs = {
        "__STRATEGY_NAME__":       strategy_name,
        "__EMAIL__":               student.get("email", ""),
        "__CENTRAL_API_URL__":     student.get("central_api_url", ""),
        "__APP_KEY__":             student.get("app_key", ""),
        "__APP_SECRET__":          student.get("app_secret", ""),
        "__ACCESS_TOKEN__":        student.get("access_token", ""),
        "__UNDERLYING__":          config.get("symbol", "SPY") + ".US",
        "__STRATEGY_TYPE__":       strategy_type,
        "__STRIKE_METHOD__":       strike_method,
        "__TARGET_DTE__":          str(int(config.get("target_dte", 7))),
        "__DTE_TOLERANCE__":       str(int(config.get("dte_tolerance", 2))),
        "__PUT_DELTA__":           str(float(config.get("short_strike_value", -0.30))),
        "__CALL_DELTA__":          str(float(config.get("short_call_strike_value", 0.30))),
        "__PUT_WING_WIDTH__":      str(float(config.get("wing_width_value", 5.0))),
        "__CALL_WING_WIDTH__":     str(float(config.get("call_wing_width_value", 5.0))),
        "__CUSTOM_LEGS__":         custom_legs_repr,
        "__ENTRY_TIME_ET__":       config.get("entry_time", "09:45"),
        "__EXIT_TIME_ET__":        config.get("exit_time", "15:45"),
        "__ENTRY_DAYS__":          entry_days_repr,
        "__PROFIT_TARGET_PCT__":   str(profit_target_pct),
        "__STOP_LOSS_MULT__":      str(stop_loss_pct),
        "__CONTRACTS__":           str(contracts),
        "__MAX_DAILY_LOSS__":      str(max_daily_loss),
        "__DRY_RUN__":             "True",  # always paper first
        "__LOG_DIR__":             str(paths.get("log_dir", "./logs")),
        "__TRADES_DIR__":          str(paths.get("trades_dir", "./trades")),
        "__STATE_DIR__":           str(paths.get("state_dir", "./state")),
    }

    script = template
    for placeholder, value in subs.items():
        script = script.replace(placeholder, value)

    remaining = [p for p in subs if p in script]
    if remaining:
        raise ValueError(f"Unsubstituted placeholders: {remaining}")
    return script


def save_lp_options_bot(config: dict, student: dict, output_path) -> str:
    """Generate and save the bot script. Returns the script path as a string."""
    from pathlib import Path
    symbol_clean = config.get("symbol", "SPY").replace(".", "_")
    strategy_type = config.get("strategy_type", "SHORT_PUT_SPREAD")
    dte = config.get("target_dte", 7)

    out = Path(output_path)
    out.mkdir(parents=True, exist_ok=True)

    script_name = f"bot_{symbol_clean}_{strategy_type}_{dte}DTE.py"
    script_path = out / script_name

    paths = {
        "log_dir":    str(out / "logs"),
        "trades_dir": str(out / "trades"),
        "state_dir":  str(out / "state"),
        "script_path": str(script_path),
    }

    script = generate_lp_options_bot(config, student, paths)
    script_path.write_text(script, encoding="utf-8")
    return str(script_path)
# ─────────────────────────────────────────────────────────────────────────────
# INSTRUCTIONS: make two changes to api/generate.py
#
# 1. Add these imports near the top (after "import json"):
#
#       import re
#       from datetime import datetime
#       from .bot_template_lp_options import LP_OPTIONS_BOT_TEMPLATE
#
# 2. Paste the function below at the END of the file.
# ─────────────────────────────────────────────────────────────────────────────


def save_lp_options_bot(config: dict, student: dict, output_path: str) -> str:
    """Generate a LongPort options bot from an Options Studio config.

    Writes two files to output_path/:
      bot_{symbol}_{strategy}_{dte}DTE.py   — the runnable bot
      bot_{symbol}_{strategy}_{dte}DTE.json — metadata for the orchestrator

    Returns the absolute path to the .py script.
    """
    email        = student["email"]
    app_key      = student["app_key"]
    app_secret   = student["app_secret"]
    access_token = student["access_token"]
    central_url  = student.get("central_api_url", "")

    symbol        = config["symbol"]
    strategy_type = config["strategy_type"]
    target_dte    = int(config.get("target_dte", 7))

    # LP options need ".US" suffix
    underlying   = symbol if symbol.endswith(".US") else f"{symbol}.US"
    symbol_clean = symbol.replace(".US", "")
    strategy_id  = f"OPT_{symbol_clean}_{strategy_type}_{target_dte}DTE"

    out_dir = Path(output_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    script_path = out_dir / f"bot_{symbol_clean}_{strategy_type}_{target_dte}DTE.py"
    meta_path   = out_dir / f"bot_{symbol_clean}_{strategy_type}_{target_dte}DTE.json"

    dry_run     = config.get("dry_run", True)
    custom_legs = config.get("custom_legs", [])

    replacements = {
        # String placeholders (template wraps in quotes already)
        "__STRATEGY_NAME__":   strategy_id,
        "__EMAIL__":           email,
        "__CENTRAL_API_URL__": central_url,
        "__APP_KEY__":         app_key,
        "__APP_SECRET__":      app_secret,
        "__ACCESS_TOKEN__":    access_token,
        "__UNDERLYING__":      underlying,
        "__STRATEGY_TYPE__":   strategy_type,
        "__STRIKE_METHOD__":   config.get("strike_method", "DELTA"),
        "__ENTRY_TIME_ET__":   config.get("entry_time_et", "09:45"),
        "__EXIT_TIME_ET__":    config.get("exit_time_et", "15:45"),
        "__LOG_DIR__":         str(LOGS_DIR).replace("\\", "/"),
        "__TRADES_DIR__":      str(TRADES_DIR).replace("\\", "/"),
        "__STATE_DIR__":       str(STATE_DIR).replace("\\", "/"),
        # Python literal placeholders (no quotes in template)
        "__TARGET_DTE__":        str(target_dte),
        "__DTE_TOLERANCE__":     str(int(config.get("dte_tolerance", 2))),
        "__PUT_DELTA__":         str(float(config.get("put_delta", -0.30))),
        "__CALL_DELTA__":        str(float(config.get("call_delta",  0.30))),
        "__PUT_WING_WIDTH__":    str(float(config.get("put_wing_width", 5.0))),
        "__CALL_WING_WIDTH__":   str(float(config.get("call_wing_width", 5.0))),
        "__PROFIT_TARGET_PCT__": str(float(config.get("profit_target_pct", 0.50))),
        "__STOP_LOSS_MULT__":    str(float(config.get("stop_loss_mult", 2.0))),
        "__CONTRACTS__":         str(int(config.get("contracts", 1))),
        "__MAX_DAILY_LOSS__":    str(float(config.get("max_daily_loss", 5000))),
        "__DRY_RUN__":           "True" if dry_run else "False",
        "__ENTRY_DAYS__":        repr(config.get("entry_days", ["Mon","Tue","Wed","Thu","Fri"])),
        "__CUSTOM_LEGS__":       repr(custom_legs),
    }

    content = LP_OPTIONS_BOT_TEMPLATE
    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)

    remaining = re.findall(r'__[A-Z_]{3,}__', content)
    if remaining:
        raise ValueError(f"Unfilled placeholders in LP options template: {remaining}")

    script_path.write_text(content, encoding="utf-8")

    # Metadata JSON — orchestrator reads this to decide what to run
    meta = {
        "strategy_id":    strategy_id,
        "email":          email,
        "symbol":         symbol_clean,
        "underlying":     underlying,
        "strategy_type":  strategy_type,
        "target_dte":     target_dte,
        "strike_method":  config.get("strike_method", "DELTA"),
        "broker":         "longport",
        "bot_type":       "lp_options",
        "enabled":        True,
        "dry_run":        dry_run,
        "script_path":    str(script_path.resolve()),
        "created_at":     datetime.utcnow().isoformat(),
        "status":         "pending",
        "pid":            None,
        "last_heartbeat": None,
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return str(script_path.resolve())