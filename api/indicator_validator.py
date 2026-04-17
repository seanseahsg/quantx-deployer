"""QuantX -- .quantx indicator file validator.

7-stage validation pipeline:
  1. Required fields check
  2. Optional field type validation
  3. ID collision check (DB)
  4. Security scan (forbidden tokens)
  5. Syntax check (compile)
  6. Dry-run execution (sandbox)
  7. Return success with preview
"""

import re
import math
import random
import logging
import threading

log = logging.getLogger("quantx-indicator-validator")

_FORBIDDEN_TOKENS = [
    "import ", "from ", "open(", "exec(", "eval(", "compile(",
    "os.", "sys.", "subprocess", "shutil", "pathlib", "socket",
    "requests.", "urllib", "http.", "__import__",
    "globals(", "locals(", "vars(",
    "getattr(", "setattr(", "delattr(",
]


def validate_quantx_file(data: dict, email: str, conn) -> dict:
    """Run 7 sequential validation stages on a .quantx indicator file.

    Returns {"status": "ok", ...} on full success,
    or {"status": "error", "stage": "...", "message": "..."} on first failure.
    """

    # ── Stage 1: Required fields ──────────────────────────────────────────
    for field in ["quantx_indicator_version", "indicator_id", "name", "calc_code"]:
        if not data.get(field):
            return {"status": "error", "stage": "required_fields",
                    "message": f"Missing required field: {field}"}

    ind_id = data["indicator_id"]
    if not re.match(r'^[A-Z][A-Z0-9_]{1,29}$', ind_id):
        return {"status": "error", "stage": "required_fields",
                "message": "indicator_id must be UPPER_SNAKE_CASE, 2-30 chars, start with letter"}

    calc_code = data["calc_code"]
    if not isinstance(calc_code, list) or not calc_code:
        return {"status": "error", "stage": "required_fields",
                "message": "calc_code must be a non-empty list of strings"}

    # ── Stage 2: Optional field types ─────────────────────────────────────
    if "params" in data and not isinstance(data["params"], list):
        return {"status": "error", "stage": "field_types",
                "message": "params must be a list"}
    if "output_labels" in data and not isinstance(data["output_labels"], list):
        return {"status": "error", "stage": "field_types",
                "message": "output_labels must be a list"}
    if "inputs" in data and not isinstance(data["inputs"], list):
        return {"status": "error", "stage": "field_types",
                "message": "inputs must be a list"}
    if "output_type" in data and data["output_type"] not in ("single", "dual", "triple", "multi"):
        return {"status": "error", "stage": "field_types",
                "message": "output_type must be one of: single, dual, triple, multi"}
    if "warmup_bars" in data and not isinstance(data["warmup_bars"], int):
        return {"status": "error", "stage": "field_types",
                "message": "warmup_bars must be an integer"}

    # ── Stage 3: ID collision check ───────────────────────────────────────
    try:
        row = conn.execute(
            "SELECT indicator_id, is_builtin FROM indicators WHERE indicator_id = ?",
            (ind_id,)).fetchone()
        if row:
            if row["is_builtin"]:
                return {"status": "error", "stage": "collision",
                        "message": f"Cannot overwrite built-in indicator: {ind_id}"}
            return {"status": "exists", "stage": "collision",
                    "message": f"{ind_id} already registered. Overwrite?",
                    "indicator_id": ind_id}
    except Exception as e:
        log.warning("DB collision check failed: %s", e)

    # ── Stage 4: Security scan ────────────────────────────────────────────
    code = "\n".join(calc_code)
    code_lower = code.lower()
    for token in _FORBIDDEN_TOKENS:
        if token.lower() in code_lower:
            return {"status": "error", "stage": "security",
                    "message": f"Forbidden token found: '{token.strip()}'"}

    expected_fn = f"calc_{ind_id.lower()}"
    if expected_fn not in code:
        return {"status": "error", "stage": "security",
                "message": f"Function must be named '{expected_fn}' (not found in code)"}

    # ── Stage 5: Syntax check ────────────────────────────────────────────
    try:
        compile(code, "<indicator>", "exec")
    except SyntaxError as e:
        return {"status": "error", "stage": "syntax",
                "message": f"Syntax error at line {e.lineno}: {e.msg}"}

    # ── Stage 6: Dry-run execution ────────────────────────────────────────
    try:
        preview, warmup_detected = _dry_run(data, code, ind_id)
    except _TimeoutError:
        return {"status": "error", "stage": "execution",
                "message": "Execution timed out (3 second limit)"}
    except Exception as e:
        return {"status": "error", "stage": "execution",
                "message": f"Execution failed: {str(e)[:300]}"}

    # ── Stage 7: Success ──────────────────────────────────────────────────
    return {
        "status": "ok",
        "indicator_id": ind_id,
        "name": data["name"],
        "category": data.get("category", "custom"),
        "output_type": data.get("output_type", "single"),
        "preview": preview,
        "warmup_detected": warmup_detected,
    }


class _TimeoutError(Exception):
    pass


def _dry_run(data: dict, code: str, ind_id: str) -> tuple:
    """Execute indicator code in sandbox with dummy data. Returns (preview, warmup_detected)."""

    # Generate 200 bars of realistic dummy OHLCV data
    random.seed(42)
    closes, opens, highs, lows, volumes = [], [], [], [], []
    price = 100.0
    for _ in range(200):
        ret = random.gauss(0, 0.015)
        price = max(price * (1 + ret), 1.0)
        closes.append(round(price, 4))
        opens.append(round(price * random.uniform(0.995, 1.005), 4))
        highs.append(round(price * random.uniform(1.001, 1.02), 4))
        lows.append(round(price * random.uniform(0.98, 0.999), 4))
        volumes.append(random.randint(100000, 5000000))

    # Build sandbox
    sandbox = {
        "__builtins__": {
            "len": len, "range": range, "list": list, "dict": dict, "tuple": tuple,
            "set": set, "min": min, "max": max, "abs": abs, "sum": sum, "round": round,
            "zip": zip, "enumerate": enumerate, "print": print, "int": int,
            "float": float, "True": True, "False": False, "None": None, "bool": bool,
            "str": str, "sorted": sorted, "reversed": reversed, "map": map,
            "filter": filter, "isinstance": isinstance, "any": any, "all": all,
            "type": type, "hasattr": hasattr, "getattr": getattr,
        },
        "math": math,
    }
    try:
        import numpy as np
        sandbox["np"] = np
    except ImportError:
        pass

    # Map input names to data arrays
    input_map = {
        "closes": closes, "close": closes,
        "highs": highs, "high": highs,
        "lows": lows, "low": lows,
        "opens": opens, "open": opens,
        "volumes": volumes, "volume": volumes,
    }

    # Execute with timeout
    result_holder = [None]
    error_holder = [None]

    def _run():
        try:
            exec(code, sandbox)
            fn_name = f"calc_{ind_id.lower()}"
            fn = sandbox.get(fn_name)
            if fn is None:
                error_holder[0] = f"Function '{fn_name}' not found after execution"
                return

            # Determine input args from declared inputs
            declared_inputs = data.get("inputs", ["closes"])
            args = []
            for inp in declared_inputs:
                arr = input_map.get(inp.lower())
                if arr is None:
                    error_holder[0] = f"Unknown input '{inp}'. Valid: closes, highs, lows, opens, volumes"
                    return
                args.append(arr)

            # Add params as keyword args or positional
            params = data.get("params", [])
            for p in params:
                args.append(p.get("default", p.get("d", 14)))

            result_holder[0] = fn(*args)
        except Exception as e:
            error_holder[0] = str(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=3.0)
    if t.is_alive():
        raise _TimeoutError("Execution timed out")

    if error_holder[0]:
        raise RuntimeError(error_holder[0])

    result = result_holder[0]
    output_type = data.get("output_type", "single")

    # Validate output shape
    if output_type == "single":
        if not isinstance(result, (list, tuple)):
            raise RuntimeError(f"Expected list, got {type(result).__name__}")
        if len(result) != 200:
            raise RuntimeError(f"Output length {len(result)} != input length 200")
        series = list(result)
    elif output_type == "dual":
        if not isinstance(result, (list, tuple)) or len(result) != 2:
            raise RuntimeError(f"Expected tuple/list of 2 series for dual output")
        if len(result[0]) != 200 or len(result[1]) != 200:
            raise RuntimeError(f"Each output series must have length 200")
        series = list(result[0])
    elif output_type == "triple":
        if not isinstance(result, (list, tuple)) or len(result) != 3:
            raise RuntimeError(f"Expected tuple/list of 3 series for triple output")
        series = list(result[0])
    elif output_type == "multi":
        expected = len(data.get("output_labels", []))
        if not isinstance(result, (list, tuple)) or len(result) != expected:
            raise RuntimeError(f"Expected {expected} series for multi output, got {len(result) if isinstance(result, (list,tuple)) else 'non-iterable'}")
        series = list(result[0])
    else:
        series = list(result) if isinstance(result, (list, tuple)) else [result]

    # Extract preview: last 5 non-None values
    non_none = [(i, v) for i, v in enumerate(series) if v is not None]
    preview_vals = [round(v, 4) if isinstance(v, float) else v for _, v in non_none[-5:]]
    warmup_detected = non_none[0][0] if non_none else 200

    return preview_vals, warmup_detected
