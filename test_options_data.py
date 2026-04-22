"""Smoke test for api/options_data.py -- exercises R2 connection + core helpers.

Run from the submodule root:
    python test_options_data.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.options_data import (
    get_chain_for_date,
    find_option_by_delta,
    get_available_expiries,
    get_available_dates,
)

SYMBOL = "SPY"
DATE = "2024-06-03"
ENTRY = "09:45"


def main():
    print(f"[1] Fetching {SYMBOL} chain for {DATE} at {ENTRY}...")
    chain = get_chain_for_date(SYMBOL, DATE, ENTRY)
    print(f"    Rows: {len(chain)}")
    if chain.empty:
        print("    ** Empty chain -- aborting further checks. **")
        return
    print(f"    Expirations available: {chain['expiration'].nunique()}")
    spot = float(chain["underlying_price"].iloc[0])
    print(f"    Underlying price:      ${spot:,.2f}")

    print(f"\n[2] Finding -0.30 delta PUT within 7 DTE...")
    expiries_7d = get_available_expiries(chain, min_dte=0, max_dte=7)
    print(f"    Eligible expirations: {expiries_7d}")
    if expiries_7d:
        pool = chain[chain["expiration"].astype(str).isin([str(e) for e in expiries_7d])]
        put = find_option_by_delta(pool, "PUT", -0.30)
        print(f"    Strike     ${put['strike']:,.2f}")
        print(f"    Expiration {put['expiration']}")
        print(f"    Delta      {put['delta']:+.3f}")
        print(f"    Bid/Ask    ${put['bid']:.2f} / ${put['ask']:.2f}   Mid ${put['mid']:.2f}")
        print(f"    IV         {put['implied_vol']:.2%}")
    else:
        print("    No expirations within 7 DTE.")

    print(f"\n[3] Finding +0.30 delta CALL within 7 DTE...")
    if expiries_7d:
        pool = chain[chain["expiration"].astype(str).isin([str(e) for e in expiries_7d])]
        call = find_option_by_delta(pool, "CALL", 0.30)
        print(f"    Strike     ${call['strike']:,.2f}")
        print(f"    Expiration {call['expiration']}")
        print(f"    Delta      {call['delta']:+.3f}")
        print(f"    Bid/Ask    ${call['bid']:.2f} / ${call['ask']:.2f}   Mid ${call['mid']:.2f}")
        print(f"    IV         {call['implied_vol']:.2%}")
    else:
        print("    No expirations within 7 DTE.")

    print(f"\n[4] Listing available {SYMBOL} dates from index.csv...")
    dates = get_available_dates(SYMBOL)
    print(f"    Total dates: {len(dates)}")
    if dates:
        print(f"    First: {dates[0]}")
        print(f"    Last:  {dates[-1]}")

    print("\nDone.")


if __name__ == "__main__":
    main()
