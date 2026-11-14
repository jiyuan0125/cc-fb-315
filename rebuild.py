#!/usr/bin/env python3
"""Rebuild the current terminal state of every order from the CDC event stream.

Input : one or more *.jsonl files (one JSON event per line) inside a directory.
Ordering is by `lsn` (globally increasing transaction sequence number), never by
file order or `ts`: the same order can have several events within one second and
file order is only disk-write order.

amount has two conventions with no marker between them:
  * early events: JSON integer, unit = cents
  * late  events: JSON string with two decimals, unit = yuan
The convention is detected per event from the JSON type. All amounts are
converted to integer cents for arithmetic, so mixed-convention totals are exact
and can be re-added line by line in cents.

Outputs:
  current.csv : oid,status,amount,reopened
                amount formatted in yuan with exactly two decimals.
                Orders whose last event is D (deleted) are no longer current and
                are not listed.
  totals.txt  : "<order rows> <amount total in yuan> <early cents lines> <late yuan lines>"
"""

import argparse
import json
import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

CENT = Decimal("0.01")


class BadEvent(Exception):
    pass


def amount_to_cents(value, where):
    """Return (cents, convention) where convention is 'cents' or 'yuan'."""
    if isinstance(value, bool):
        raise BadEvent(f"{where}: amount is a bool")
    if isinstance(value, int):
        return value, "cents"
    if isinstance(value, float):
        # Not part of either documented convention: refuse rather than guess.
        raise BadEvent(f"{where}: amount is a JSON float ({value!r})")
    if isinstance(value, str):
        try:
            d = Decimal(value)
        except Exception as exc:  # noqa: BLE001
            raise BadEvent(f"{where}: amount {value!r} is not a valid decimal") from exc
        if not d.is_finite():
            raise BadEvent(f"{where}: amount {value!r} is not finite")
        # Round each line to a whole cent, then keep everything in integer cents.
        return int((d * 100).to_integral_value(rounding=ROUND_HALF_UP)), "yuan"
    raise BadEvent(f"{where}: amount has unsupported type {type(value).__name__}")


def cents_to_yuan(cents):
    return format(Decimal(cents).scaleb(-2), "f")


def load_events(data_dir):
    files = sorted(Path(data_dir).glob("*.jsonl"))
    if not files:
        raise SystemExit(f"no *.jsonl files in {data_dir}")
    events = []
    for path in files:
        with path.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                where = f"{path}:{lineno}"
                try:
                    ev = json.loads(line)
                    events.append(
                        {
                            "op": ev["op"],
                            "lsn": ev["lsn"],
                            "ts": ev["ts"],
                            "oid": ev["oid"],
                            "amount": ev["amount"],
                            "status": ev["status"],
                            "_where": where,
                        }
                    )
                except (json.JSONDecodeError, KeyError) as exc:
                    raise BadEvent(f"{where}: malformed event: {exc}") from exc
    return events


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("data_dir", nargs="?", default="cdc", help="directory with *.jsonl (default: cdc)")
    ap.add_argument("--out", default=".", help="output directory (default: .)")
    args = ap.parse_args(argv)

    events = load_events(args.data_dir)

    # lsn is the transaction order; it must be unique and usable as the sort key.
    seen_lsn = {}
    for ev in events:
        if not isinstance(ev["lsn"], int) or isinstance(ev["lsn"], bool):
            raise BadEvent(f"{ev['_where']}: lsn is not an integer")
        if ev["lsn"] in seen_lsn:
            raise BadEvent(f"{ev['_where']}: duplicate lsn {ev['lsn']} (also {seen_lsn[ev['lsn']]})")
        seen_lsn[ev["lsn"]] = ev["_where"]
        if ev["op"] not in ("I", "U", "D"):
            raise BadEvent(f"{ev['_where']}: unknown op {ev['op']!r}")
        if not isinstance(ev["oid"], str):
            raise BadEvent(f"{ev['_where']}: oid is not a string")

    by_oid = {}
    early_lines = 0
    late_lines = 0
    for ev in events:
        _, conv = amount_to_cents(ev["amount"], ev["_where"])
        if conv == "cents":
            early_lines += 1
        else:
            late_lines += 1
        by_oid.setdefault(ev["oid"], []).append(ev)

    rows = []
    total_cents = 0
    for oid, evs in by_oid.items():
        evs.sort(key=lambda e: e["lsn"])

        reopened = False
        deleted = False
        for ev in evs:
            if ev["op"] == "D":
                deleted = True
            elif ev["op"] == "I" and deleted:
                reopened = True

        last = evs[-1]
        if last["op"] == "D":
            continue  # deleted orders have no current state

        cents, _ = amount_to_cents(last["amount"], last["_where"])
        total_cents += cents
        rows.append((oid, last["status"], cents, reopened))

    rows.sort(key=lambda r: r[0])

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    with (out / "current.csv").open("w", encoding="utf-8", newline="") as fh:
        fh.write("oid,status,amount,reopened\n")
        for oid, status, cents, reopened in rows:
            fh.write(f"{oid},{status},{cents_to_yuan(cents)},{1 if reopened else 0}\n")

    with (out / "totals.txt").open("w", encoding="utf-8") as fh:
        fh.write(
            f"{len(rows)} {cents_to_yuan(total_cents)} {early_lines} {late_lines}\n"
        )

    # Self-check: re-add the emitted per-order cents exactly as they were summed.
    assert sum(r[2] for r in rows) == total_cents
    if early_lines + late_lines != len(events):
        raise SystemExit("internal error: line convention counts do not add up")

    return 0


if __name__ == "__main__":
    sys.exit(main())
