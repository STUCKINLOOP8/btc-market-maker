"""Build a held-out strategy comparison from saved benchmark batches."""

import argparse
import json
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from html import escape
from pathlib import Path

from .analytics import EQUITY_SAMPLE_MS, MARKOUT_HORIZONS_MS, measure_audit
from .benchmark import PACKAGE, PROJECT, _read_json, _sha256, _verify_file, _write_json
from .recording import read_recording


SENSITIVITY = PROJECT / "runs" / "milestone-5" / "sensitivity"
DEFAULT_OUTPUT = PROJECT / "runs" / "milestone-5" / "report"
DEFAULT_CASES = (
    ("Reference", SENSITIVITY / "reference"),
    ("Lower maker fee", SENSITIVITY / "fees" / "5-bps"),
    ("Higher maker fee", SENSITIVITY / "fees" / "10-bps"),
    ("Proportional cancellations", SENSITIVITY / "queue" / "proportional-cancellation"),
)
MIN_FILLS = 10
STRATEGY_LABELS = {
    "fixed-spread": "Fixed spread",
    "avellaneda-stoikov": "Avellaneda–Stoikov",
    "avellaneda-stoikov-imbalance": "Avellaneda–Stoikov + imbalance",
}
DEFINITIONS = {
    "P&L after fees": "Change in marked paper equity. The replay wallet has already paid maker fees.",
    "Excess P&L vs hold": "Ending paper equity minus the value of the untouched starting wallet at the same midpoint.",
    "Time quoted": "Time with at least one activated order and a valid midpoint. Pending cancellations count until they settle. The percentage uses valid-midpoint time.",
    "Inventory variance": "Time-weighted population variance of BTC held over the full recording, in BTC².",
    "Signed markouts": "Mean midpoint change from fill time to each horizon in USDT per BTC, positive in the fill direction. Horizons after the recording or across a feed outage are unavailable.",
    "Sharpe": "Unannualized mean simple equity return divided by sample standard deviation. Uses adjacent 1-second equity samples in the same feed period and a zero risk-free rate. Requires at least two returns and nonzero variance.",
    "Max drawdown": "Largest percentage decline from a prior positive equity peak, using 1-second equity samples. Skips unavailable equity marks.",
    "Win rate": "Share of closed FIFO buy/sell matches with strictly positive realized P&L after prorated fees. Each matched lot counts once. Open lots and starting inventory are excluded.",
}


def _capture_window(session: dict) -> tuple[int, int]:
    window = session.get("capture_window")
    if window is not None:
        return window["first_receive_elapsed_ms"], window["last_receive_elapsed_ms"]
    end = 0
    for message in read_recording(_verify_file(session["recording"])):
        end = message.received_elapsed_ms
    return 0, end


def _rank_runs(rows: list[dict], min_fills: int) -> None:
    comparable: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        row["rank"] = None
        if row["fills"] < min_fills:
            row["ranking_status"] = "Too few fills"
        elif row["excess_pnl_usdt"] is None:
            row["ranking_status"] = "P&L unavailable"
        else:
            row["ranking_status"] = "Eligible"
            comparable[row["session_id"]].append(row)

    for group in comparable.values():
        if len(group) < 2:
            continue
        for row in group:
            row["rank"] = 1 + sum(other["excess_pnl_usdt"] > row["excess_pnl_usdt"] for other in group)


def _optional_decimal(value: str | None) -> Decimal | None:
    return Decimal(value) if value is not None else None


def _read_case(label: str, batch: Path, min_fills: int) -> dict:
    batch = batch.resolve()
    config = _read_json(batch / "config.json")
    summary = _read_json(batch / "summary.json")
    dataset = _read_json(_verify_file(config["dataset_manifest"]))
    if summary["dataset_id"] != config["dataset_id"] or summary["split"] != config["split"]:
        raise ValueError(f"Benchmark summary and config disagree in {batch}")
    windows = {
        session["id"]: _capture_window(session) for session in dataset["sessions"] if session["id"] in config["session_ids"]
    }
    starting_btc = Decimal(config["settings"]["starting_btc"])
    starting_usdt = Decimal(config["settings"]["starting_usdt"])
    rows = []
    for run in summary["runs"]:
        audit = (batch / run["directory"] / "audit.jsonl").resolve()
        if not audit.is_relative_to(batch) or _sha256(audit) != run["audit_sha256"]:
            raise ValueError(f"Audit does not match the saved benchmark: {audit}")
        start, end = windows[run["session_id"]]
        with audit.open(encoding="utf-8") as file:
            metrics = measure_audit(
                (json.loads(line) for line in file), start_ms=start, end_ms=end,
                starting_btc=starting_btc, starting_usdt=starting_usdt,
            )
        if metrics["fills"] != run["fills"] or metrics["filled_volume_btc"] != Decimal(run["filled_volume_btc"]):
            raise ValueError(f"Audit fills do not match the saved benchmark: {audit}")
        rows.append({
            "session_id": run["session_id"],
            "strategy": run["strategy"],
            "net_pnl_usdt": _optional_decimal(run["net_pnl_usdt"]),
            "excess_pnl_usdt": _optional_decimal(run["excess_pnl_usdt"]),
            "fees_usdt": Decimal(run["fees_usdt"]),
            "orders_submitted": run["orders_submitted"],
            "audit_sha256": run["audit_sha256"],
            **metrics,
        })
    _rank_runs(rows, min_fills)
    return {
        "label": label,
        "source_batch": (batch.relative_to(PROJECT) if batch.is_relative_to(PROJECT) else batch).as_posix(),
        "dataset_manifest": config["dataset_manifest"],
        "dataset_id": config["dataset_id"],
        "split": config["split"],
        "settings": config["settings"],
        "execution": config["execution"],
        "runs": rows,
    }


def _number(value: Decimal | int | None, *, signed: bool = False) -> str:
    if value is None:
        return "—"
    if not value:
        return "0"
    formatted = f"{value:,.8f}".rstrip("0").rstrip(".")
    if formatted in ("0", "-0"):
        formatted = f"{value:.3E}"
    return "+" + formatted if signed and value > 0 else formatted


def _supporting_cell(value: Decimal | None, label: str, note: str, *, percent: bool = False) -> str:
    formatted = "N/A" if value is None else _number(value) + ("%" if percent else "")
    return f'<span title="{escape(DEFINITIONS[label])}">{formatted}</span><small>{escape(note)}</small>'


def _render_row(row: dict) -> str:
    quoted_pct = row["time_quoted_pct"]
    coverage = f"{quoted_pct:.1f}%" if quoted_pct is not None else "—"
    cells = [
        escape(row["session_id"]),
        escape(STRATEGY_LABELS.get(row["strategy"], row["strategy"])),
        _number(row["net_pnl_usdt"], signed=True),
        _number(row["excess_pnl_usdt"], signed=True),
        _supporting_cell(
            row["sharpe"], "Sharpe",
            f'1-second returns · unannualized · n={row["sharpe_return_samples"]}',
        ),
        _supporting_cell(row["max_drawdown_pct"], "Max drawdown", "1-second equity · peak to trough", percent=True),
        _supporting_cell(
            row["win_rate_pct"], "Win rate",
            f'{row["winning_round_trips"]}/{row["closed_round_trips"]} closed FIFO matches profitable after fees',
            percent=True,
        ),
        f'{_number(Decimal(row["time_quoted_ms"]) / 1000)} s'
        f'<small>{coverage}</small>',
        _number(row["filled_volume_btc"]),
        _number(row["inventory_variance_btc2"]),
    ]
    for markout in row["markouts"].values():
        mean = _number(markout["mean_usdt_per_btc"], signed=True)
        cells.append(f'{mean}<small>n={markout["samples"]}</small>')
    cells.append(str(row["fills"]))
    status = f'Rank {row["rank"]}' if row["rank"] is not None else escape(row["ranking_status"])
    cells.append(f'<span class="rank">{status}</span>')
    return "<tr>" + "".join(f"<td>{cell}</td>" for cell in cells) + "</tr>"


def _render_case(case: dict, *, opened: bool) -> str:
    rows = case["runs"]
    fee = _number(Decimal(case["settings"]["maker_fee_rate"]) * 10000)
    queue = escape(case["execution"]["queue_model"])
    headers = (
        "Session", "Strategy", "P&amp;L after fees<br>USDT", "Excess vs hold<br>USDT",
        "Sharpe", "Max drawdown", "Win rate", "Time quoted",
        "Filled volume<br>BTC", "Inventory variance<br>BTC²", "1s", "5s", "30s", "60s", "Fills", "Ranking",
    )
    return f'''
<details{" open" if opened else ""} class="panel">
  <summary><span>{escape(case["label"])}</span><small>{fee} bps maker fee · {queue}</small></summary>
  <div class="table-scroll" tabindex="0" role="region" aria-label="{escape(case["label"])} results">
    <table>
      <thead><tr class="groups"><th colspan="2"></th><th colspan="2">Performance</th>
        <th colspan="3">Supporting metrics</th>
        <th colspan="3">Execution</th><th colspan="4">Signed markouts · USDT/BTC</th><th colspan="2">Coverage</th></tr>
      <tr>{"".join(f'<th scope="col">{title}</th>' for title in headers)}</tr></thead>
      <tbody>{"".join(_render_row(row) for row in rows)}</tbody>
    </table>
  </div>
</details>'''


def _render_html(report: dict) -> str:
    cases = report["cases"]
    rows = [row for case in cases for row in case["runs"]]
    dataset_label = escape(", ".join(dict.fromkeys(case["dataset_id"] for case in cases)))
    metric_notes = "".join(f"<dt>{escape(term)}</dt><dd>{escape(text)}</dd>" for term, text in DEFINITIONS.items())
    style = (PACKAGE / "static" / "report.css").read_text(encoding="utf-8")
    return f'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC/USDT strategy comparison</title>
<style>{style}</style>
</head>
<body>
<main>
  <header><h1>BTC/USDT strategy comparison</h1><p>{dataset_label}</p></header>
  <dl class="totals">
    <div><dt>Runs</dt><dd>{len(rows)}</dd></div>
    <div><dt>Fill events across runs</dt><dd>{sum(row["fills"] for row in rows)}</dd></div>
    <div><dt>Eligible for ranking</dt><dd>{sum(row["ranking_status"] == "Eligible" for row in rows)}</dd></div>
  </dl>
  <p class="policy">A run needs at least {report["minimum_fills"]} fills to qualify. Within each scenario and session, eligible strategies rank by excess P&amp;L versus hold.</p>
  {"".join(_render_case(case, opened=index == 0) for index, case in enumerate(cases))}
  <section class="panel notes"><h2>Metric definitions</h2><dl>{metric_notes}</dl></section>
</main>
</body>
</html>
'''


def build_report(cases: list[tuple[str, Path]], output: Path, min_fills: int = MIN_FILLS) -> dict:
    if min_fills < 1:
        raise ValueError("Minimum fills must be at least one")
    report = {
        "schema_version": 1,
        "minimum_fills": min_fills,
        "markout_horizons_ms": MARKOUT_HORIZONS_MS,
        "equity_sample_ms": EQUITY_SAMPLE_MS,
        "definitions": DEFINITIONS,
        "cases": [_read_case(label, batch, min_fills) for label, batch in cases],
    }
    html = _render_html(report)
    output.mkdir(parents=True)
    _write_json(output / "metrics.json", report)
    (output / "comparison.html").write_text(html, encoding="utf-8", newline="\n")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", nargs=2, metavar=("LABEL", "BATCH"),
                        help="a labeled benchmark directory; repeat for additional scenarios")
    parser.add_argument("--min-fills", type=int, default=MIN_FILLS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="new output directory")
    args = parser.parse_args(argv)
    try:
        cases = [(label, Path(path)) for label, path in args.case] if args.case else list(DEFAULT_CASES)
        report = build_report(cases, args.output, args.min_fills)
    except (InvalidOperation, KeyError, OSError, TypeError, ValueError) as error:
        parser.error(str(error))
    total = sum(len(case["runs"]) for case in report["cases"])
    print(f"Reported {total} runs across {len(report['cases'])} cases in {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
