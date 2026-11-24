"""Run the three paper strategies on identical held-out Binance sessions."""

import argparse
import json
import re
from collections import Counter
from dataclasses import asdict, dataclass, replace
from decimal import Decimal, InvalidOperation
from hashlib import file_digest, sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from .binance_replay import load_binance_replay
from .parsing import read_decimal, read_integer
from .quote_filters import load_binance_quote_filters
from .queue_model import CANCELLATION_WINDOW_MS, QUEUE_MODELS, QueueModel
from .recording import RECORDING_VERSION
from .replay import DEFAULT_LATENCY_MS, DEFAULT_STARTING_BTC, DEFAULT_STARTING_USDT, ReplayResult, run_replay
from .risk import INVENTORY_CAP_FRACTION
from .strategy import (
    AS_HORIZON_SECONDS,
    AS_MODEL_LOT_BTC,
    DEFAULT_QUOTE_SIZE_BTC,
    DEFAULT_RISK_AVERSION,
    IMBALANCE_DEPTH_LEVELS,
    MAX_IMBALANCE_TICKS,
    STRATEGY_TYPES,
    AvellanedaStoikovImbalanceStrategy,
    AvellanedaStoikovStrategy,
    FixedSpreadStrategy,
    create_strategy,
)
from .volatility import SAMPLE_INTERVAL_MS, VOLATILITY_WINDOW_SECONDS
from .wallet import DEFAULT_MAKER_FEE_RATE, Wallet


PACKAGE = Path(__file__).resolve().parent
PROJECT = PACKAGE.parent
DEFAULT_MANIFEST = PROJECT / "data" / "milestone-5" / "manifest.json"
DEFAULT_OUTPUT = PROJECT / "runs" / "milestone-5" / "base"
SCHEMA_VERSION = 1
SOURCES = (
    "benchmark.py", "binance_book.py", "binance_replay.py", "execution.py", "models.py", "order_book.py",
    "parsing.py", "queue_model.py", "quote_filters.py", "recording.py", "replay.py", "risk.py", "strategy.py", "volatility.py",
    "wallet.py",
)


@dataclass(frozen=True, slots=True)
class BenchmarkSettings:
    starting_btc: Decimal = DEFAULT_STARTING_BTC
    starting_usdt: Decimal = DEFAULT_STARTING_USDT
    target_btc: Decimal = DEFAULT_STARTING_BTC
    spread_usdt: Decimal = Decimal("120")
    quote_size_btc: Decimal = DEFAULT_QUOTE_SIZE_BTC
    maker_fee_rate: Decimal = DEFAULT_MAKER_FEE_RATE
    latency_ms: int = DEFAULT_LATENCY_MS
    risk_aversion: Decimal = DEFAULT_RISK_AVERSION
    fee_guard: bool = True
    queue_model: QueueModel = "trade-only"

    @classmethod
    def from_json(cls, data: dict) -> "BenchmarkSettings":
        fee_guard = data["fee_guard"]
        if not isinstance(fee_guard, bool):
            raise ValueError("fee_guard must be a JSON boolean")
        queue_model = data.get("queue_model", "trade-only")
        if queue_model not in QUEUE_MODELS:
            raise ValueError(f"Unknown queue model: {queue_model!r}")
        return cls(
            starting_btc=read_decimal(data["starting_btc"], "starting_btc", allow_zero=True),
            starting_usdt=read_decimal(data["starting_usdt"], "starting_usdt", allow_zero=True),
            target_btc=read_decimal(data["target_btc"], "target_btc", allow_zero=True),
            spread_usdt=read_decimal(data["spread_usdt"], "spread_usdt"),
            quote_size_btc=read_decimal(data["quote_size_btc"], "quote_size_btc"),
            maker_fee_rate=read_decimal(data["maker_fee_rate"], "maker_fee_rate", allow_zero=True),
            latency_ms=read_integer(data["latency_ms"], "latency_ms"),
            risk_aversion=read_decimal(data["risk_aversion"], "risk_aversion"),
            fee_guard=fee_guard,
            queue_model=queue_model,
        )


def _read_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return data


def _write_json(path: Path, data: dict) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as file:
        json.dump(data, file, default=str, indent=2)
        file.write("\n")


def _sha256(path: Path) -> str:
    with path.open("rb") as file:
        return file_digest(file, "sha256").hexdigest()


def _manifest_reference(path: Path) -> dict:
    path = path.resolve()
    label = path.relative_to(PROJECT) if path.is_relative_to(PROJECT) else path
    return {"path": label.as_posix(), "sha256": _sha256(path)}


def _verify_file(reference: dict) -> Path:
    path = PROJECT / reference["path"]
    if ("bytes" in reference and path.stat().st_size != reference["bytes"]) or _sha256(path) != reference["sha256"]:
        raise ValueError(f"Dataset file does not match its manifest: {path}")
    return path


def _held_out_sessions(manifest: dict) -> list[dict]:
    if (
        manifest["schema_version"] != SCHEMA_VERSION
        or manifest["status"] != "complete"
        or manifest["symbol"] != "BTCUSDT"
        or manifest["recording_format_version"] != RECORDING_VERSION
    ):
        raise ValueError("Expected a complete BTCUSDT recording manifest using the supported schema")
    sessions = manifest["sessions"]
    assignments = {session["id"]: session["split"] for session in sessions}
    if len(assignments) != len(sessions):
        raise ValueError("Duplicate session ID in the dataset manifest")
    plan = manifest.get("capture_plan")
    if plan is not None and assignments != {entry["id"]: entry["split"] for entry in plan}:
        raise ValueError("Session assignments differ from the capture plan")
    held_out = [session for session in sessions if session["split"] == "held_out"]
    if not held_out:
        raise ValueError("The manifest has no held-out sessions")
    for session in held_out:
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", session["id"]):
            raise ValueError(f"Invalid session ID: {session['id']!r}")
    return held_out


def _write_audit(path: Path, result: ReplayResult) -> str:
    with path.open("x", encoding="utf-8", newline="\n") as file:
        for event in result.audit_log:
            row = {"timestamp_ms": event.timestamp_ms, "kind": event.kind, "details": dict(event.details)}
            file.write(json.dumps(row, default=str, separators=(",", ":")) + "\n")
    return _sha256(path)


def _result_summary(result: ReplayResult) -> dict:
    counts = Counter(event.kind for event in result.audit_log)
    filled_volume = sum(
        (dict(event.details)["fill_quantity"] for event in result.audit_log if event.kind == "FILL"),
        Decimal("0"),
    )
    return {
        "audit_events": len(result.audit_log),
        "orders_submitted": counts["ORDER_SUBMITTED"],
        "orders_canceled": counts["ORDER_CANCELED"],
        "orders_rejected": counts["ORDER_REJECTED"],
        "orders_skipped": counts["ORDER_SKIPPED"],
        "fills": counts["FILL"],
        "filled_volume_btc": filled_volume,
        "fees_usdt": result.fees_usdt,
        "net_pnl_usdt": result.net_pnl_usdt,
        "excess_pnl_usdt": result.excess_pnl_usdt,
        "pending_markouts": result.pending_markouts,
        "final_balances": asdict(result.balances),
    }


def _strategy_parameters(strategy: FixedSpreadStrategy) -> dict:
    parameters = asdict(strategy)
    if isinstance(strategy, AvellanedaStoikovStrategy):
        parameters.update(
            horizon_seconds=AS_HORIZON_SECONDS, model_lot_btc=AS_MODEL_LOT_BTC,
            midpoint_sample_interval_ms=SAMPLE_INTERVAL_MS, volatility_window_seconds=VOLATILITY_WINDOW_SECONDS,
        )
    if isinstance(strategy, AvellanedaStoikovImbalanceStrategy):
        parameters.update(imbalance_depth_levels=IMBALANCE_DEPTH_LEVELS, imbalance_limit_ticks=MAX_IMBALANCE_TICKS)
    return parameters


def run_benchmark(manifest_path: Path, output: Path, settings: BenchmarkSettings) -> list[dict]:
    """Verify inputs and publish a complete batch only after all replays finish."""
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"Output already exists: {output}. Choose another --output directory.")
    manifest = _read_json(manifest_path)
    held_out = _held_out_sessions(manifest)
    files = [
        (_verify_file(session["recording"]), _verify_file(session["market_filters"]))
        for session in held_out
    ]
    versions = {name: sha256((PACKAGE / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for name in SOURCES}
    config = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": manifest["dataset_id"],
        "dataset_manifest": _manifest_reference(manifest_path),
        "split": "held_out",
        "session_ids": [session["id"] for session in held_out],
        "strategies": list(STRATEGY_TYPES),
        "settings": asdict(settings),
        "execution": {
            "queue_model": settings.queue_model,
            "post_only": True,
            "inventory_cap_fraction": INVENTORY_CAP_FRACTION,
        },
        "source_sha256": versions,
    }
    if settings.queue_model == "proportional-cancellation":
        config["execution"]["cancellation_window_each_side_ms"] = CANCELLATION_WINDOW_MS
    rows = []
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output.parent, prefix=".benchmark-") as staging:
        batch = Path(staging) / "batch"
        batch.mkdir()
        _write_json(batch / "config.json", config)
        for session, (recording_path, filter_path) in zip(held_out, files, strict=True):
            events = load_binance_replay(recording_path)
            filters = load_binance_quote_filters(filter_path)
            for name, strategy_type in STRATEGY_TYPES.items():
                strategy = create_strategy(
                    name, settings.spread_usdt, settings.quote_size_btc,
                    risk_aversion=settings.risk_aversion if issubclass(strategy_type, AvellanedaStoikovStrategy) else None,
                )
                wallet = Wallet(
                    btc=settings.starting_btc, usdt=settings.starting_usdt, maker_fee_rate=settings.maker_fee_rate,
                )
                result = run_replay(
                    events, wallet, latency_ms=settings.latency_ms, target_btc=settings.target_btc,
                    strategy=strategy, quote_filters=filters, fee_guard=settings.fee_guard,
                    queue_model=settings.queue_model,
                )
                relative_run = Path(session["id"]) / name
                run_path = batch / relative_run
                run_path.mkdir(parents=True)
                _write_json(run_path / "config.json", {
                    "schema_version": SCHEMA_VERSION,
                    "dataset_id": config["dataset_id"],
                    "dataset_manifest": config["dataset_manifest"],
                    "session_id": session["id"],
                    "split": config["split"],
                    "recording": session["recording"],
                    "market_filters": session["market_filters"],
                    "strategy": name,
                    "strategy_source_sha256": versions["strategy.py"],
                    "strategy_parameters": _strategy_parameters(strategy),
                    "settings": config["settings"],
                    "execution": config["execution"],
                    "source_sha256": versions,
                })
                audit_hash = _write_audit(run_path / "audit.jsonl", result)
                summary = _result_summary(result)
                _write_json(run_path / "result.json", {**summary, "audit_sha256": audit_hash})
                rows.append({
                    "session_id": session["id"], "strategy": name, "directory": relative_run.as_posix(),
                    **summary, "audit_sha256": audit_hash,
                })
        _write_json(batch / "summary.json", {"dataset_id": config["dataset_id"], "split": "held_out", "runs": rows})
        batch.rename(output)
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--manifest", type=Path, help="capture manifest (defaults to the Milestone 5 dataset)")
    source.add_argument("--config", type=Path, help="config.json from a saved benchmark batch")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="new directory for the benchmark artifacts")
    parser.add_argument("--maker-fee", type=Decimal, help="override the saved decimal maker fee rate")
    parser.add_argument("--queue-model", choices=QUEUE_MODELS, help="override the saved fill queue model")
    args = parser.parse_args(argv)
    try:
        if args.config is None:
            manifest_path = args.manifest or DEFAULT_MANIFEST
            settings = BenchmarkSettings()
        else:
            saved = _read_json(args.config)
            if saved["schema_version"] != SCHEMA_VERSION or saved["split"] != "held_out":
                raise ValueError("Expected a supported held-out benchmark configuration")
            manifest_path = _verify_file(saved["dataset_manifest"])
            settings = BenchmarkSettings.from_json(saved["settings"])
        if args.maker_fee is not None:
            settings = replace(settings, maker_fee_rate=args.maker_fee)
        if args.queue_model is not None:
            settings = replace(settings, queue_model=args.queue_model)
        rows = run_benchmark(manifest_path, args.output, settings)
    except (InvalidOperation, KeyError, OSError, TypeError, ValueError) as error:
        parser.error(str(error))
    for row in rows:
        print(f"{row['session_id']} {row['strategy']} orders={row['orders_submitted']} "
              f"fills={row['fills']} volume_btc={row['filled_volume_btc']} net_pnl_usdt={row['net_pnl_usdt']}")
    print(f"Saved {len(rows)} held-out runs to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
