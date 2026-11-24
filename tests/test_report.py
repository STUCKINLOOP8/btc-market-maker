import unittest
from decimal import Decimal

from market_maker.analytics import measure_audit
from market_maker.replay import DEFAULT_FIXTURE, load_fixture, run_replay
from market_maker.report import _rank_runs, _render_row
from market_maker.wallet import Wallet


D = Decimal


def event(timestamp_ms, kind, **details):
    return {"timestamp_ms": timestamp_ms, "kind": kind, "details": details}


class ReportMetricsTests(unittest.TestCase):
    def measure(self, entries, end_ms):
        return measure_audit(entries, start_ms=0, end_ms=end_ms, starting_btc=D("0.50"))

    def test_time_quoted_counts_overlaps_once_and_keeps_cancel_pending_time(self):
        metrics = self.measure([
            event(0, "BOOK", mid_price="100"),
            event(10, "ORDER_ACTIVATED", order_id="bid"),
            event(20, "FILL", order_id="bid", side="buy", fill_quantity="0.05", btc="0.55", status="PARTIALLY_FILLED"),
            event(25, "ORDER_ACTIVATED", order_id="ask"),
            event(35, "CANCEL_REQUESTED", order_id="bid"),
            event(40, "ORDER_CANCELED", order_id="bid"),
            event(50, "FILL", order_id="ask", side="sell", fill_quantity="0.05", btc="0.50", status="FILLED"),
            event(60, "FEED_UNAVAILABLE"),
            event(70, "BOOK", mid_price="100"),
            event(80, "ORDER_ACTIVATED", order_id="bid-2"),
        ], 100)
        self.assertEqual(metrics["time_quoted_ms"], 60)
        self.assertEqual(metrics["valid_midpoint_ms"], 90)
        self.assertEqual(metrics["time_quoted_pct"], D("6000") / 90)
        self.assertEqual(metrics["filled_volume_btc"], D("0.10"))
        self.assertEqual(metrics["inventory_variance_btc2"], D("0.000525"))

    def test_signed_markouts_use_last_midpoint_at_each_horizon(self):
        metrics = self.measure([
            event(0, "BOOK", mid_price="100"),
            event(0, "FILL", order_id="bid", side="buy", fill_quantity="0.05", btc="0.55", status="FILLED"),
            event(1000, "BOOK", mid_price="101"),
            event(1000, "BOOK", mid_price="102"),
            event(5000, "BOOK", mid_price="104"),
            event(10000, "FILL", order_id="ask", side="sell", fill_quantity="0.05", btc="0.50", status="FILLED"),
            event(11000, "BOOK", mid_price="103"),
            event(15000, "BOOK", mid_price="100"),
            event(30000, "BOOK", mid_price="106"),
            event(40000, "BOOK", mid_price="100"),
            event(60000, "BOOK", mid_price="110"),
        ], 60000)
        samples = metrics["markouts"]
        for horizon, mean in (("1s", "1.5"), ("5s", "4"), ("30s", "5"), ("60s", "10")):
            self.assertEqual(samples[horizon]["mean_usdt_per_btc"], D(mean))
        self.assertEqual([entry["samples"] for entry in samples.values()], [2, 2, 2, 1])
        self.assertEqual(samples["60s"]["unavailable"], 1)

    def test_markouts_do_not_cross_feed_outages_and_missing_samples_are_blank(self):
        metrics = self.measure([
            event(0, "BOOK", mid_price="100"),
            event(0, "FILL", order_id="bid", side="buy", fill_quantity="0.05", btc="0.55", status="FILLED"),
            event(1000, "BOOK", mid_price="101"),
            event(2000, "FEED_UNAVAILABLE"),
            event(3000, "BOOK", mid_price="130"),
        ], 60000)
        self.assertEqual(metrics["markouts"]["1s"]["mean_usdt_per_btc"], 1)
        for horizon in ("5s", "30s", "60s"):
            self.assertEqual(metrics["markouts"][horizon], {"mean_usdt_per_btc": None, "samples": 0, "unavailable": 1})

    def test_five_second_markout_matches_the_existing_replay(self):
        tape = load_fixture(DEFAULT_FIXTURE)
        result = run_replay(tape, Wallet(btc=D("0.50"), usdt=D("34225")))
        audit = [event(entry.timestamp_ms, entry.kind, **dict(entry.details)) for entry in result.audit_log]
        engine_values = [entry["details"]["markout_usdt_per_btc"] for entry in audit if entry["kind"] == "MARKOUT_5S"]
        report = self.measure(audit, tape[-1].timestamp_ms)["markouts"]["5s"]
        self.assertEqual(report["samples"], len(engine_values))
        self.assertEqual(report["mean_usdt_per_btc"], sum(engine_values) / len(engine_values))

    def test_ranking_excludes_low_fill_runs_and_compares_only_within_a_session(self):
        rows = [
            {"session_id": "first", "fills": 9, "excess_pnl_usdt": D("100")},
            {"session_id": "first", "fills": 10, "excess_pnl_usdt": D("1")},
            {"session_id": "first", "fills": 11, "excess_pnl_usdt": D("2")},
            {"session_id": "second", "fills": 10, "excess_pnl_usdt": D("5")},
        ]
        _rank_runs(rows, 10)
        self.assertEqual([row["rank"] for row in rows], [None, 2, 1, None])
        self.assertEqual(rows[0]["ranking_status"], "Too few fills")

    def supporting_metrics(self, entries, end_ms, *, btc="1", usdt="0"):
        return measure_audit(entries, start_ms=0, end_ms=end_ms, starting_btc=D(btc), starting_usdt=D(usdt))

    def test_sharpe_uses_sample_standard_deviation_and_last_same_time_mark(self):
        metrics = self.supporting_metrics([
            event(0, "BOOK", mid_price="100"),
            event(1000, "BOOK", mid_price="120"),
            event(1000, "BOOK", mid_price="100"),
            event(2000, "BOOK", mid_price="110"),
        ], 2000)
        # Returns: 0 and 0.1. Mean = 0.05, sample SD = 0.1 / sqrt(2).
        self.assertAlmostEqual(metrics["sharpe"], D("0.5").sqrt(), places=20)
        self.assertEqual(metrics["sharpe_return_samples"], 2)

    def test_drawdown_uses_the_high_water_mark_even_with_no_trades(self):
        metrics = self.supporting_metrics([
            event(0, "BOOK", mid_price="100"),
            event(1000, "BOOK", mid_price="110"),
            event(2000, "BOOK", mid_price="99"),
            event(3000, "BOOK", mid_price="105"),
        ], 3000)
        self.assertEqual(metrics["max_drawdown_pct"], D("10"))
        self.assertEqual(metrics["closed_round_trips"], 0)
        self.assertIsNone(metrics["win_rate_pct"])

    def test_sharpe_is_unavailable_when_returns_have_zero_variance(self):
        metrics = self.supporting_metrics([
            event(0, "BOOK", mid_price="100"),
            event(1000, "BOOK", mid_price="110"),
            event(2000, "BOOK", mid_price="121"),
        ], 2000)
        self.assertEqual(metrics["sharpe_return_samples"], 2)
        self.assertIsNone(metrics["sharpe"])
        self.assertEqual(metrics["max_drawdown_pct"], D("0"))

    def test_sharpe_skips_returns_across_feed_recovery_even_between_samples(self):
        metrics = self.supporting_metrics([
            event(0, "BOOK", mid_price="100"),
            event(1000, "BOOK", mid_price="110"),
            event(1200, "FEED_UNAVAILABLE"),
            event(1500, "BOOK", mid_price="200"),
        ], 3000)
        self.assertEqual(metrics["sharpe_return_samples"], 2)
        self.assertAlmostEqual(metrics["sharpe"], D("0.5").sqrt(), places=20)

    def test_win_rate_matches_partial_lots_fifo_after_fees_and_ignores_open_lots(self):
        metrics = self.supporting_metrics([
            event(0, "BOOK", mid_price="100"),
            event(100, "FILL", order_id="buy", side="buy", fill_quantity="2",
                  price="100", fee_usdt="2", btc="2", usdt="298", status="FILLED"),
            event(200, "FILL", order_id="win", side="sell", fill_quantity="1",
                  price="110", fee_usdt="1", btc="1", usdt="407", status="FILLED"),
            event(300, "FILL", order_id="flat", side="sell", fill_quantity="1",
                  price="102", fee_usdt="1", btc="0", usdt="508", status="FILLED"),
            event(400, "FILL", order_id="open", side="buy", fill_quantity="1",
                  price="100", fee_usdt="1", btc="1", usdt="407", status="FILLED"),
        ], 1000, btc="0", usdt="500")
        # The two matches net +8 and 0 USDT. The final buy is still open.
        self.assertEqual(metrics["closed_round_trips"], 2)
        self.assertEqual(metrics["winning_round_trips"], 1)
        self.assertEqual(metrics["win_rate_pct"], D("50"))

    def test_html_puts_supporting_metric_definitions_beside_the_values(self):
        metrics = self.supporting_metrics([event(0, "BOOK", mid_price="100")], 0)
        row = {
            **metrics, "session_id": "sample", "strategy": "fixed-spread",
            "net_pnl_usdt": D("0"), "excess_pnl_usdt": D("0"), "rank": None, "ranking_status": "Too few fills",
        }
        html = _render_row(row)
        self.assertIn("N/A</span><small>1-second returns · unannualized · n=0</small>", html)
        self.assertIn("0%</span><small>1-second equity · peak to trough</small>", html)
        self.assertIn("N/A</span><small>0/0 closed FIFO matches profitable after fees</small>", html)
