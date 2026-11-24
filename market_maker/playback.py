"""Local FastAPI terminal for recorded and live paper trading."""

import asyncio
import os
import sqlite3
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Literal
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.sse import EventSourceResponse, ServerSentEvent
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .binance_replay import load_binance_replay
from .binance_spot import BinanceSpot
from .exchange import ExchangeSession
from .models import BookSnapshot, ClockAdvance, DepthUpdate, FeedUnavailable
from .quote_filters import BinanceQuoteFilters, load_binance_quote_filters
from .record import LiveMarketEvent, stream_binance
from .replay import DEFAULT_STARTING_BTC, DEFAULT_STARTING_USDT, Portfolio, ReplaySession, load_fixture
from .strategy import DEFAULT_QUOTE_SIZE_BTC, DEFAULT_SPREAD_USDT, StrategyName, create_strategy
from .wallet import Wallet


PROJECT = Path(__file__).resolve().parents[1]
DEFAULT_PORT = 8080
BOOK_LEVELS = 20
STREAM_BATCH_SIZE = 256
DECIMAL_ENCODERS = {Decimal: str}
LIVE_TICK_SECONDS = 0.05
PositiveDecimal = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]


class QuoteSettings(BaseModel):
    spread_usdt: PositiveDecimal = DEFAULT_SPREAD_USDT
    quote_size_btc: PositiveDecimal = DEFAULT_QUOTE_SIZE_BTC


class LoadRecording(QuoteSettings):
    recording: str = Field(min_length=1)
    orders: str | None = Field(default=None, min_length=1)
    strategy: StrategyName | None = None


class StartLive(QuoteSettings):
    strategy: StrategyName = "fixed-spread"
    spread_usdt: PositiveDecimal = Decimal("250")


class StartExchange(StartLive):
    environment: Literal["testnet"]
    confirmation: str
    max_order_usdt: PositiveDecimal
    max_drawdown_usdt: PositiveDecimal


class PlaybackSpeed(BaseModel):
    speed: float = Field(ge=0.1, le=100, allow_inf_nan=False, strict=True)


@dataclass(frozen=True, slots=True)
class EquityPoint:
    timestamp_ms: int
    equity_usdt: Decimal | None
    hold_equity_usdt: Decimal | None


class Playback:
    """Own one paper session and its wall clock on the server's event loop."""

    def __init__(self) -> None:
        self.session: ReplaySession | None = None
        self.status = "empty"
        self.speed = 1.0
        self.changed = asyncio.Event()
        self.run_id: str | None = None
        self.source: LoadRecording | StartLive | None = None
        self.timestamp_ms: int | None = None
        self.position = 0
        self.error: str | None = None
        self._book: BookSnapshot | None = None
        self._portfolio: Portfolio | None = None
        self._equity_history: list[EquityPoint] = []
        self._remaining_ms = 0.0
        self._due_at: float | None = None
        self._task: asyncio.Task[None] | None = None
        self._live_task: asyncio.Task[None] | None = None
        self._live_pending: deque[LiveMarketEvent] = deque()
        self._live_started_at = 0.0
        self._live_unavailable: FeedUnavailable | None = None
        self._last_depth_ms: int | None = None
        self._last_fill: dict | None = None

    @property
    def is_live(self) -> bool:
        return isinstance(self.source, StartLive)

    @property
    def can_replace(self) -> bool:
        return self.status not in ("live", "halting")

    def replace(self, source: LoadRecording | StartLive, session: ReplaySession) -> None:
        self._stop_clock()
        if self._live_task is not None:
            self._live_task.cancel()
            self._live_task = None
        self.session = session
        self.source = source
        self.run_id = uuid4().hex
        self.status = "live" if self.is_live else "paused"
        self.timestamp_ms = None
        self.position = 0
        self.error = None
        self._book = None
        self._portfolio = session.portfolio
        self._equity_history.clear()
        self._remaining_ms = 0.0
        self._publish()

    def start_live(self, source: StartLive, filters: BinanceQuoteFilters) -> None:
        strategy = create_strategy(source.strategy, source.spread_usdt, source.quote_size_btc)
        wallet = Wallet(btc=DEFAULT_STARTING_BTC, usdt=DEFAULT_STARTING_USDT)
        self.speed = 1.0
        self._live_started_at = asyncio.get_running_loop().time()
        self._live_pending = deque()
        self._live_unavailable = None
        self._last_depth_ms = None
        self._last_fill = None
        self.replace(source, ReplaySession((), wallet, strategy=strategy, quote_filters=filters, live=True))
        self._live_task = asyncio.create_task(self._run_live())

    def state(self) -> dict:
        session = self.session
        balances = None
        orders = []
        if session is not None:
            wallet = session.balances
            balances = {
                "btc": wallet.btc,
                "usdt": wallet.usdt,
                "reserved_btc": wallet.reserved_btc,
                "reserved_usdt": wallet.reserved_usdt,
                "available_btc": wallet.available_btc,
                "available_usdt": wallet.available_usdt,
            }
            orders = [
                {
                    **jsonable_encoder(order, custom_encoder=DECIMAL_ENCODERS),
                    "queue_ahead": session.queue_ahead(order.order_id),
                }
                for order in session.orders
            ]
        return jsonable_encoder({
            "mode": "live" if self.is_live else "replay",
            "run_id": self.run_id,
            "source": self.source,
            "status": self.status,
            "speed": self.speed,
            "timestamp_ms": self.timestamp_ms,
            "next_timestamp_ms": session.next_timestamp_ms if session is not None else None,
            "position": self.position,
            "unresolved_order_count": session.unresolved_order_count if session is not None else 0,
            "audit_count": session.audit_count if session is not None else 0,
            "book": self._book,
            "orders": orders,
            "balances": balances,
            "starting_balances": session.starting_balances if session is not None else None,
            "portfolio": self._portfolio,
            "equity_point_count": len(self._equity_history),
            "feed": self._feed_state() if self.is_live else None,
            "last_fill": self._last_fill if self.is_live else None,
            "error": self.error,
        }, custom_encoder=DECIMAL_ENCODERS)

    def _feed_state(self) -> dict:
        unavailable = self._live_unavailable
        reason = unavailable.reason if unavailable is not None else None
        if self.status in ("halted", "failed"):
            return {"health": "stopped", "age_ms": None, "reason": None, "detail": None}
        if reason is not None and reason != "awaiting_snapshot":
            health = "unavailable"
        else:
            health = "ready" if self._last_depth_ms is not None else "syncing"
        return {
            "health": health,
            "age_ms": self._live_elapsed_ms() - self._last_depth_ms if self._last_depth_ms is not None else None,
            "reason": reason,
            "detail": unavailable.detail if unavailable is not None else None,
        }

    def equity_since(self, offset: int) -> list[EquityPoint]:
        return self._equity_history[offset:offset + STREAM_BATCH_SIZE]

    def play(self) -> None:
        self._require_input()
        if self.status != "playing":
            self.status = "playing"
            self._start_clock()
            self._publish()

    def pause(self) -> None:
        self._require_replay()
        if self.status == "playing":
            self._stop_clock()
            self.status = "paused"
            self._publish()

    def step(self) -> None:
        if self.status == "playing":
            raise HTTPException(409, "Pause playback before stepping")
        self._require_input()
        self._advance()

    def halt(self) -> None:
        if self.session is None:
            raise HTTPException(409, "Start a paper session before halting")
        if self.status in ("halting", "halted"):
            return
        if self.status == "live":
            self._drain_live()
        self._stop_clock()
        self.timestamp_ms = self.session.halt()
        self._portfolio = self.session.portfolio
        next_time = self.session.next_timestamp_ms
        if next_time is None:
            self.status = "halted"
        else:
            self.status = "halting"
            self._remaining_ms = next_time - self.timestamp_ms
            if not self.is_live or self._live_task is None or self._live_task.done():
                self._start_clock()
        self._publish()

    def set_speed(self, speed: float) -> None:
        self._require_replay()
        if self.speed == speed:
            return
        running = self._task is not None
        if running:
            self._stop_clock()
        self.speed = speed
        if running:
            self._start_clock()
        self._publish()

    async def close(self) -> None:
        tasks = [task for task in (self._stop_clock(), self._live_task) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _require_input(self) -> None:
        self._require_replay()
        if self.status not in ("paused", "playing"):
            raise HTTPException(409, "Load a recording with events left to replay")

    def _require_replay(self) -> None:
        if self.is_live:
            raise HTTPException(409, "Playback controls apply to recordings")

    def _publish(self) -> None:
        self.changed.set()
        self.changed = asyncio.Event()

    def _start_clock(self) -> None:
        self._due_at = asyncio.get_running_loop().time() + self._delay_seconds()
        self._task = asyncio.create_task(self._play())

    def _stop_clock(self) -> asyncio.Task[None] | None:
        task = self._task
        if task is not None:
            assert self._due_at is not None
            self._remaining_ms = max(0, self._due_at - asyncio.get_running_loop().time()) * self.speed * 1000
            task.cancel()
            self._task = None
            self._due_at = None
        return task

    def _delay_seconds(self) -> float:
        return self._remaining_ms / (self.speed * 1000)

    async def _play(self) -> None:
        loop = asyncio.get_running_loop()
        while self._due_at is not None:
            due_at = self._due_at
            await asyncio.sleep(max(0, due_at - loop.time()))
            self._advance()
            if self.status not in ("playing", "halting"):
                self._task = None
                self._due_at = None
                return
            self._due_at = due_at + self._delay_seconds()

    def _live_elapsed_ms(self) -> int:
        return int((asyncio.get_running_loop().time() - self._live_started_at) * 1000)

    async def _run_live(self) -> None:
        feed = asyncio.create_task(stream_binance(self._live_pending.append, started_at=self._live_started_at))
        try:
            while self.status in ("live", "halting"):
                await asyncio.sleep(LIVE_TICK_SECONDS)
                if self.status == "live" and feed.done():
                    self.error = str(feed.exception() or "Binance feed stopped")
                    self.halt()
                if self.status in ("live", "halting"):
                    self._drain_live()
                    if self.status == "failed":
                        self.halt()
                    self._publish()
        finally:
            feed.cancel()
            await asyncio.gather(feed, return_exceptions=True)

    def _drain_live(self) -> None:
        assert self.session is not None
        # Leave the current millisecond open until all same-time frames can be ordered.
        through = self._live_elapsed_ms() - 1
        events: list[LiveMarketEvent | ClockAdvance] = []
        while self._live_pending and self._live_pending[0].timestamp_ms <= through:
            events.append(self._live_pending.popleft())
        latest_ms = events[-1].timestamp_ms if events else self.timestamp_ms
        if latest_ms is None or latest_ms < through:
            if through >= 0:
                events.append(ClockAdvance(through, 0))
        self.session.append_live_events(events)
        while self.status in ("live", "halting"):
            next_time = self.session.next_timestamp_ms
            if next_time is None or next_time > through:
                break
            self._advance(publish=False)

    def _advance(self, *, publish: bool = True) -> None:
        assert self.session is not None
        try:
            step = self.session.step()
        except (ValueError, InvalidOperation) as error:
            self.status = "failed"
            self.error = str(error)
            self._portfolio = self.session.portfolio
        else:
            assert step is not None
            event = step.event
            self.timestamp_ms = event.timestamp_ms
            if not isinstance(event, ClockAdvance):
                self.position += 1
            if isinstance(event, (BookSnapshot, DepthUpdate, FeedUnavailable)):
                self._book = self.session.book_snapshot(limit=BOOK_LEVELS)
                if self.is_live:
                    if isinstance(event, FeedUnavailable):
                        self._live_unavailable = event
                    else:
                        self._last_depth_ms = event.timestamp_ms
                        self._live_unavailable = None
            if self.is_live:
                for audit in step.audit_log:
                    if audit.kind == "FILL":
                        self._last_fill = {"timestamp_ms": audit.timestamp_ms, **dict(audit.details)}
            self._portfolio = self.session.portfolio
            self._record_equity(event.timestamp_ms, self._portfolio)
            next_time = self.session.next_timestamp_ms
            if next_time is None:
                if self.status == "halting":
                    self.status = "halted"
                elif not self.is_live:
                    self.status = "finished"
            else:
                self._remaining_ms = next_time - event.timestamp_ms
        if publish:
            self._publish()

    def _record_equity(self, timestamp_ms: int, portfolio: Portfolio) -> None:
        equity, hold = portfolio.equity_usdt, portfolio.hold_equity_usdt
        if self._equity_history:
            last = self._equity_history[-1]
            if equity == last.equity_usdt and hold == last.hold_equity_usdt:
                return
        elif equity is None:
            return
        self._equity_history.append(EquityPoint(timestamp_ms, equity, hold))


def _project_file(root: Path, name: str, suffix: str) -> Path:
    path = (root / name).resolve()
    if not path.is_relative_to(root) or path.suffix.lower() != suffix:
        raise ValueError(f"Expected a {suffix} file inside the project folder")
    if not path.is_file():
        raise ValueError(f"File not found: {name}")
    return path


def _load_session(root: Path, source: LoadRecording) -> ReplaySession:
    if source.strategy is not None and source.orders is not None:
        raise ValueError("Choose a strategy or an order script")
    recording = _project_file(root, source.recording, ".jsonl")
    orders = load_fixture(_project_file(root, source.orders, ".json")) if source.orders is not None else ()
    events = load_binance_replay(recording, orders)
    wallet = Wallet(btc=DEFAULT_STARTING_BTC, usdt=DEFAULT_STARTING_USDT)
    strategy = (
        create_strategy(source.strategy, source.spread_usdt, source.quote_size_btc)
        if source.strategy is not None else None
    )
    return ReplaySession(events, wallet, strategy=strategy)


def _cursor_offset(value: str) -> int:
    return int(value) if value.isascii() and value.isdecimal() and len(value) <= 16 else 0


def _require_local(request: Request) -> None:
    loopback = {"127.0.0.1", "::1", "localhost"}
    if request.client is None or request.client.host not in loopback or request.url.hostname not in loopback:
        raise HTTPException(403, "Exchange mode is only available from this computer")
    origin = request.headers.get("origin")
    if origin is not None and origin != f"{request.url.scheme}://{request.url.netloc}":
        raise HTTPException(403, "Exchange controls require the terminal's own origin")


def _exchange_state(session: ExchangeSession, source: StartExchange) -> dict:
    balances = session.balances
    stopped = session.can_replace
    health = "stopped" if stopped else "ready" if session.feed_ready else (
        "syncing" if session.feed_reason == "awaiting_snapshot" else "unavailable"
    )
    return jsonable_encoder({
        "mode": "exchange",
        "exchange_environment": session.client.environment,
        "run_id": session.run_id,
        "source": source.model_dump(exclude={"confirmation"}),
        "status": session.status,
        "speed": 1.0,
        "timestamp_ms": session.elapsed_ms,
        "next_timestamp_ms": None,
        "position": session.position,
        "unresolved_order_count": session.unresolved_order_count,
        "audit_count": len(session.audit),
        "equity_point_count": len(session.equity_history),
        "book": session.depth,
        "orders": [
            {**jsonable_encoder(order, custom_encoder=DECIMAL_ENCODERS), "queue_ahead": None}
            for order in session.orders.values()
        ],
        "balances": None if balances is None else {
            **jsonable_encoder(balances, custom_encoder=DECIMAL_ENCODERS),
            "available_btc": balances.available_btc, "available_usdt": balances.available_usdt,
        },
        "starting_balances": session.starting_balances,
        "portfolio": session.portfolio,
        "feed": {
            "health": health,
            "age_ms": session.elapsed_ms - session.last_depth_ms if not stopped and session.last_depth_ms is not None else None,
            "reason": ("depth_timeout" if not session.feed_reason else session.feed_reason) if health == "unavailable" else None,
            "detail": None,
        },
        "last_fill": session.last_fill,
        "error": session.error,
    }, custom_encoder=DECIMAL_ENCODERS)


def create_app(project_root: Path = PROJECT) -> FastAPI:
    root = project_root.resolve()
    playback = Playback()
    load_lock = asyncio.Lock()
    exchange: ExchangeSession | None = None
    exchange_source: StartExchange | None = None

    def current_state(request: Request) -> dict:
        if exchange is not None:
            _require_local(request)
            assert exchange_source is not None
            return _exchange_state(exchange, exchange_source)
        return playback.state()

    def require_paper() -> None:
        if exchange is not None:
            raise HTTPException(409, "Playback controls apply to recordings")

    def require_switchable(request: Request) -> None:
        if exchange is not None:
            _require_local(request)
        if not (playback.can_replace if exchange is None else exchange.can_replace):
            raise HTTPException(409, "Halt the live session and wait for outstanding orders to settle")

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if exchange is not None:
                await exchange.close()
            await playback.close()

    app = FastAPI(title="BTC/USDT Trading Terminal", lifespan=lifespan)
    app.mount("/assets", StaticFiles(directory=Path(__file__).with_name("static")), name="playback-assets")

    @app.get("/", include_in_schema=False)
    async def terminal() -> FileResponse:
        return FileResponse(Path(__file__).with_name("playback.html"), media_type="text/html")

    @app.get("/about", include_in_schema=False)
    async def landing_page() -> FileResponse:
        return FileResponse(Path(__file__).with_name("about.html"), media_type="text/html")

    @app.get("/api/replay/state")
    async def state(request: Request) -> dict:
        return current_state(request)

    @app.post("/api/replay/load")
    async def load(source: LoadRecording, request: Request) -> dict:
        nonlocal exchange, exchange_source
        async with load_lock:
            require_switchable(request)
            try:
                session = await asyncio.to_thread(_load_session, root, source)
            except (OSError, ValueError, InvalidOperation, KeyError, TypeError, IndexError) as error:
                raise HTTPException(400, f"Could not load replay: {error}") from error
            require_switchable(request)
            playback.replace(source, session)
            exchange = exchange_source = None
        return playback.state()

    @app.post("/api/live/start")
    async def start_live(source: StartLive, request: Request) -> dict:
        nonlocal exchange, exchange_source
        async with load_lock:
            require_switchable(request)
            try:
                filters = await asyncio.to_thread(load_binance_quote_filters)
                require_switchable(request)
                playback.start_live(source, filters)
            except (OSError, ValueError, InvalidOperation, KeyError, TypeError) as error:
                raise HTTPException(400, f"Could not start live paper trading: {error}") from error
            exchange = exchange_source = None
        return playback.state()

    @app.get("/api/exchange/config")
    async def exchange_config(request: Request) -> dict:
        _require_local(request)
        environment = os.environ.get("BINANCE_TRADING_ENV")
        configured = environment == "testnet" and all(os.environ.get(key) for key in (
            "BINANCE_API_KEY", "BINANCE_API_SECRET",
        ))
        return {"environment": environment if configured else None}

    @app.post("/api/exchange/start")
    async def start_exchange(source: StartExchange, request: Request) -> dict:
        nonlocal exchange, exchange_source
        _require_local(request)
        async with load_lock:
            require_switchable(request)
            try:
                client = BinanceSpot.configured()
                if source.confirmation != "TESTNET":
                    raise ValueError("Type TESTNET to confirm Binance Spot Testnet")
                strategy = create_strategy(source.strategy, source.spread_usdt, source.quote_size_btc)
                candidate = await ExchangeSession.start(
                    root, client, strategy, max_order_usdt=source.max_order_usdt,
                    max_drawdown_usdt=source.max_drawdown_usdt, publish=playback._publish,
                )
            except (ValueError, OSError, InvalidOperation, sqlite3.Error) as error:
                raise HTTPException(400, f"Could not start exchange trading: {error}") from error
            try:
                require_switchable(request)
            except HTTPException:
                await candidate.close()
                raise
            if playback.status == "playing":
                playback.pause()
            exchange = candidate
            exchange_source = source
            playback._publish()
        return current_state(request)

    @app.post("/api/replay/play")
    async def play() -> dict:
        require_paper()
        playback.play()
        return playback.state()

    @app.post("/api/replay/pause")
    async def pause() -> dict:
        require_paper()
        playback.pause()
        return playback.state()

    @app.post("/api/replay/step")
    async def step() -> dict:
        require_paper()
        playback.step()
        return playback.state()

    @app.post("/api/replay/halt")
    async def halt(request: Request) -> dict:
        if exchange is None:
            playback.halt()
        else:
            _require_local(request)
            exchange.halt()
        return current_state(request)

    @app.post("/api/replay/speed")
    async def speed(request: PlaybackSpeed) -> dict:
        require_paper()
        playback.set_speed(request.speed)
        return playback.state()

    @app.get("/api/replay/stream", response_class=EventSourceResponse)
    async def stream(request: Request, last_event_id: str | None = Header(default=None)) -> AsyncIterator[ServerSentEvent]:
        previous_run, _, offsets = (last_event_id or "").partition(":")
        audit_offset, _, equity_offset = offsets.partition(":")
        audit_cursor, equity_cursor = _cursor_offset(audit_offset), _cursor_offset(equity_offset)
        while True:
            changed = playback.changed
            try:
                current = current_state(request)
            except HTTPException:
                # A connection opened in paper mode can outlive a switch to exchange mode.
                return
            run_id = current["run_id"]
            session = playback.session
            audit_count, equity_count = current["audit_count"], current["equity_point_count"]
            if run_id != previous_run or audit_cursor > audit_count or equity_cursor > equity_count:
                audit_cursor = equity_cursor = 0
            if exchange is not None:
                entries = exchange.audit[audit_cursor:audit_cursor + STREAM_BATCH_SIZE]
                equity_points = [
                    EquityPoint(*values)
                    for values in exchange.equity_history[equity_cursor:equity_cursor + STREAM_BATCH_SIZE]
                ]
            else:
                entries = session.audit_since(audit_cursor, STREAM_BATCH_SIZE) if session is not None else ()
                equity_points = playback.equity_since(equity_cursor)
            audit_cursor += len(entries)
            equity_cursor += len(equity_points)
            previous_run = run_id
            audit_log = [
                {
                    "timestamp_ms": entry.timestamp_ms,
                    "kind": entry.kind,
                    "details": jsonable_encoder(dict(entry.details), custom_encoder=DECIMAL_ENCODERS),
                }
                for entry in entries
            ]
            yield ServerSentEvent(
                event="replay", id=f"{run_id}:{audit_cursor}:{equity_cursor}" if run_id is not None else None,
                data={"state": current, "audit_log": audit_log,
                      "equity_points": jsonable_encoder(equity_points, custom_encoder=DECIMAL_ENCODERS)},
            )
            if audit_cursor == audit_count and equity_cursor == equity_count:
                await changed.wait()

    return app


app = create_app()


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=DEFAULT_PORT)


if __name__ == "__main__":
    main()
