"""Signed Binance Spot REST requests and durable ownership of submitted orders."""

import asyncio
import hashlib
import hmac
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from functools import cached_property
from pathlib import Path
from time import monotonic, time_ns
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener


TESTNET_URL = "https://testnet.binance.vision"
RECV_WINDOW_MS = 3000


class BinanceError(RuntimeError):
    def __init__(self, method: str, path: str, *, status: int | None = None, code: int | None = None) -> None:
        self.status = status
        self.code = code
        detail = f"HTTP {status}, Binance code {code}" if status is not None else "response unavailable"
        super().__init__(f"{method} {path}: {detail}")

    @property
    def definitive_rejection(self) -> bool:
        # Timeouts, rate limits, connection errors and unrecognized errors leave execution unknown.
        return self.status == 400 and self.code in {
            -1013, -1021, -1022, -1100, -1102, -1111, -1116, -1121, -2010,
        }


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class BinanceSpot:
    """Keep API credentials server-side and send them only to Spot Testnet."""

    def __init__(self, environment: str, key: str, secret: str) -> None:
        if environment != "testnet":
            raise ValueError("Only Binance Spot Testnet is supported")
        self.environment = environment
        self._key = key
        self._secret = secret.encode()
        self._offset_ms = 0
        self._blocked_until = 0.0

    @cached_property
    def _opener(self):
        # Load TLS trust stores on the same worker path as the HTTP request.
        return build_opener(_NoRedirect())

    @classmethod
    def configured(cls) -> "BinanceSpot":
        environment = os.environ.get("BINANCE_TRADING_ENV")
        key = os.environ.get("BINANCE_API_KEY")
        secret = os.environ.get("BINANCE_API_SECRET")
        if environment != "testnet" or not key or not secret:
            raise ValueError("Set BINANCE_TRADING_ENV=testnet, BINANCE_API_KEY and BINANCE_API_SECRET on the server")
        return cls("testnet", key, secret)

    async def sync_time(self) -> None:
        before = time_ns() // 1_000_000
        data = await self.request("GET", "/api/v3/time", signed=False)
        after = time_ns() // 1_000_000
        self._offset_ms = int(data["serverTime"]) - (before + after) // 2

    async def request(self, method: str, path: str, *, signed: bool = True, **parameters):
        return await asyncio.to_thread(self._request, method, path, signed, parameters)

    def _request(self, method: str, path: str, signed: bool, parameters: dict):
        if not path.startswith("/api/v3/"):
            raise ValueError("Expected a Spot Testnet API path")
        if monotonic() < self._blocked_until:
            raise BinanceError(method, path, status=429)
        headers = {"Accept": "application/json"}
        if signed:
            parameters["recvWindow"] = RECV_WINDOW_MS
            parameters["timestamp"] = time_ns() // 1_000_000 + self._offset_ms
            headers["X-MBX-APIKEY"] = self._key
        query = urlencode(parameters)
        if signed:
            signature = hmac.new(self._secret, query.encode(), hashlib.sha256).hexdigest()
            query += f"&signature={signature}"
        if method == "POST":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            request = Request(TESTNET_URL + path, data=query.encode(), headers=headers, method=method)
        else:
            url = TESTNET_URL + path + (f"?{query}" if query else "")
            request = Request(url, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=5) as response:
                data = json.load(response)
            if not isinstance(data, (dict, list)):
                raise ValueError("Unexpected response format")
            return data
        except HTTPError as error:
            code = None
            if error.code in (418, 429):
                retry_after = error.headers.get("Retry-After", "")
                self._blocked_until = monotonic() + (int(retry_after) if retry_after.isdecimal() else 60)
            with closing(error):
                try:
                    response = json.load(error)
                    if isinstance(response, dict) and type(response.get("code")) is int:
                        code = response["code"]
                except (OSError, ValueError):
                    pass
            raise BinanceError(method, path, status=error.code, code=code) from None
        except (OSError, ValueError):
            raise BinanceError(method, path) from None


class OrderJournal:
    """Persist each client ID before a submit so recovery can query the exact order."""

    def __init__(self, root: Path, environment: str) -> None:
        directory = root / ".exchange"
        directory.mkdir(exist_ok=True)
        self._lease = sqlite3.connect(directory / f"{environment}-lease.sqlite3", timeout=0)
        try:
            self._lease.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as error:
            self._lease.close()
            raise ValueError("An exchange session is already using this project and environment") from error
        self._orders = None
        try:
            self._orders = sqlite3.connect(directory / f"{environment}.sqlite3")
            self._orders.execute(
                "CREATE TABLE IF NOT EXISTS orders (client_id TEXT PRIMARY KEY, account TEXT NOT NULL, exchange_id INTEGER)"
            )
            self._orders.commit()
        except BaseException:
            if self._orders is not None:
                self._orders.close()
            self._lease.close()
            raise

    def pending(self) -> list[tuple[str, str, int | None]]:
        return self._orders.execute("SELECT client_id, account, exchange_id FROM orders ORDER BY rowid").fetchall()

    def add(self, client_id: str, account: str) -> None:
        with self._orders:
            self._orders.execute("INSERT INTO orders VALUES (?, ?, NULL)", (client_id, account))

    def identify(self, client_id: str, exchange_id: int) -> None:
        with self._orders:
            self._orders.execute("UPDATE orders SET exchange_id = ? WHERE client_id = ?", (exchange_id, client_id))

    def remove(self, client_id: str) -> None:
        with self._orders:
            self._orders.execute("DELETE FROM orders WHERE client_id = ?", (client_id,))

    def close(self) -> None:
        self._orders.close()
        self._lease.close()


class AsyncOrderJournal:
    """Keep SQLite's connection and disk operations on one dedicated thread."""

    def __init__(self) -> None:
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="order-journal")
        self._journal: OrderJournal | None = None

    async def _run(self, operation, *args):
        return await asyncio.get_running_loop().run_in_executor(self._executor, operation, *args)

    def _open(self, root: Path, environment: str) -> list[tuple[str, str, int | None]]:
        self._journal = OrderJournal(root, environment)
        return self._journal.pending()

    async def open(self, root: Path, environment: str) -> list[tuple[str, str, int | None]]:
        return await self._run(self._open, root, environment)

    async def add(self, client_id: str, account: str) -> None:
        assert self._journal is not None
        await self._run(self._journal.add, client_id, account)

    async def identify(self, client_id: str, exchange_id: int) -> None:
        assert self._journal is not None
        await self._run(self._journal.identify, client_id, exchange_id)

    async def remove(self, client_id: str) -> None:
        assert self._journal is not None
        await self._run(self._journal.remove, client_id)

    def _close(self) -> None:
        if self._journal is not None:
            self._journal.close()

    async def close(self) -> None:
        try:
            # Queue close behind any write that continued through cancellation.
            await self._run(self._close)
        finally:
            await asyncio.to_thread(self._executor.shutdown)
