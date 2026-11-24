# BTC/USDT paper market maker

The terminal can replay saved recordings, run paper trading on live Binance Spot data, or use a separately configured Binance exchange connection. Paper orders, fills, fees, and balances are simulated locally.

## Setup

Run the commands from this project folder in PowerShell. The project was verified with Python 3.14.6 and the exact dependency versions in `requirements.txt`.

```powershell
python -m pip install -r requirements.txt
python -B -m market_maker.playback
```

Open the [paper terminal](http://127.0.0.1:8080). Keep the server running while using the terminal and run the replay and benchmark commands in a second PowerShell window.

## Verify the Milestone 1 core

```powershell
python -B -m market_maker.core_check
```

This command runs the event, wallet, execution, queue, risk, and replay tests. If they pass, it prints the fixture's event trace, including the five-second markout and the 4.93 USDT losing round trip. Test progress and timing go to stderr. The trace goes to stdout and is identical across runs.

## Replay a recording

1. Select **Saved recording** under **Data source**.
2. For a short replay with a known partial fill and cancellation, enter `fixtures/order_timeline_recording.jsonl` as the recording path. Choose **Scripted orders** and enter `fixtures/order_timeline_orders.json` as the order script path.
3. Select **Load recording**. Use **Step** to process one event or **Play** to run at the selected speed. Select an order or timeline event to inspect queue position, fills, and wallet changes.

Run the same fixture from PowerShell to print the audit log and final balances. For strategy replay, choose one of the strategy options in the terminal or pass `--strategy` on the command line.

```powershell
python -B -m market_maker.replay fixtures/order_timeline_recording.jsonl --orders fixtures/order_timeline_orders.json
python -B -m market_maker.replay binance-btcusdt-milestone2.jsonl --strategy fixed-spread --spread-usdt 120 --quote-size-btc 0.001
```

## Run the held-out benchmarks

The default manifest is [data/milestone-5/manifest.json](data/milestone-5/manifest.json). The runner verifies the recording and market-filter hashes, then runs Fixed Spread, Avellaneda–Stoikov, and Avellaneda–Stoikov + Imbalance on the same held-out sessions with shared settings. Each batch writes `config.json` and `summary.json`. Each session and strategy pair also gets its own `config.json`, `result.json`, and `audit.jsonl`.

```powershell
$run = 'runs/milestone-5/benchmark-' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff')
python -B -m market_maker.benchmark --output $run
python -B -m market_maker.benchmark --config "$run/config.json" --output "${run}-repeat"
Get-FileHash "$run/summary.json", "${run}-repeat/summary.json" | Select-Object Hash, Path
```

Matching summary hashes verify the reproduced results, including the audit hashes stored in each summary. Each command needs a new output directory.

Run the fee and queue sensitivity cases separately from the same saved settings.

```powershell
python -B -m market_maker.benchmark --config "$run/config.json" --maker-fee 0.0005 --output "${run}-fee"
python -B -m market_maker.benchmark --config "$run/config.json" --queue-model proportional-cancellation --output "${run}-queue"
python -B -m market_maker.report --case 'Baseline' $run --case 'Lower maker fee' "${run}-fee" --case 'Proportional cancellations' "${run}-queue" --output "${run}-report"
```

Open `comparison.html` in the report directory. The report includes net and excess P&L, time quoted, filled volume, inventory variance, 1s, 5s, 30s, and 60s markouts, plus Sharpe, maximum drawdown, and win rate. The supporting metric definitions appear beside the values. The default ranking requires at least 10 fills per run. Benchmark batches and reports under `runs/` are generated locally.

## Start live paper trading

1. Select **Live Binance · paper** under **Data source**. Choose a strategy, spread, and order size. The default is Fixed Spread with a 250 USDT spread and 0.001 BTC per side.
2. Select **Start live paper**. The feed should move from **Synchronizing** to **Healthy** once the local book is ready. **Feed age** measures milliseconds since the last accepted synchronized book update.
3. Watch **Working bid**, **Working ask**, and **Latest fill**. The order timeline shows the trade, fee, and wallet changes behind each fill. If a healthy feed shows blank quotes, inspect the timeline for skipped or rejected proposals and their reasons.
4. A feed gap, timeout, or disconnect pauses quoting. The terminal cancels outstanding paper orders and resumes only after resynchronizing the order book.
5. Select **Halt** to stop new quotes and settle outstanding cancellations. Wait for **Halted**, zero unresolved orders, and zero BTC and USDT reserved. The **Remaining BTC** field includes any fills completed during the session.

Live mode starts with a fresh paper wallet of 0.50 BTC and 34,225 USDT. It uses public WebSocket and REST market data and requires an internet connection. The session runs on the server and survives a browser reload.

## Set up Binance Spot Testnet

Paper works without configuration. To verify exchange order placement and cancellation with virtual funds, create a Binance Spot Testnet HMAC API key. Enter the key and secret into the PowerShell session that starts the server. The prompts mask input. Enable Spot trading on the key and leave withdrawals disabled. [Binance Spot Testnet setup](https://developers.binance.com/en/docs/products/spot/testnet/general-info)

```powershell
$env:BINANCE_TRADING_ENV = 'testnet'
$env:BINANCE_API_KEY = [System.Net.NetworkCredential]::new('', (Read-Host 'Binance API key' -AsSecureString)).Password
$env:BINANCE_API_SECRET = [System.Net.NetworkCredential]::new('', (Read-Host 'Binance API secret' -AsSecureString)).Password
python -B -m market_maker.playback
```

1. Open the terminal and select **Binance Spot Testnet**. Choose a strategy, spread, and size. Enter the maximum USDT value per order and the maximum session drawdown in USDT.
2. Type **TESTNET** in the confirmation field and start the session. Watch the exchange-confirmed orders, fills, and account balances.
3. Click **Halt** and wait for **Halted** with zero unresolved bot orders. You can then switch to **Live Binance · paper** or **Saved recording** from the same terminal.

On Testnet, equity and drawdown are marked on virtual BTC and USDT balances. The timeline shows the commission asset returned by Testnet.

**Halt** cancels the bot's outstanding Testnet orders and preserves completed fills. While order status is uncertain, the terminal blocks further quoting and keeps the session in **Halting**. Bot client order IDs begin with `pmm-`. The local `.exchange/` journal lets the next Testnet session recover pending orders before a fresh session can quote. Halt and let orders settle before shutting down the server.

## Run the tests

```powershell
python -B -m unittest discover -s tests -v
```

The terminal integration tests use a local server and simulate Binance feed recovery, so the suite can run without reaching Binance.
