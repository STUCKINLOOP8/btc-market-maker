import {createOrderTimeline} from './order-timeline.js';

const byId = (id) => document.getElementById(id);
const loadForm = byId('load');
const loadButton = byId('load-button');
const modePicker = loadForm.elements.namedItem('mode');
const strategyPicker = loadForm.elements.namedItem('strategy');
const spreadInput = loadForm.elements.namedItem('spread_usdt');
const strategyLabels = new Map([...strategyPicker.options].map(({value, text}) => [value, text]));
const speedInput = byId('speed');
const notice = byId('notice');
const chart = byId('equity-chart');
const stream = new EventSource('/api/replay/stream');
const workingStatuses = new Set(['ACTIVE', 'PARTIALLY_FILLED', 'CANCEL_PENDING']);
const chartMoney = new Intl.NumberFormat('en', {maximumFractionDigits: 2});
const chartTime = new Intl.NumberFormat('en', {maximumFractionDigits: 3});
const theme = getComputedStyle(document.documentElement);
const equityHistory = [];
const timeline = createOrderTimeline(decimalText);

let latestState;
let renderedBook;
let renderedOrders;
let animationFrame;
let loading = false;
let exchangeEnvironment = null;

function showNotice(message) {
  notice.textContent = message;
  notice.hidden = !message;
}

async function command(action, payload, endpoint = '/api/replay/' + action) {
  showNotice('');
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const {detail} = await response.json();
      throw new Error(typeof detail === 'string' ? detail : 'Check the playback inputs.');
    }
  } catch (error) {
    showNotice(error.message);
  }
}

function showSourceFields() {
  const live = modePicker.value !== 'replay';
  const exchange = modePicker.value === 'exchange';
  const recording = loadForm.elements.namedItem('recording');
  const scriptOption = strategyPicker.options[0];
  if (live && !strategyPicker.value) strategyPicker.value = 'fixed-spread';
  scriptOption.hidden = live;
  scriptOption.disabled = live;
  byId('recording-field').hidden = live;
  recording.disabled = live;
  loadButton.textContent = exchange
    ? 'Start Testnet'
    : live ? 'Start live paper' : 'Load recording';
  const strategySelected = Boolean(strategyPicker.value);
  byId('script-field').hidden = strategySelected;
  loadForm.elements.namedItem('orders').disabled = strategySelected;
  for (const label of loadForm.querySelectorAll('.quote-setting')) {
    label.hidden = !strategySelected;
    label.querySelector('input').disabled = !strategySelected;
  }
  for (const label of loadForm.querySelectorAll('.exchange-setting')) {
    label.hidden = !exchange;
    label.querySelector('input').disabled = !exchange;
  }
  const prompt = byId('exchange-notice');
  prompt.hidden = !exchange;
  prompt.textContent = exchangeEnvironment === 'testnet'
    ? 'Binance Spot Testnet uses virtual funds.'
    : 'Configure Spot Testnet credentials on the server to enable this mode.';
  byId('exchange-confirm-label').textContent = 'Type TESTNET to confirm';

  // Toggle replay controls visibility based on selected mode
  const replayControls = byId('replay-controls');
  if (replayControls) {
    replayControls.hidden = live;
  }
  speedInput.disabled = live;

  updateLoadButton();
}

modePicker.addEventListener('change', showSourceFields);
strategyPicker.addEventListener('change', showSourceFields);
showSourceFields();

modePicker.addEventListener('change', () => {
  const live = modePicker.value !== 'replay';
  const previousDefault = live ? '100' : '250';
  if (spreadInput.value === previousDefault) spreadInput.value = live ? '250' : '100';
  loadForm.elements.namedItem('exchange_confirmation').value = '';
});

fetch('/api/exchange/config')
  .then((response) => response.ok ? response.json() : {})
  .then(({environment}) => {
    exchangeEnvironment = environment === 'testnet' ? environment : null;
    showSourceFields();
  })
  .catch(() => {});

function updateLoadButton() {
  loadButton.disabled = loading || ['starting', 'live', 'halting'].includes(latestState?.status)
    || (modePicker.value === 'exchange' && exchangeEnvironment == null);
}

loadForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const fields = new FormData(loadForm);
  loading = true;
  updateLoadButton();
  const payload = {
    strategy: fields.get('strategy') || null,
  };
  if (payload.strategy) {
    payload.spread_usdt = fields.get('spread_usdt');
    payload.quote_size_btc = fields.get('quote_size_btc');
  }
  if (fields.get('mode') === 'exchange') {
    await command('start', {
      ...payload,
      environment: exchangeEnvironment,
      confirmation: fields.get('exchange_confirmation').trim(),
      max_order_usdt: fields.get('max_order_usdt'),
      max_drawdown_usdt: fields.get('max_drawdown_usdt'),
    }, '/api/exchange/start');
    loadForm.elements.namedItem('exchange_confirmation').value = '';
  } else if (fields.get('mode') === 'live') {
    await command('start', payload, '/api/live/start');
  } else {
    payload.recording = fields.get('recording').trim();
    payload.orders = fields.get('orders')?.trim() || null;
    await command('load', payload);
  }
  loading = false;
  updateLoadButton();
});

for (const action of ['play', 'pause', 'step']) {
  byId(action).addEventListener('click', () => command(action));
}
byId('halt').addEventListener('click', async () => {
  byId('halt').disabled = true;
  await command('halt');
  byId('halt').disabled = !canHalt(latestState);
});
speedInput.addEventListener('change', () => {
  if (speedInput.reportValidity()) command('speed', {speed: speedInput.valueAsNumber});
});

function decimalText(value) {
  if (value == null) return '—';
  // Keep the server's Decimal digits intact for tables and current values.
  const [mantissa, exponent = '0'] = value.toLowerCase().split('e');
  const negative = mantissa.startsWith('-');
  const [whole, fraction = ''] = mantissa.replace(/^[+-]/, '').split('.');
  const digits = whole + fraction;
  if (!/[1-9]/.test(digits)) return '0';
  const point = whole.length + Number(exponent);
  if (Math.abs(point) > 64) return value;
  const padded = digits.padStart(digits.length + Math.max(0, 1 - point), '0').padEnd(Math.max(1, point), '0');
  const split = Math.max(1, point);
  const integer = padded.slice(0, split).replace(/^0+(?=\d)/, '').replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  const decimals = padded.slice(split).replace(/0+$/, '');
  return (negative ? '-' : '') + integer + (decimals ? '.' + decimals : '');
}

function metric(id, value, unit, signed = false) {
  const node = byId(id);
  if (!node) return;
  const amount = decimalText(value);
  const positive = signed && value != null && amount !== '0' && !amount.startsWith('-');
  const negative = signed && amount.startsWith('-');
  node.textContent = (positive ? '▲ +' : negative ? '▼ ' : '') + (negative ? amount.slice(1) : amount) + (value != null && unit ? ' ' + unit : '');
  node.classList.toggle('positive', positive);
  node.classList.toggle('negative', negative);
}

function canHalt(state) {
  return Boolean(state?.run_id) && state.status !== 'halting' && state.status !== 'halted';
}

function renderHalt(state) {
  const unresolved = state.unresolved_order_count ?? 0;
  byId('halt-unresolved').textContent = unresolved;
  metric('halt-btc', state.balances?.btc, 'BTC');
  metric('halt-inventory', state.portfolio?.inventory_delta_btc, 'BTC', true);

  const haltBtn = byId('halt');
  if (state.status === 'halting') {
    haltBtn.textContent = 'Halting...';
  } else if (state.status === 'halted') {
    haltBtn.textContent = 'Halted';
  } else {
    haltBtn.textContent = 'Halt';
  }
}

function renderLive(state) {
  const feed = state.feed;
  const feedPill = byId('feed-pill');
  if (feed == null) {
    if (feedPill) {
      feedPill.dataset.health = 'ready';
      byId('feed-health').textContent = state.mode === 'replay' ? 'Recorded feed' : 'Healthy';
      byId('feed-age').textContent = state.timestamp_ms != null ? `${state.timestamp_ms} ms` : '—';
    }
    return;
  }
  const health = byId('feed-health');
  const healthLabel = {
    syncing: 'Synchronizing',
    ready: 'Healthy',
    unavailable: 'Quoting paused · resyncing',
    stopped: 'Stopped',
  }[feed.health] ?? feed.health;
  health.textContent = healthLabel;
  health.dataset.state = feed.health;
  if (feedPill) feedPill.dataset.health = feed.health;

  byId('feed-age').textContent = feed.age_ms == null ? '—' : feed.age_ms + ' ms';
  const reason = feed.reason !== 'awaiting_snapshot' ? feed.reason : null;
  byId('feed-reason-field').hidden = !reason;
  byId('feed-reason').textContent = reason?.replaceAll('_', ' ') ?? '';
  byId('feed-reason').title = feed.detail ?? '';
  for (const [side, id] of [['buy', 'paper-bid'], ['sell', 'paper-ask']]) {
    const quote = state.orders.find((order) => order.side === side && workingStatuses.has(order.status));
    metric(id, quote?.limit_price, 'USDT');
  }
  const fill = state.last_fill;
  byId('last-fill').textContent = fill
    ? fill.side + ' ' + decimalText(fill.fill_quantity) + ' BTC @ ' + decimalText(fill.price) + ' USDT'
    : 'No fills yet';
}

function renderMode(state) {
  const isExchange = state.mode === 'exchange';
  const isLive = state.mode === 'live';
  const isReplay = state.mode === 'replay';

  byId('wallet-heading').textContent = isExchange ? 'Binance wallet' : 'Paper wallet';
  byId('equity-label').textContent = isExchange ? 'BTC + USDT equity' : 'Paper equity';
  byId('orders-heading').textContent = isExchange ? 'Exchange orders' : 'Paper orders';
  byId('status').textContent = isExchange
    ? 'Testnet · ' + state.status
    : ({empty: 'No session loaded', live: 'Live paper'}[state.status] ?? state.status);

  // Persistent mode badge
  const modeBadge = byId('mode-badge');
  if (modeBadge) {
    if (isExchange) {
      modeBadge.textContent = 'TESTNET';
      modeBadge.dataset.mode = 'exchange';
    } else if (isLive) {
      modeBadge.textContent = 'LIVE PAPER';
      modeBadge.dataset.mode = 'live';
    } else {
      modeBadge.textContent = 'REPLAY';
      modeBadge.dataset.mode = 'replay';
    }
  }

  // Show play, pause, step, speed only in replay mode; hide in live paper / testnet
  const replayControls = byId('replay-controls');
  if (replayControls) {
    replayControls.hidden = !isReplay;
  }
  speedInput.disabled = !isReplay;
}

function row(values) {
  const element = document.createElement('tr');
  for (const value of values) {
    const cell = element.insertCell();
    cell.textContent = value;
  }
  return element;
}

function renderBook(state) {
  const book = state.book;
  const bookKey = state.run_id + ':' + (book?.sequence ?? 'none');
  if (bookKey === renderedBook) return;
  renderedBook = bookKey;
  byId('midpoint').textContent = 'Midpoint ' + decimalText(state.portfolio?.mid_price);
  byId('depth-empty').hidden = book != null;
  byId('depth-tables').hidden = book == null;
  const largest = book ? Math.max(0, ...[...book.bids, ...book.asks].map(({quantity}) => Number(quantity))) : 0;

  const workingOrders = (state.orders || []).filter((o) => workingStatuses.has(o.status));

  for (const [side, direction] of [['bids', 'buy'], ['asks', 'sell']]) {
    const rows = (book?.[side] ?? []).map(({price, quantity}) => {
      const element = row([decimalText(price), decimalText(quantity)]);
      element.cells[0].classList.add(direction);
      const size = largest > 0 ? Number(quantity) / largest * 100 : 0;
      element.cells[1].style.setProperty('--depth', size + '%');

      // Mark my working bid, ask and queue position
      const myOrder = workingOrders.find((o) => o.side === direction && o.limit_price === price);
      if (myOrder) {
        element.classList.add('my-order');
        const badge = document.createElement('span');
        badge.className = 'my-order-pill ' + direction;
        const qText = myOrder.queue_ahead != null ? ` Q: ${decimalText(myOrder.queue_ahead)}` : '';
        badge.textContent = `MY ${direction === 'buy' ? 'BID' : 'ASK'}${qText}`;
        element.cells[0].append(badge);
      }

      return element;
    });
    byId(side).replaceChildren(...rows);
  }
}

function makeSideChip(side) {
  const chip = document.createElement('span');
  const upper = (side || '').toUpperCase();
  chip.className = `chip chip-${side.toLowerCase()}`;
  chip.innerHTML = `<span class="chip-icon" aria-hidden="true">${upper === 'BUY' ? '▲' : '▼'}</span> ${upper}`;
  return chip;
}

function makeStatusChip(rawStatus) {
  const chip = document.createElement('span');
  const upper = (rawStatus || '').toUpperCase();
  let label = upper.replaceAll('_', ' ');
  let icon = '●';
  let cls = 'active';

  if (upper === 'FILLED') {
    icon = '✔';
    cls = 'filled';
  } else if (upper.includes('PARTIAL')) {
    icon = '◐';
    cls = 'partial';
  } else if (upper === 'PENDING_NEW' || upper === 'PENDING') {
    icon = '⏳';
    cls = 'pending';
    label = 'PENDING';
  } else if (upper === 'CANCEL_PENDING') {
    icon = '⏳';
    cls = 'pending';
    label = 'CANCELING';
  } else if (upper === 'CANCELED') {
    icon = '✖';
    cls = 'canceled';
  } else if (upper === 'REJECTED') {
    icon = '⊘';
    cls = 'rejected';
  } else if (upper === 'EXPIRED') {
    icon = '⏱';
    cls = 'expired';
  }
  chip.className = `chip chip-${cls}`;
  chip.innerHTML = `<span class="chip-icon" aria-hidden="true">${icon}</span> ${label}`;
  return chip;
}

function renderOrders(orders) {
  const key = JSON.stringify(orders);
  if (key === renderedOrders) return;
  renderedOrders = key;
  byId('orders-empty').hidden = orders.length > 0;
  byId('order-rows').replaceChildren(...orders.map((order) => {
    const queueAhead = workingStatuses.has(order.status) ? order.queue_ahead : null;
    const element = row([
      '', '', '', decimalText(order.limit_price),
      decimalText(order.original_quantity), decimalText(order.filled_quantity), decimalText(queueAhead),
    ]);
    const inspect = document.createElement('button');
    inspect.className = 'order-link';
    inspect.type = 'button';
    inspect.textContent = order.order_id;
    inspect.setAttribute('aria-controls', 'timeline');
    inspect.addEventListener('click', () => timeline.selectOrder(order.order_id));
    element.cells[0].append(inspect);
    element.cells[1].append(makeSideChip(order.side));
    element.cells[2].append(makeStatusChip(order.status));
    return element;
  }));
}

function renderPortfolio(state) {
  const portfolio = state.portfolio;
  metric('net-pnl', portfolio?.net_pnl_usdt, 'USDT', true);
  metric('excess-pnl', portfolio?.excess_pnl_usdt, 'USDT', true);
  metric('inventory', portfolio?.inventory_delta_btc, 'BTC', true);
  metric('volume', portfolio?.filled_volume_btc, 'BTC');
  metric('equity', portfolio?.equity_usdt, '');
  metric('hold', portfolio?.hold_equity_usdt, '');
  for (const asset of ['btc', 'usdt']) {
    byId(asset + '-total').textContent = decimalText(state.balances?.[asset]);
    byId(asset + '-available').textContent = decimalText(state.balances?.['available_' + asset]);
    byId(asset + '-reserved').textContent = decimalText(state.balances?.['reserved_' + asset]);
  }

  // Update Stacked Bar Allocation Visualizer
  const btcAvail = Number(state.balances?.available_btc || 0);
  const btcRes = Number(state.balances?.reserved_btc || 0);
  const btcTotal = btcAvail + btcRes;
  const btcAvailPct = btcTotal > 0 ? (btcAvail / btcTotal) * 100 : 100;
  const btcResPct = btcTotal > 0 ? (btcRes / btcTotal) * 100 : 0;
  const btcAvailEl = byId('btc-bar-avail');
  const btcResEl = byId('btc-bar-res');
  if (btcAvailEl && btcResEl) {
    btcAvailEl.style.width = btcAvailPct + '%';
    btcResEl.style.width = btcResPct + '%';
    const btcRatio = byId('btc-bar-ratio');
    if (btcRatio) {
      const availStr = decimalText(state.balances?.available_btc) || '0';
      const resStr = decimalText(state.balances?.reserved_btc) || '0';
      btcRatio.textContent = `Available ${availStr} · Reserved ${resStr}`;
    }
  }

  const usdtAvail = Number(state.balances?.available_usdt || 0);
  const usdtRes = Number(state.balances?.reserved_usdt || 0);
  const usdtTotal = usdtAvail + usdtRes;
  const usdtAvailPct = usdtTotal > 0 ? (usdtAvail / usdtTotal) * 100 : 100;
  const usdtResPct = usdtTotal > 0 ? (usdtRes / usdtTotal) * 100 : 0;
  const usdtAvailEl = byId('usdt-bar-avail');
  const usdtResEl = byId('usdt-bar-res');
  if (usdtAvailEl && usdtResEl) {
    usdtAvailEl.style.width = usdtAvailPct + '%';
    usdtResEl.style.width = usdtResPct + '%';
    const usdtRatio = byId('usdt-bar-ratio');
    if (usdtRatio) {
      const availStr = decimalText(state.balances?.available_usdt) || '0';
      const resStr = decimalText(state.balances?.reserved_usdt) || '0';
      usdtRatio.textContent = `Available ${availStr} · Reserved ${resStr}`;
    }
  }

  // Excess vs hold strip under equity chart
  const excessVal = portfolio?.excess_pnl_usdt;
  const excessEl = byId('chart-excess-val');
  if (excessEl) {
    if (excessVal == null) {
      excessEl.textContent = '— USDT';
      excessEl.className = 'excess-val';
    } else {
      const num = Number(excessVal);
      const sign = num > 0 ? '+' : '';
      const arrow = num > 0 ? '▲ ' : num < 0 ? '▼ ' : '';
      excessEl.textContent = `${arrow}${sign}${decimalText(excessVal)} USDT`;
      excessEl.className = 'excess-val ' + (num > 0 ? 'gain' : num < 0 ? 'loss' : 'neutral');
    }
  }
}

function drawEquity(through) {
  const width = chart.clientWidth;
  const height = chart.clientHeight;
  const scale = window.devicePixelRatio || 1;
  const pixelWidth = Math.round(width * scale);
  const pixelHeight = Math.round(height * scale);
  if (chart.width !== pixelWidth || chart.height !== pixelHeight) {
    chart.width = pixelWidth;
    chart.height = pixelHeight;
  }
  const ctx = chart.getContext('2d');
  ctx.setTransform(scale, 0, 0, scale, 0, 0);
  ctx.clearRect(0, 0, width, height);

  let low = Infinity;
  let high = -Infinity;
  for (const point of equityHistory) {
    if (point.equity_usdt == null) continue;
    low = Math.min(low, Number(point.equity_usdt), Number(point.hold_equity_usdt));
    high = Math.max(high, Number(point.equity_usdt), Number(point.hold_equity_usdt));
  }
  byId('chart-empty').hidden = Number.isFinite(low);
  if (!Number.isFinite(low) || width < 100) return;

  const padding = Math.max((high - low) * 0.12, 0.04);
  low -= padding;
  high += padding;
  ctx.font = '12px "IBM Plex Mono", Consolas, monospace';
  const labelWidth = Math.max(ctx.measureText(chartMoney.format(high)).width, ctx.measureText(chartMoney.format(low)).width);
  const left = labelWidth + 12;
  const right = width - 12;
  const top = 12;
  const bottom = height - 26;
  const first = equityHistory[0].timestamp_ms;
  const last = Math.max(through, equityHistory.at(-1).timestamp_ms);
  const x = (timestamp) => left + (timestamp - first) / Math.max(1, last - first) * (right - left);
  const y = (value) => bottom - (value - low) / (high - low) * (bottom - top);

  // Draw feed outages as faint bands
  if (equityHistory.length > 1) {
    ctx.fillStyle = 'rgba(255, 92, 122, 0.12)';
    let outageStart = null;
    for (let i = 0; i < equityHistory.length; i++) {
      const pt = equityHistory[i];
      if (pt.equity_usdt == null) {
        if (outageStart === null) outageStart = pt.timestamp_ms;
      } else {
        if (outageStart !== null) {
          const xStart = x(outageStart);
          const xEnd = x(pt.timestamp_ms);
          ctx.fillRect(xStart, top, Math.max(3, xEnd - xStart), bottom - top);
          outageStart = null;
        }
      }
    }
    if (outageStart !== null) {
      const xStart = x(outageStart);
      const xEnd = x(last);
      ctx.fillRect(xStart, top, Math.max(3, xEnd - xStart), bottom - top);
    }
  }

  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  ctx.fillStyle = theme.getPropertyValue('--text-secondary').trim() || '#9AA3B8';
  ctx.strokeStyle = '#2A3042';
  ctx.lineWidth = 1;
  for (let tick = 0; tick <= 4; tick += 1) {
    const value = low + (high - low) * tick / 4;
    const level = y(value);
    ctx.fillText(chartMoney.format(value), left - 7, level);
    ctx.beginPath();
    ctx.moveTo(left, level);
    ctx.lineTo(right, level);
    ctx.stroke();
  }
  ctx.textAlign = 'left';
  ctx.fillText(chartTime.format(first / 1000) + ' s', left, bottom + 14);
  if (last > first) {
    ctx.textAlign = 'right';
    ctx.fillText(chartTime.format(last / 1000) + ' s', right, bottom + 14);
  }

  function plot(key, color, dash, lineWidth = 2) {
    ctx.beginPath();
    ctx.strokeStyle = color;
    ctx.fillStyle = color;
    ctx.lineWidth = lineWidth;
    ctx.setLineDash(dash);
    let previous = null;
    for (const point of equityHistory) {
      const at = x(point.timestamp_ms);
      if (previous !== null) ctx.lineTo(at, previous);
      if (point[key] === null) {
        previous = null;
        continue;
      }
      const level = y(Number(point[key]));
      if (previous === null) ctx.moveTo(at, level);
      else ctx.lineTo(at, level);
      previous = level;
    }
    if (previous !== null) ctx.lineTo(x(last), previous);
    ctx.stroke();
    if (previous !== null) {
      ctx.beginPath();
      ctx.arc(x(last), previous, lineWidth * 1.5, 0, 2 * Math.PI);
      ctx.fill();
    }
  }

  // Draw buy-and-hold FIRST as a muted yellow dashed line beneath the paper equity line
  plot('hold_equity_usdt', theme.getPropertyValue('--muted-yellow').trim() || '#E5C158', [4, 3], 1.2);
  // Draw paper equity SECOND on top as a cyan solid line so identical values do not overlap noisily
  plot('equity_usdt', theme.getPropertyValue('--cyan').trim() || '#35D6F0', [], 2.4);
  ctx.setLineDash([]);
}

function scheduleRender() {
  if (animationFrame !== undefined) return;
  animationFrame = requestAnimationFrame(() => {
    animationFrame = undefined;
    if (!latestState) return;
    renderBook(latestState);
    renderOrders(latestState.orders);
    renderPortfolio(latestState);
    drawEquity(latestState.timestamp_ms);
  });
}

stream.addEventListener('replay', ({data}) => {
  const {state, audit_log: audit, equity_points: points = []} = JSON.parse(data);
  if (latestState?.run_id !== state.run_id) {
    equityHistory.length = 0;
    timeline.reset(state.starting_balances);
  }
  latestState = state;
  equityHistory.push(...points);
  timeline.append(audit);
  renderMode(state);
  byId('status').dataset.state = state.status;
  byId('active-strategy').textContent = state.source ? strategyLabels.get(state.source.strategy ?? '') : '—';
  byId('time').textContent = state.timestamp_ms === null ? '—' : state.timestamp_ms + ' ms';
  byId('position').textContent = state.position;
  byId('audit').textContent = state.audit_count;
  byId('play').disabled = state.status !== 'paused';
  byId('pause').disabled = state.status !== 'playing';
  byId('step').disabled = state.status !== 'paused';
  byId('halt').disabled = !canHalt(state);
  updateLoadButton();
  renderHalt(state);
  renderLive(state);
  if (document.activeElement !== speedInput) speedInput.value = state.speed;
  if (state.error) showNotice(state.error);
  scheduleRender();
});

stream.onerror = () => {
  byId('status').textContent = latestState?.mode === 'exchange'
    ? 'Testnet · reconnecting'
    : 'Reconnecting';
  byId('status').dataset.state = 'reconnecting';
  const feedPill = byId('feed-pill');
  if (feedPill) {
    feedPill.dataset.health = 'reconnecting';
    byId('feed-health').textContent = 'Reconnecting';
  }
};
new ResizeObserver(scheduleRender).observe(chart);

// Check if ?replay=1 requested in URL
const urlParams = new URLSearchParams(window.location.search);
if (urlParams.get('replay') === '1' && modePicker.value !== 'replay') {
  modePicker.value = 'replay';
  showSourceFields();
}
