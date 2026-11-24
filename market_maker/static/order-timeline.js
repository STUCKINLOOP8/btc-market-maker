const eventLabels = new Map([
  ['QUOTE_PROPOSED', 'Quote proposed'],
  ['QUOTE_ROUNDED', 'Quote rounded'],
  ['ORDER_SUBMITTED', 'Submitted'],
  ['ORDER_ACTIVATED', 'Activated'],
  ['EXCHANGE_ORDER_UPDATED', 'Exchange order updated'],
  ['QUEUE_ADVANCED', 'Queue advanced'],
  ['FILL', 'Fill'],
  ['CANCEL_REQUESTED', 'Cancel requested'],
  ['ORDER_CANCELED', 'Canceled'],
  ['ORDER_REJECTED', 'Rejected'],
  ['ORDER_SKIPPED', 'Skipped'],
  ['HALT_REQUESTED', 'Halt requested'],
  ['CANCEL_IGNORED', 'Cancel ignored'],
]);

const walletFields = [
  ['btc', 'Total BTC'],
  ['usdt', 'Total USDT'],
  ['reserved_btc', 'Reserved BTC'],
  ['reserved_usdt', 'Reserved USDT'],
];

function eventTitle({kind, details}) {
  if (kind !== 'FILL') return eventLabels.get(kind) || kind;
  if (details.status === 'FILLED') return 'Filled';
  if (details.status === 'CANCEL_PENDING') return 'Partial fill · cancel pending';
  return 'Partial fill';
}

function getEventPrice(event, submittedPrice) {
  const {kind, details} = event;
  if (kind === 'QUOTE_PROPOSED' || kind === 'QUOTE_ROUNDED') return details.limit_price;
  if (kind === 'ORDER_SUBMITTED' || (kind === 'ORDER_SKIPPED' && details.price != null)) return details.price;
  if (details.trade_id != null) return details.price ?? submittedPrice;
  return details.price ?? details.limit_price ?? submittedPrice;
}

function getEventQuantity(event) {
  const {kind, details} = event;
  if (kind === 'QUOTE_PROPOSED' || kind === 'QUOTE_ROUNDED') return details.quantity;
  if (kind === 'ORDER_SUBMITTED' || (kind === 'ORDER_SKIPPED' && details.quantity != null)) return details.quantity;
  if (details.fill_quantity != null) return details.fill_quantity;
  if (details.trade_quantity != null) return details.trade_quantity;
  return details.quantity;
}

function createSideChip(side) {
  if (!side) return null;
  const chip = document.createElement('span');
  const upper = side.toUpperCase();
  chip.className = `chip chip-${side.toLowerCase()}`;
  chip.innerHTML = `<span class="chip-icon" aria-hidden="true">${upper === 'BUY' ? '▲' : '▼'}</span> ${upper}`;
  return chip;
}

function createStatusChip(status) {
  if (!status) return null;
  const chip = document.createElement('span');
  const upper = status.toUpperCase();
  let icon = '●';
  let modifier = 'active';
  if (upper === 'FILLED') {
    icon = '✔';
    modifier = 'filled';
  } else if (upper.includes('PARTIAL')) {
    icon = '◐';
    modifier = 'partial';
  } else if (upper.includes('PENDING')) {
    icon = '⏳';
    modifier = 'pending';
  } else if (upper === 'CANCELED') {
    icon = '✖';
    modifier = 'canceled';
  } else if (upper === 'REJECTED') {
    icon = '⊘';
    modifier = 'rejected';
  } else if (upper === 'EXPIRED') {
    icon = '⏱';
    modifier = 'expired';
  }
  chip.className = `chip chip-${modifier}`;
  chip.innerHTML = `<span class="chip-icon" aria-hidden="true">${icon}</span> ${upper.replaceAll('_', ' ')}`;
  return chip;
}

export function createOrderTimeline(decimalText) {
  const list = document.getElementById('timeline-events');
  const showAll = document.getElementById('timeline-all');
  const empty = document.getElementById('timeline-empty');
  const inspector = document.getElementById('inspector-content');
  const inspectorEmpty = document.getElementById('inspector-empty');
  const detailsList = document.getElementById('inspector-details');
  const walletTable = document.getElementById('inspector-wallet');
  const walletRows = document.getElementById('inspector-wallet-rows');
  const walletEmpty = document.getElementById('inspector-wallet-empty');

  const history = [];
  const submissions = new Map();
  const expandedGroups = new Set();
  let wallet = null;
  let orderFilter = null;
  let selectedButton = null;
  let selectedRecord = null;

  function inspect(record, button) {
    if (selectedButton && selectedButton !== button) {
      selectedButton.setAttribute('aria-pressed', 'false');
    }
    selectedButton = button;
    selectedRecord = record;
    if (button) {
      button.setAttribute('aria-pressed', 'true');
    }

    inspector.hidden = !record;
    inspectorEmpty.hidden = Boolean(record);
    if (!record) {
      inspectorEmpty.textContent = 'Click an event to see quote inputs, queue position and fills.';
      return;
    }

    const {event, before, after, submittedPrice} = record;
    const {kind, timestamp_ms: time, details} = event;
    const fields = [['Event', eventTitle(event)], ['Replay time', time + ' ms']];
    if (details.order_id != null) fields.push(['Order', details.order_id]);
    if (details.side != null) fields.push(['Side', details.side]);
    if (details.mid_price != null) fields.push(['Midpoint (USDT)', decimalText(details.mid_price)]);
    if (kind === 'QUOTE_PROPOSED' || kind === 'QUOTE_ROUNDED') {
      fields.push(['Limit price (USDT)', decimalText(details.limit_price)],
                  ['Order size (BTC)', decimalText(details.quantity)]);
    }
    if (kind === 'ORDER_SUBMITTED' || (kind === 'ORDER_SKIPPED' && details.price != null && details.quantity != null)) {
      fields.push(['Limit price (USDT)', decimalText(details.price)], ['Order size (BTC)', decimalText(details.quantity)]);
    }
    if (details.queue_ahead != null) fields.push(['Initial queue ahead (BTC)', decimalText(details.queue_ahead)]);
    if (details.trade_id != null) {
      fields.push(['Trade ID', String(details.trade_id)], ['Trade size (BTC)', decimalText(details.trade_quantity)]);
      const tradePrice = details.price ?? submittedPrice;
      if (tradePrice != null) fields.push(['Trade price (USDT)', decimalText(tradePrice)]);
    }
    if (details.queue_before != null) {
      fields.push(['Queue before (BTC)', decimalText(details.queue_before)], ['Queue after (BTC)', decimalText(details.queue_after)]);
    }
    if (details.fill_quantity != null) fields.push(['Fill size (BTC)', decimalText(details.fill_quantity)]);
    if (details.filled_quantity != null) fields.push(['Total filled (BTC)', decimalText(details.filled_quantity)]);
    if (details.fee_usdt != null) fields.push(['Maker fee (USDT)', decimalText(details.fee_usdt)]);
    if (details.commission != null) {
      fields.push(['Commission (' + details.commission_asset + ')', decimalText(details.commission)]);
    }
    if (details.reason != null) fields.push(['Reason', details.reason]);

    detailsList.replaceChildren(...fields.flatMap(([label, value]) => {
      const term = document.createElement('dt');
      term.textContent = label;
      const definition = document.createElement('dd');
      definition.textContent = value;
      return [term, definition];
    }));

    const changes = [];
    if (before && after) {
      for (const [key, label] of walletFields) {
        const previous = decimalText(before[key]);
        const current = decimalText(after[key]);
        if (previous === current) continue;
        const row = document.createElement('tr');
        const heading = document.createElement('th');
        heading.scope = 'row';
        heading.textContent = label;
        row.append(heading);
        row.insertCell().textContent = previous;
        row.insertCell().textContent = current;
        changes.push(row);
      }
    }
    walletRows.replaceChildren(...changes);
    walletTable.hidden = changes.length === 0;
    walletEmpty.hidden = changes.length > 0;
  }

  function createRowButton(record, deltaMs, isSubRow = false) {
    const {event, submittedPrice} = record;
    const button = document.createElement('button');
    button.type = 'button';
    button.className = isSubRow ? 'timeline-row-btn sub-row' : 'timeline-row-btn';
    button.setAttribute('aria-controls', 'timeline-inspector');
    button.setAttribute('aria-pressed', 'false');

    // 1. Time Column (+delta)
    const timeCol = document.createElement('div');
    timeCol.className = 'col-time';
    const mainTime = document.createElement('span');
    mainTime.className = 'event-time';
    mainTime.textContent = event.timestamp_ms + ' ms';
    const deltaSpan = document.createElement('span');
    deltaSpan.className = 'delta-time';
    deltaSpan.textContent = deltaMs >= 0 ? `+${deltaMs} ms` : `${deltaMs} ms`;
    timeCol.append(mainTime, deltaSpan);

    // 2. Type Chip
    const typeCol = document.createElement('div');
    typeCol.className = 'col-type';
    const typeChip = document.createElement('span');
    typeChip.className = 'chip chip-event';
    typeChip.textContent = eventTitle(event);
    typeCol.append(typeChip);

    // 3. Side Chip
    const sideCol = document.createElement('div');
    sideCol.className = 'col-side';
    if (event.details.side) {
      const sideChip = createSideChip(event.details.side);
      if (sideChip) sideCol.append(sideChip);
    }

    // 4. Order ID
    const orderCol = document.createElement('div');
    orderCol.className = 'col-order';
    orderCol.textContent = event.details.order_id || '—';

    // 5. Price
    const priceCol = document.createElement('div');
    priceCol.className = 'col-price';
    const priceVal = getEventPrice(event, submittedPrice);
    priceCol.textContent = priceVal != null ? decimalText(priceVal) : '—';

    // 6. Quantity
    const qtyCol = document.createElement('div');
    qtyCol.className = 'col-qty';
    const qtyVal = getEventQuantity(event);
    qtyCol.textContent = qtyVal != null ? decimalText(qtyVal) : '—';

    // 7. Trade ID / Queue / Context
    const metaCol = document.createElement('div');
    metaCol.className = 'col-meta';
    if (event.details.trade_id != null) {
      metaCol.innerHTML = `<span class="trade-tag">#${event.details.trade_id}</span>`;
    } else if (event.details.queue_after != null) {
      metaCol.textContent = `Q: ${decimalText(event.details.queue_after)}`;
    } else if (event.details.reason != null) {
      metaCol.textContent = event.details.reason;
    } else if (event.details.queue_ahead != null) {
      metaCol.textContent = `Q: ${decimalText(event.details.queue_ahead)}`;
    } else {
      metaCol.textContent = '—';
    }

    button.append(timeCol, typeCol, sideCol, orderCol, priceCol, qtyCol, metaCol);
    button.addEventListener('click', () => inspect(record, button));
    return button;
  }

  function renderTimeline() {
    list.replaceChildren();
    const visibleRecords = history.filter(
      (r) => orderFilter === null || r.event.details.order_id === orderFilter
    );

    if (visibleRecords.length === 0) {
      empty.hidden = false;
      inspect(null, null);
      return;
    }
    empty.hidden = true;

    // Group consecutive identical events (same order_id and same kind)
    const groups = [];
    let currentGroup = null;

    for (let i = 0; i < visibleRecords.length; i++) {
      const record = visibleRecords[i];
      const prevRecord = i > 0 ? visibleRecords[i - 1] : null;
      const deltaMs = prevRecord ? (record.event.timestamp_ms - prevRecord.event.timestamp_ms) : 0;
      const orderId = record.event.details.order_id;
      const kind = record.event.kind;

      if (
        currentGroup &&
        currentGroup.orderId === orderId &&
        currentGroup.kind === kind &&
        orderId != null
      ) {
        currentGroup.items.push({record, deltaMs});
      } else {
        if (currentGroup) groups.push(currentGroup);
        currentGroup = {
          orderId,
          kind,
          groupKey: `${orderId}:${kind}:${record.event.timestamp_ms}`,
          items: [{record, deltaMs}],
        };
      }
    }
    if (currentGroup) groups.push(currentGroup);

    let lastButton = null;
    let lastRecord = null;
    let matchFound = false;

    for (const group of groups) {
      if (group.items.length === 1) {
        // Single row
        const {record, deltaMs} = group.items[0];
        const item = document.createElement('li');
        item.className = 'timeline-single';
        const button = createRowButton(record, deltaMs, false);
        item.append(button);
        list.append(item);

        if (selectedRecord && selectedRecord.event === record.event) {
          inspect(record, button);
          matchFound = true;
        }
        lastButton = button;
        lastRecord = record;
      } else {
        // Grouped row
        const isExpanded = expandedGroups.has(group.groupKey);
        const firstItem = group.items[0];
        const lastItem = group.items[group.items.length - 1];

        const groupLi = document.createElement('li');
        groupLi.className = `timeline-group ${isExpanded ? 'expanded' : 'collapsed'}`;

        const headerBtn = document.createElement('button');
        headerBtn.type = 'button';
        headerBtn.className = 'group-header-btn';
        headerBtn.setAttribute('aria-expanded', String(isExpanded));
        headerBtn.setAttribute('aria-controls', `group-children-${group.groupKey}`);

        // Left Section: Expand icon, badge count & event type
        const summary = document.createElement('div');
        summary.className = 'group-summary';

        const toggleIcon = document.createElement('span');
        toggleIcon.className = 'group-toggle-icon';
        toggleIcon.setAttribute('aria-hidden', 'true');
        toggleIcon.textContent = isExpanded ? '▼' : '▶';

        const countBadge = document.createElement('span');
        countBadge.className = 'chip group-badge';
        countBadge.textContent = `${group.items.length}× ${eventTitle(firstItem.record.event)}`;

        const orderSpan = document.createElement('span');
        orderSpan.className = 'group-order';
        orderSpan.textContent = group.orderId;

        summary.append(toggleIcon, countBadge, orderSpan);

        // Middle Section: Time range
        const timeRange = document.createElement('div');
        timeRange.className = 'group-timerange';
        timeRange.textContent = `${firstItem.record.event.timestamp_ms} – ${lastItem.record.event.timestamp_ms} ms`;

        // Right Section: Queue shift (first -> last queue value)
        const queueShift = document.createElement('div');
        queueShift.className = 'group-queueshift';
        const qFirst = firstItem.record.event.details.queue_before ??
                       firstItem.record.event.details.queue_ahead ??
                       firstItem.record.event.details.queue_after;
        const qLast = lastItem.record.event.details.queue_after ??
                      lastItem.record.event.details.queue_ahead;
        if (qFirst != null && qLast != null) {
          queueShift.textContent = `${decimalText(qFirst)} → ${decimalText(qLast)} BTC`;
        } else {
          queueShift.textContent = `${group.items.length} identical events`;
        }

        const actionHint = document.createElement('span');
        actionHint.className = 'group-hint';
        actionHint.textContent = isExpanded ? 'Click to collapse' : 'Click to expand';

        headerBtn.append(summary, timeRange, queueShift, actionHint);

        // Child list
        const childrenOl = document.createElement('ol');
        childrenOl.id = `group-children-${group.groupKey}`;
        childrenOl.className = 'group-children';
        childrenOl.hidden = !isExpanded;

        for (const itemData of group.items) {
          const childLi = document.createElement('li');
          const childBtn = createRowButton(itemData.record, itemData.deltaMs, true);
          childLi.append(childBtn);
          childrenOl.append(childLi);

          if (selectedRecord && selectedRecord.event === itemData.record.event) {
            inspect(itemData.record, childBtn);
            matchFound = true;
          }
          lastButton = childBtn;
          lastRecord = itemData.record;
        }

        // Toggle expansion on header click
        headerBtn.addEventListener('click', () => {
          if (expandedGroups.has(group.groupKey)) {
            expandedGroups.delete(group.groupKey);
            groupLi.classList.remove('expanded');
            groupLi.classList.add('collapsed');
            childrenOl.hidden = true;
            headerBtn.setAttribute('aria-expanded', 'false');
            toggleIcon.textContent = '▶';
            actionHint.textContent = 'Click to expand';
            // When collapsing, keep latest of this group inspected
            inspect(lastItem.record, headerBtn);
          } else {
            expandedGroups.add(group.groupKey);
            groupLi.classList.remove('collapsed');
            groupLi.classList.add('expanded');
            childrenOl.hidden = false;
            headerBtn.setAttribute('aria-expanded', 'true');
            toggleIcon.textContent = '▼';
            actionHint.textContent = 'Click to collapse';
            // When expanding, inspect latest of this group
            inspect(lastItem.record, headerBtn);
          }
        });

        groupLi.append(headerBtn, childrenOl);
        list.append(groupLi);

        if (selectedRecord && selectedRecord.event === lastItem.record.event && !isExpanded) {
          inspect(lastItem.record, headerBtn);
          matchFound = true;
        }
      }
    }

    // Auto-select latest event if no active matching selection
    if (!matchFound && lastRecord && lastButton) {
      inspect(lastRecord, lastButton);
    }
  }

  function filter(orderId = null) {
    orderFilter = orderId;
    showAll.hidden = orderId === null;
    renderTimeline();
    if (orderId !== null && selectedButton) {
      selectedButton.focus();
    }
  }

  showAll.addEventListener('click', () => {
    filter(null);
    list.querySelector('button')?.focus();
  });

  return {
    reset(startingBalances) {
      history.length = 0;
      submissions.clear();
      expandedGroups.clear();
      wallet = startingBalances;
      selectedRecord = null;
      selectedButton = null;
      filter(null);
    },
    append(audit) {
      for (const event of audit) {
        const {details, kind} = event;
        const before = wallet;
        if (walletFields.every(([key]) => details[key] != null)) wallet = details;
        if (!eventLabels.has(kind)) continue;
        if (kind === 'ORDER_SUBMITTED') submissions.set(details.order_id, details.price);
        const record = {event, before, after: wallet, submittedPrice: submissions.get(details.order_id)};
        history.push(record);
      }
      renderTimeline();
    },
    selectOrder: filter,
  };
}
