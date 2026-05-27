"""Advanced order semantics layered over the existing Orderbook.

The existing ``marketsimulator`` package stays as the matching core.  This
module owns higher-level exchange semantics such as post-only, IOC/FOK, expiry,
cancel-replace modifies, and conditional trigger orders.
"""

import csv
import os
from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import datetime, time

from marketsimulator.orderbook import Orderbook


ORDER_TYPE_LIMIT = "limit"
ORDER_TYPE_STOP_LIMIT = "stop_limit"
ORDER_TYPE_STOP_MARKET = "stop_market"
ORDER_TYPE_TAKE_PROFIT_LIMIT = "take_profit_limit"
ORDER_TYPE_TAKE_PROFIT_MARKET = "take_profit_market"

TIF_GTC = "GTC"
TIF_DAY = "DAY"
TIF_GTD = "GTD"
TIF_IOC = "IOC"
TIF_FOK = "FOK"

TRIGGER_ABOVE = "above"
TRIGGER_BELOW = "below"

STATUS_ACTIVE = "active"
STATUS_CANCELED = "canceled"
STATUS_EXPIRED = "expired"
STATUS_FILLED = "filled"
STATUS_PARTIALLY_FILLED_CANCELED = "partially_filled_canceled"
STATUS_PENDING_TRIGGER = "pending_trigger"
STATUS_REJECTED = "rejected"
STATUS_TRIGGERED = "triggered"

EVENT_FIELDS = [
    "seq",
    "timestamp",
    "event_type",
    "client_order_id",
    "native_order_id",
    "order_type",
    "side",
    "qty",
    "price",
    "trigger_price",
    "trigger_direction",
    "time_in_force",
    "post_only",
    "status",
    "reason",
    "pre_leaves_qty",
    "post_leaves_qty",
    "traded_qty",
    "canceled_qty",
    "linked_order_id",
]

TRADE_FIELDS = [
    "seq",
    "trade_seq",
    "timestamp",
    "price",
    "qty",
    "aggressor_native_order_id",
    "aggressor_client_order_id",
    "passive_native_order_id",
    "passive_client_order_id",
    "aggressor_side",
    "passive_side",
    "aggressor_remaining_qty",
    "passive_remaining_qty",
]


@dataclass
class OrderRequest:
    client_order_id: object
    is_buy: bool
    qty: float
    price: float = None
    order_type: str = ORDER_TYPE_LIMIT
    time_in_force: str = TIF_GTC
    post_only: bool = False
    trigger_price: float = None
    trigger_direction: str = None
    expires_at: datetime = None
    linked_order_id: object = None
    timestamp: datetime = None


@dataclass
class ManagedOrder:
    request: OrderRequest
    native_order_id: int = None
    status: str = STATUS_PENDING_TRIGGER
    created_at: datetime = None
    updated_at: datetime = None


@dataclass
class ExecutionReport:
    events: list
    trades: list

    @property
    def accepted(self):
        return not any(event["status"] == STATUS_REJECTED for event in self.events)


def _side(is_buy):
    return "buy" if is_buy else "sell"


def _blank_event():
    return OrderedDict((field, None) for field in EVENT_FIELDS)


def _blank_trade():
    return OrderedDict((field, None) for field in TRADE_FIELDS)


def _clean(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


class AdvancedEngine:
    """Order manager that adds exchange semantics without changing Orderbook."""

    def __init__(self, ticker, day_end=None, max_impact=20, resilience=0):
        self.orderbook = Orderbook(
            ticker=ticker,
            max_impact=max_impact,
            resilience=resilience,
        )
        self.day_end = day_end
        self.orders = {}
        self.native_to_client = {}
        self.events = []
        self.trade_events = []
        self.last_trade_price = None
        self._next_native_order_id = 1

    def submit(self, request):
        timestamp = request.timestamp or datetime.now()
        event_start, trade_start = self._start_report()
        self.advance_time(timestamp, _internal=True)
        self._submit(request, timestamp)
        self._check_triggers(timestamp)
        return self._finish_report(event_start, trade_start)

    def submit_limit(
        self,
        client_order_id,
        is_buy,
        qty,
        price,
        timestamp=None,
        time_in_force=TIF_GTC,
        post_only=False,
        expires_at=None,
    ):
        return self.submit(
            OrderRequest(
                client_order_id=client_order_id,
                is_buy=is_buy,
                qty=qty,
                price=price,
                timestamp=timestamp,
                time_in_force=time_in_force,
                post_only=post_only,
                expires_at=expires_at,
            )
        )

    def submit_stop(
        self,
        client_order_id,
        is_buy,
        qty,
        trigger_price,
        price=None,
        timestamp=None,
        time_in_force=TIF_GTC,
        trigger_direction=None,
        expires_at=None,
    ):
        if trigger_direction is None:
            trigger_direction = TRIGGER_ABOVE if is_buy else TRIGGER_BELOW
        order_type = ORDER_TYPE_STOP_MARKET if price is None else ORDER_TYPE_STOP_LIMIT
        return self.submit(
            OrderRequest(
                client_order_id=client_order_id,
                is_buy=is_buy,
                qty=qty,
                price=price,
                order_type=order_type,
                time_in_force=time_in_force,
                trigger_price=trigger_price,
                trigger_direction=trigger_direction,
                expires_at=expires_at,
                timestamp=timestamp,
            )
        )

    def submit_take_profit(
        self,
        client_order_id,
        is_buy,
        qty,
        trigger_price,
        price=None,
        timestamp=None,
        time_in_force=TIF_GTC,
        trigger_direction=None,
        expires_at=None,
    ):
        if trigger_direction is None:
            trigger_direction = TRIGGER_BELOW if is_buy else TRIGGER_ABOVE
        order_type = (
            ORDER_TYPE_TAKE_PROFIT_MARKET
            if price is None
            else ORDER_TYPE_TAKE_PROFIT_LIMIT
        )
        return self.submit(
            OrderRequest(
                client_order_id=client_order_id,
                is_buy=is_buy,
                qty=qty,
                price=price,
                order_type=order_type,
                time_in_force=time_in_force,
                trigger_price=trigger_price,
                trigger_direction=trigger_direction,
                expires_at=expires_at,
                timestamp=timestamp,
            )
        )

    def submit_tpsl(
        self,
        group_id,
        is_buy,
        qty,
        take_profit_trigger_price,
        stop_loss_trigger_price,
        take_profit_price=None,
        stop_loss_price=None,
        timestamp=None,
        time_in_force=TIF_GTC,
        expires_at=None,
    ):
        timestamp = timestamp or datetime.now()
        event_start, trade_start = self._start_report()
        tp_id = "{}:tp".format(group_id)
        sl_id = "{}:sl".format(group_id)
        take_profit_order_type = (
            ORDER_TYPE_TAKE_PROFIT_MARKET
            if take_profit_price is None
            else ORDER_TYPE_TAKE_PROFIT_LIMIT
        )
        stop_loss_order_type = (
            ORDER_TYPE_STOP_MARKET
            if stop_loss_price is None
            else ORDER_TYPE_STOP_LIMIT
        )
        take_profit_direction = TRIGGER_BELOW if is_buy else TRIGGER_ABOVE
        stop_loss_direction = TRIGGER_ABOVE if is_buy else TRIGGER_BELOW
        self.advance_time(timestamp, _internal=True)
        self._submit(
            OrderRequest(
                client_order_id=tp_id,
                is_buy=is_buy,
                qty=qty,
                price=take_profit_price,
                order_type=take_profit_order_type,
                time_in_force=time_in_force,
                trigger_price=take_profit_trigger_price,
                trigger_direction=take_profit_direction,
                expires_at=expires_at,
                linked_order_id=sl_id,
                timestamp=timestamp,
            ),
            timestamp,
        )
        self._submit(
            OrderRequest(
                client_order_id=sl_id,
                is_buy=is_buy,
                qty=qty,
                price=stop_loss_price,
                order_type=stop_loss_order_type,
                time_in_force=time_in_force,
                trigger_price=stop_loss_trigger_price,
                trigger_direction=stop_loss_direction,
                expires_at=expires_at,
                linked_order_id=tp_id,
                timestamp=timestamp,
            ),
            timestamp,
        )
        self._check_triggers(timestamp)
        return self._finish_report(event_start, trade_start)

    def cancel(self, client_order_id, timestamp=None, reason="user_cancel"):
        timestamp = timestamp or datetime.now()
        event_start, trade_start = self._start_report()
        self.advance_time(timestamp, _internal=True)
        self._cancel(client_order_id, timestamp, reason=reason, event_type="cancel")
        return self._finish_report(event_start, trade_start)

    def modify(
        self,
        client_order_id,
        qty=None,
        price=None,
        timestamp=None,
        time_in_force=None,
        post_only=None,
        expires_at=None,
    ):
        timestamp = timestamp or datetime.now()
        event_start, trade_start = self._start_report()
        self.advance_time(timestamp, _internal=True)
        order = self.orders.get(client_order_id)
        if order is None or order.status not in (STATUS_ACTIVE, STATUS_PENDING_TRIGGER):
            self._record_reject(
                timestamp=timestamp,
                request=OrderRequest(client_order_id, True, qty or 0),
                event_type="modify",
                reason="unknown_or_inactive_order",
            )
            return self._finish_report(event_start, trade_start)

        new_request = replace(order.request)
        if qty is not None:
            new_request.qty = qty
        if price is not None:
            new_request.price = price
        if time_in_force is not None:
            new_request.time_in_force = time_in_force
        if post_only is not None:
            new_request.post_only = post_only
        if expires_at is not None:
            new_request.expires_at = expires_at
        new_request.timestamp = timestamp

        if order.status == STATUS_PENDING_TRIGGER:
            order.request = new_request
            order.updated_at = timestamp
            self._record_event(
                event_type="modify",
                timestamp=timestamp,
                request=new_request,
                native_order_id=None,
                status=STATUS_PENDING_TRIGGER,
                reason="pending_trigger_modified",
                post_leaves_qty=new_request.qty,
            )
            return self._finish_report(event_start, trade_start)

        current = self._state(order.native_order_id)
        current_leaves = current["leavesqty"]
        same_price = new_request.price == order.request.price
        can_downsize = qty is not None and qty <= current_leaves
        metadata_only = qty is None and price is None

        if same_price and (can_downsize or metadata_only):
            if can_downsize:
                self.orderbook.modif(order.native_order_id, current_leaves - qty)
            order.request = new_request
            order.updated_at = timestamp
            after = self._state(order.native_order_id)
            order.status = STATUS_FILLED if after["leavesqty"] == 0 else STATUS_ACTIVE
            self._record_event(
                event_type="modify",
                timestamp=timestamp,
                request=new_request,
                native_order_id=order.native_order_id,
                status=order.status,
                reason="priority_preserved",
                pre_leaves_qty=current_leaves,
                post_leaves_qty=after["leavesqty"],
            )
        else:
            old_native = order.native_order_id
            self.orderbook.cancel(old_native)
            self.native_to_client.pop(old_native, None)
            order.native_order_id = None
            order.status = STATUS_CANCELED
            self._record_event(
                event_type="modify",
                timestamp=timestamp,
                request=new_request,
                native_order_id=old_native,
                status=STATUS_CANCELED,
                reason="cancel_replace",
                pre_leaves_qty=current_leaves,
                post_leaves_qty=0,
                canceled_qty=current_leaves,
            )
            self._submit(new_request, timestamp, replace_existing=True)
            self._check_triggers(timestamp)

        return self._finish_report(event_start, trade_start)

    def advance_time(self, timestamp, _internal=False):
        event_start, trade_start = self._start_report()
        self._expire_orders(timestamp)
        if _internal:
            return None
        return self._finish_report(event_start, trade_start)

    def write_csvs(self, out_dir):
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        event_path = os.path.join(out_dir, "engine_events.csv")
        trade_path = os.path.join(out_dir, "trade_events.csv")
        self._write_dict_csv(event_path, EVENT_FIELDS, self.events)
        self._write_dict_csv(trade_path, TRADE_FIELDS, self.trade_events)
        return event_path, trade_path

    def _submit(self, request, timestamp, replace_existing=False):
        request = replace(request, timestamp=timestamp)
        existing = self.orders.get(request.client_order_id)
        if (
            existing is not None
            and existing.status in (STATUS_ACTIVE, STATUS_PENDING_TRIGGER)
            and not replace_existing
        ):
            self._record_reject(timestamp, request, "submit", "duplicate_client_order_id")
            return

        reason = self._validate(request, timestamp)
        if reason is not None:
            self._record_reject(timestamp, request, "submit", reason)
            return

        if self._is_conditional(request):
            managed = ManagedOrder(
                request=request,
                status=STATUS_PENDING_TRIGGER,
                created_at=timestamp,
                updated_at=timestamp,
            )
            self.orders[request.client_order_id] = managed
            self._record_event(
                event_type="submit",
                timestamp=timestamp,
                request=request,
                native_order_id=None,
                status=STATUS_PENDING_TRIGGER,
                reason="waiting_for_trigger",
                post_leaves_qty=request.qty,
            )
            return

        self._submit_active(request, timestamp)

    def _submit_active(self, request, timestamp):
        price = request.price
        tif = request.time_in_force
        if price is None:
            price = self._market_limit_price(request.is_buy)
            tif = TIF_IOC
            request = replace(request, price=price, time_in_force=tif)

        if request.post_only and self._would_cross(request.is_buy, price):
            self._record_reject(timestamp, request, "submit", "post_only_would_cross")
            return

        if tif == TIF_FOK and self._available_qty(request.is_buy, price) < request.qty:
            self._record_reject(timestamp, request, "submit", "fok_not_fillable")
            return

        native_order_id = self._allocate_native_order_id()
        self.native_to_client[native_order_id] = request.client_order_id
        trade_start = self.orderbook.ntrds
        self.orderbook.send(
            is_buy=request.is_buy,
            qty=request.qty,
            price=price,
            uid=native_order_id,
            is_mine=True,
            timestamp=timestamp,
        )
        trade_end = self.orderbook.ntrds
        trades = self._record_trades(trade_start, trade_end, request.qty)
        traded_qty = sum(trade["qty"] for trade in trades)

        state = self._state(native_order_id)
        pre_cancel_leaves = state["leavesqty"]
        canceled_qty = 0
        if tif in (TIF_IOC, TIF_FOK) and state["leavesqty"] > 0:
            canceled_qty = state["leavesqty"]
            self.orderbook.cancel(native_order_id)
            state = self._state(native_order_id)

        if state["active"]:
            status = STATUS_ACTIVE
        elif traded_qty == request.qty:
            status = STATUS_FILLED
        elif traded_qty > 0 and canceled_qty > 0:
            status = STATUS_PARTIALLY_FILLED_CANCELED
        else:
            status = STATUS_CANCELED
        managed = ManagedOrder(
            request=request,
            native_order_id=native_order_id,
            status=status,
            created_at=timestamp,
            updated_at=timestamp,
        )
        self.orders[request.client_order_id] = managed

        self._record_event(
            event_type="submit",
            timestamp=timestamp,
            request=request,
            native_order_id=native_order_id,
            status=status,
            reason=None,
            post_leaves_qty=state["leavesqty"],
            traded_qty=traded_qty,
            canceled_qty=canceled_qty,
        )
        self._refresh_orders_from_trades(trades)
        if canceled_qty and pre_cancel_leaves:
            managed.status = status

    def _check_triggers(self, timestamp):
        if self.last_trade_price is None:
            return

        made_progress = True
        while made_progress:
            made_progress = False
            pending = [
                order for order in self.orders.values()
                if order.status == STATUS_PENDING_TRIGGER
            ]
            for order in pending:
                if order.status != STATUS_PENDING_TRIGGER:
                    continue
                request = order.request
                if not self._triggered(request):
                    continue

                order.status = STATUS_TRIGGERED
                order.updated_at = timestamp
                self._record_event(
                    event_type="trigger",
                    timestamp=timestamp,
                    request=request,
                    native_order_id=None,
                    status=STATUS_TRIGGERED,
                    reason="trigger_price_reached",
                    post_leaves_qty=request.qty,
                )
                if request.linked_order_id is not None:
                    self._cancel(
                        request.linked_order_id,
                        timestamp,
                        reason="linked_order_triggered",
                        event_type="cancel",
                    )

                active_request = self._activation_request(request, timestamp)
                self._submit_active(active_request, timestamp)
                made_progress = True

    def _activation_request(self, request, timestamp):
        order_type = request.order_type
        if order_type in (ORDER_TYPE_STOP_MARKET, ORDER_TYPE_TAKE_PROFIT_MARKET):
            return replace(
                request,
                order_type=ORDER_TYPE_LIMIT,
                price=self._market_limit_price(request.is_buy),
                time_in_force=TIF_IOC,
                timestamp=timestamp,
            )
        return replace(request, order_type=ORDER_TYPE_LIMIT, timestamp=timestamp)

    def _cancel(self, client_order_id, timestamp, reason, event_type):
        order = self.orders.get(client_order_id)
        if order is None or order.status not in (STATUS_ACTIVE, STATUS_PENDING_TRIGGER):
            self._record_event(
                event_type=event_type,
                timestamp=timestamp,
                request=OrderRequest(client_order_id, True, 0),
                native_order_id=None,
                status=STATUS_REJECTED,
                reason="unknown_or_inactive_order",
            )
            return

        if order.status == STATUS_PENDING_TRIGGER:
            order.status = STATUS_CANCELED
            order.updated_at = timestamp
            self._record_event(
                event_type=event_type,
                timestamp=timestamp,
                request=order.request,
                native_order_id=None,
                status=STATUS_CANCELED,
                reason=reason,
                pre_leaves_qty=order.request.qty,
                post_leaves_qty=0,
                canceled_qty=order.request.qty,
            )
            return

        before = self._state(order.native_order_id)
        self.orderbook.cancel(order.native_order_id)
        after = self._state(order.native_order_id)
        order.status = STATUS_CANCELED
        order.updated_at = timestamp
        self._record_event(
            event_type=event_type,
            timestamp=timestamp,
            request=order.request,
            native_order_id=order.native_order_id,
            status=STATUS_CANCELED,
            reason=reason,
            pre_leaves_qty=before["leavesqty"],
            post_leaves_qty=after["leavesqty"],
            canceled_qty=before["leavesqty"],
        )

    def _expire_orders(self, timestamp):
        for order in list(self.orders.values()):
            if order.status not in (STATUS_ACTIVE, STATUS_PENDING_TRIGGER):
                continue
            expires_at = self._expires_at(order.request, order.created_at)
            if expires_at is None or timestamp < expires_at:
                continue
            if order.status == STATUS_PENDING_TRIGGER:
                order.status = STATUS_EXPIRED
                order.updated_at = timestamp
                self._record_event(
                    event_type="expire",
                    timestamp=timestamp,
                    request=order.request,
                    native_order_id=None,
                    status=STATUS_EXPIRED,
                    reason="time_in_force_expired",
                    pre_leaves_qty=order.request.qty,
                    post_leaves_qty=0,
                    canceled_qty=order.request.qty,
                )
            else:
                before = self._state(order.native_order_id)
                self.orderbook.cancel(order.native_order_id)
                after = self._state(order.native_order_id)
                order.status = STATUS_EXPIRED
                order.updated_at = timestamp
                self._record_event(
                    event_type="expire",
                    timestamp=timestamp,
                    request=order.request,
                    native_order_id=order.native_order_id,
                    status=STATUS_EXPIRED,
                    reason="time_in_force_expired",
                    pre_leaves_qty=before["leavesqty"],
                    post_leaves_qty=after["leavesqty"],
                    canceled_qty=before["leavesqty"],
                )

    def _record_trades(self, trade_start, trade_end, incoming_qty):
        trades = []
        aggressor_remaining = incoming_qty
        for pos in range(trade_start, trade_end):
            qty = self._number(self.orderbook.trades["vol"][pos])
            aggressor_native = int(self.orderbook.trades["agg_ord"][pos])
            passive_native = int(self.orderbook.trades["pas_ord"][pos])
            aggressor_client = self.native_to_client.get(aggressor_native)
            passive_client = self.native_to_client.get(passive_native)
            aggressor_is_buy = bool(self.orderbook.trades["buy_init"][pos])
            passive_state = self._state(passive_native)

            aggressor_remaining -= qty
            trade = _blank_trade()
            trade.update(
                {
                    "seq": len(self.events),
                    "trade_seq": len(self.trade_events),
                    "timestamp": _clean(self.orderbook.trades["timestamp"][pos]),
                    "price": self._number(self.orderbook.trades["price"][pos]),
                    "qty": qty,
                    "aggressor_native_order_id": aggressor_native,
                    "aggressor_client_order_id": aggressor_client,
                    "passive_native_order_id": passive_native,
                    "passive_client_order_id": passive_client,
                    "aggressor_side": _side(aggressor_is_buy),
                    "passive_side": _side(not aggressor_is_buy),
                    "aggressor_remaining_qty": aggressor_remaining,
                    "passive_remaining_qty": passive_state["leavesqty"],
                }
            )
            self.trade_events.append(trade)
            trades.append(trade)
            self.last_trade_price = trade["price"]
        return trades

    def _refresh_orders_from_trades(self, trades):
        client_ids = set()
        for trade in trades:
            client_ids.add(trade["aggressor_client_order_id"])
            client_ids.add(trade["passive_client_order_id"])
        for client_id in client_ids:
            if client_id is None:
                continue
            order = self.orders.get(client_id)
            if order is None or order.native_order_id is None:
                continue
            state = self._state(order.native_order_id)
            if state["leavesqty"] == 0 and not state["active"]:
                order.status = STATUS_FILLED
            elif state["active"]:
                order.status = STATUS_ACTIVE

    def _record_event(
        self,
        event_type,
        timestamp,
        request,
        native_order_id,
        status,
        reason=None,
        pre_leaves_qty=None,
        post_leaves_qty=None,
        traded_qty=0,
        canceled_qty=0,
    ):
        event = _blank_event()
        event.update(
            {
                "seq": len(self.events),
                "timestamp": _clean(timestamp),
                "event_type": event_type,
                "client_order_id": request.client_order_id,
                "native_order_id": native_order_id,
                "order_type": request.order_type,
                "side": _side(request.is_buy),
                "qty": request.qty,
                "price": request.price,
                "trigger_price": request.trigger_price,
                "trigger_direction": request.trigger_direction,
                "time_in_force": request.time_in_force,
                "post_only": request.post_only,
                "status": status,
                "reason": reason,
                "pre_leaves_qty": pre_leaves_qty,
                "post_leaves_qty": post_leaves_qty,
                "traded_qty": traded_qty,
                "canceled_qty": canceled_qty,
                "linked_order_id": request.linked_order_id,
            }
        )
        self.events.append(event)
        return event

    def _record_reject(self, timestamp, request, event_type, reason):
        return self._record_event(
            event_type=event_type,
            timestamp=timestamp,
            request=request,
            native_order_id=None,
            status=STATUS_REJECTED,
            reason=reason,
        )

    def _validate(self, request, timestamp):
        if request.qty is None or request.qty <= 0:
            return "qty_must_be_positive"
        if request.time_in_force not in (TIF_GTC, TIF_DAY, TIF_GTD, TIF_IOC, TIF_FOK):
            return "unsupported_time_in_force"
        if request.order_type not in (
            ORDER_TYPE_LIMIT,
            ORDER_TYPE_STOP_LIMIT,
            ORDER_TYPE_STOP_MARKET,
            ORDER_TYPE_TAKE_PROFIT_LIMIT,
            ORDER_TYPE_TAKE_PROFIT_MARKET,
        ):
            return "unsupported_order_type"
        if request.order_type == ORDER_TYPE_LIMIT and request.price is None:
            return "limit_price_required"
        if self._is_conditional(request):
            if request.trigger_price is None:
                return "trigger_price_required"
            if request.trigger_direction not in (TRIGGER_ABOVE, TRIGGER_BELOW):
                return "trigger_direction_required"
            if request.order_type in (
                ORDER_TYPE_STOP_LIMIT,
                ORDER_TYPE_TAKE_PROFIT_LIMIT,
            ) and request.price is None:
                return "limit_price_required"
        if request.time_in_force == TIF_GTD and request.expires_at is None:
            return "expires_at_required"
        expires_at = self._expires_at(request, timestamp)
        if expires_at is not None and timestamp >= expires_at:
            return "already_expired"
        return None

    def _is_conditional(self, request):
        return request.order_type in (
            ORDER_TYPE_STOP_LIMIT,
            ORDER_TYPE_STOP_MARKET,
            ORDER_TYPE_TAKE_PROFIT_LIMIT,
            ORDER_TYPE_TAKE_PROFIT_MARKET,
        )

    def _triggered(self, request):
        if request.trigger_direction == TRIGGER_ABOVE:
            return self.last_trade_price >= request.trigger_price
        return self.last_trade_price <= request.trigger_price

    def _would_cross(self, is_buy, price):
        if is_buy:
            best = self.orderbook.best_ask
            return best is not None and best[0] <= price
        best = self.orderbook.best_bid
        return best is not None and best[0] >= price

    def _available_qty(self, is_buy, limit_price):
        total = 0
        if is_buy:
            current = self.orderbook._asks.best_pricelevel
            while current is not None and current.price <= limit_price:
                total += current.vol
                current = current.next
        else:
            current = self.orderbook._bids.best_pricelevel
            while current is not None and current.price >= limit_price:
                total += current.vol
                current = current.next
        return total

    def _expires_at(self, request, created_at):
        if request.time_in_force == TIF_GTD:
            return request.expires_at
        if request.time_in_force != TIF_DAY:
            return None
        if request.expires_at is not None:
            return request.expires_at
        if isinstance(self.day_end, datetime):
            return self.day_end
        day = created_at.date()
        end = self.day_end if isinstance(self.day_end, time) else time(23, 59, 59, 999999)
        return datetime.combine(day, end)

    def _state(self, native_order_id):
        try:
            return self.orderbook.get(native_order_id)
        except KeyError:
            return {"leavesqty": 0, "active": False}

    def _market_limit_price(self, is_buy):
        return float("inf") if is_buy else 0.0

    def _allocate_native_order_id(self):
        native_order_id = self._next_native_order_id
        self._next_native_order_id += 1
        return native_order_id

    def _number(self, value):
        as_float = float(value)
        if as_float.is_integer():
            return int(as_float)
        return as_float

    def _start_report(self):
        return len(self.events), len(self.trade_events)

    def _finish_report(self, event_start, trade_start):
        return ExecutionReport(
            events=self.events[event_start:],
            trades=self.trade_events[trade_start:],
        )

    @staticmethod
    def _write_dict_csv(path, fieldnames, rows):
        with open(path, "w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
