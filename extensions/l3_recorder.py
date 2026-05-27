"""Export order-level replay inputs and expected match results.

This module intentionally does not modify ``marketsimulator``.  It reuses the
existing ``Orderbook`` API and records the state transitions around each input
event.

Example:
    python -m extensions.l3_recorder \
        --input data/historic_orders/orders-san-2019-5-23.csv \
        --ticker san \
        --out-dir /tmp/l3-san \
        --max-events 1000
"""

import argparse
import csv
import math
import os
from collections import OrderedDict

import pandas as pd

from marketsimulator.orderbook import Orderbook


INPUT_EVENT_FIELDS = [
    "seq",
    "timestamp",
    "event_type",
    "raw_ordtype",
    "order_id",
    "side",
    "qty",
    "price",
    "qty_down",
    "accepted",
    "pre_leaves_qty",
    "post_leaves_qty",
    "post_active",
    "traded_qty",
    "rested_qty",
    "reason",
]

TRADE_EVENT_FIELDS = [
    "seq",
    "trade_seq",
    "timestamp",
    "price",
    "qty",
    "aggressor_order_id",
    "passive_order_id",
    "aggressor_side",
    "passive_side",
    "aggressor_remaining_qty",
    "passive_remaining_qty",
]


def _is_nan(value):
    try:
        return math.isnan(value)
    except TypeError:
        return False


def _clean(value):
    if value is None or _is_nan(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _clean_number(value):
    value = _clean(value)
    if value is None:
        return None
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        return value
    if as_float.is_integer():
        return int(as_float)
    return as_float


def _parse_bool(value):
    if value is None or _is_nan(value):
        return None
    if isinstance(value, bool):
        return value
    if str(value).lower() == "true":
        return True
    if str(value).lower() == "false":
        return False
    raise ValueError("Cannot parse bool value: {}".format(value))


def _side(is_buy):
    if is_buy is None:
        return None
    return "buy" if is_buy else "sell"


def _event_type(raw_ordtype):
    if raw_ordtype == "new":
        return "new"
    if raw_ordtype == "cancel":
        return "cancel"
    if raw_ordtype == "modif":
        return "modify"
    raise ValueError("Unexpected ordtype: {}".format(raw_ordtype))


def _order_state(orderbook, order_id):
    try:
        return orderbook.get(order_id)
    except KeyError:
        return None


def _blank_input_event():
    return OrderedDict((field, None) for field in INPUT_EVENT_FIELDS)


def _blank_trade_event():
    return OrderedDict((field, None) for field in TRADE_EVENT_FIELDS)


class L3ReplayRecorder:
    """Replay L3-style orders and record expected results.

    The recorder consumes rows with the same schema as the repository's
    historical CSV files:

    ``ordtype;uid;is_buy;qty;price;timestamp``
    """

    def __init__(self, ticker, max_impact=20, resilience=0):
        self.orderbook = Orderbook(
            ticker=ticker,
            max_impact=max_impact,
            resilience=resilience,
        )
        self.input_events = []
        self.trade_events = []

    def replay_csv(self, path, max_events=None):
        orders = pd.read_csv(path, sep=";", float_precision="round_trip")
        orders["timestamp"] = pd.to_datetime(orders["timestamp"])
        return self.replay_dataframe(orders, max_events=max_events)

    def replay_dataframe(self, orders, max_events=None):
        count = len(orders) if max_events is None else min(max_events, len(orders))
        for seq in range(count):
            self.apply_order_row(seq, orders.iloc[seq])
        return self

    def apply_order_row(self, seq, row):
        raw_ordtype = row["ordtype"]
        event_type = _event_type(raw_ordtype)
        order_id = int(row["uid"])
        timestamp = row["timestamp"]
        qty = _clean_number(row["qty"])
        price = _clean_number(row["price"])
        is_buy = _parse_bool(row["is_buy"])
        side = _side(is_buy)

        before = _order_state(self.orderbook, order_id)
        trade_start = self.orderbook.ntrds
        accepted = True
        reason = None

        try:
            if raw_ordtype == "new":
                self.orderbook.send(
                    is_buy=is_buy,
                    qty=qty,
                    price=price,
                    uid=order_id,
                    is_mine=order_id < 0,
                    timestamp=timestamp,
                )
            elif raw_ordtype == "cancel":
                self.orderbook.cancel(uid=order_id)
            elif raw_ordtype == "modif":
                self.orderbook.modif(uid=order_id, qty_down=qty)
            else:
                raise ValueError("Unexpected ordtype: {}".format(raw_ordtype))
        except KeyError:
            accepted = False
            reason = "unknown_order"

        trade_end = self.orderbook.ntrds
        after = _order_state(self.orderbook, order_id)
        trades = self._record_trade_events(
            seq=seq,
            trade_start=trade_start,
            trade_end=trade_end,
            incoming_qty=qty if raw_ordtype == "new" else 0,
        )
        traded_qty = sum(trade["qty"] for trade in trades)

        event = _blank_input_event()
        event.update(
            {
                "seq": seq,
                "timestamp": _clean(timestamp),
                "event_type": event_type,
                "raw_ordtype": raw_ordtype,
                "order_id": order_id,
                "side": side if side is not None else _side(before["is_buy"]) if before else None,
                "qty": qty if raw_ordtype == "new" else None,
                "price": price if raw_ordtype == "new" else None,
                "qty_down": qty if raw_ordtype == "modif" else None,
                "accepted": accepted,
                "pre_leaves_qty": before["leavesqty"] if before else None,
                "post_leaves_qty": after["leavesqty"] if after else None,
                "post_active": after["active"] if after else None,
                "traded_qty": traded_qty,
                "rested_qty": after["leavesqty"] if raw_ordtype == "new" and after else None,
                "reason": reason,
            }
        )
        self.input_events.append(event)
        return event, trades

    def _record_trade_events(self, seq, trade_start, trade_end, incoming_qty):
        trades = []
        aggressor_remaining = _clean_number(incoming_qty) or 0

        for pos in range(trade_start, trade_end):
            qty = _clean_number(self.orderbook.trades["vol"][pos])
            aggressor_id = int(self.orderbook.trades["agg_ord"][pos])
            passive_id = int(self.orderbook.trades["pas_ord"][pos])
            aggressor_is_buy = bool(self.orderbook.trades["buy_init"][pos])
            passive = _order_state(self.orderbook, passive_id)

            aggressor_remaining -= qty
            passive_remaining = passive["leavesqty"] if passive else 0

            trade = _blank_trade_event()
            trade.update(
                {
                    "seq": seq,
                    "trade_seq": len(self.trade_events),
                    "timestamp": _clean(self.orderbook.trades["timestamp"][pos]),
                    "price": _clean_number(self.orderbook.trades["price"][pos]),
                    "qty": qty,
                    "aggressor_order_id": aggressor_id,
                    "passive_order_id": passive_id,
                    "aggressor_side": _side(aggressor_is_buy),
                    "passive_side": _side(not aggressor_is_buy),
                    "aggressor_remaining_qty": aggressor_remaining,
                    "passive_remaining_qty": passive_remaining,
                }
            )
            self.trade_events.append(trade)
            trades.append(trade)

        return trades

    def write_csvs(self, out_dir):
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        input_path = os.path.join(out_dir, "input_events.csv")
        trade_path = os.path.join(out_dir, "trade_events.csv")
        _write_dict_csv(input_path, INPUT_EVENT_FIELDS, self.input_events)
        _write_dict_csv(trade_path, TRADE_EVENT_FIELDS, self.trade_events)
        return input_path, trade_path


def _write_dict_csv(path, fieldnames, rows):
    with open(path, "w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def export_l3_csv(input_path, ticker, out_dir, max_events=None):
    recorder = L3ReplayRecorder(ticker=ticker)
    recorder.replay_csv(input_path, max_events=max_events)
    return recorder.write_csvs(out_dir)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Historical order CSV path")
    parser.add_argument("--ticker", required=True, help="Ticker/liquidity-band key")
    parser.add_argument("--out-dir", required=True, help="Output directory")
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args(argv)

    input_path, trade_path = export_l3_csv(
        input_path=args.input,
        ticker=args.ticker,
        out_dir=args.out_dir,
        max_events=args.max_events,
    )
    print("wrote {}".format(input_path))
    print("wrote {}".format(trade_path))


if __name__ == "__main__":
    main()
