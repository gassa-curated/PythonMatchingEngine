from datetime import datetime, time

from extensions.advanced_engine import (
    AdvancedEngine,
    OrderRequest,
    ORDER_TYPE_STOP_LIMIT,
    STATUS_ACTIVE,
    STATUS_CANCELED,
    STATUS_EXPIRED,
    STATUS_FILLED,
    STATUS_PARTIALLY_FILLED_CANCELED,
    STATUS_PENDING_TRIGGER,
    STATUS_REJECTED,
    TIF_DAY,
    TIF_FOK,
    TIF_GTD,
    TIF_IOC,
    TRIGGER_ABOVE,
)


def at(second):
    return datetime(2019, 5, 23, 9, 0, second)


def test_post_only_rejects_crossing_order():
    engine = AdvancedEngine(ticker="band6stock")
    engine.submit_limit("ask-1", is_buy=False, qty=100, price=10.0, timestamp=at(0))

    report = engine.submit_limit(
        "buy-post",
        is_buy=True,
        qty=10,
        price=10.0,
        post_only=True,
        timestamp=at(1),
    )

    assert report.events[-1]["status"] == STATUS_REJECTED
    assert report.events[-1]["reason"] == "post_only_would_cross"
    assert report.trades == []
    assert engine.orderbook.best_ask == (10.0, 100)


def test_ioc_matches_then_cancels_remainder():
    engine = AdvancedEngine(ticker="band6stock")
    engine.submit_limit("ask-1", is_buy=False, qty=100, price=10.0, timestamp=at(0))

    report = engine.submit_limit(
        "buy-ioc",
        is_buy=True,
        qty=150,
        price=10.0,
        time_in_force=TIF_IOC,
        timestamp=at(1),
    )

    assert report.events[-1]["status"] == STATUS_PARTIALLY_FILLED_CANCELED
    assert report.events[-1]["traded_qty"] == 100
    assert report.events[-1]["canceled_qty"] == 50
    assert len(report.trades) == 1
    assert engine.orderbook.best_bid is None


def test_fok_rejects_when_full_quantity_is_not_available():
    engine = AdvancedEngine(ticker="band6stock")
    engine.submit_limit("ask-1", is_buy=False, qty=100, price=10.0, timestamp=at(0))

    report = engine.submit_limit(
        "buy-fok",
        is_buy=True,
        qty=150,
        price=10.0,
        time_in_force=TIF_FOK,
        timestamp=at(1),
    )

    assert report.events[-1]["status"] == STATUS_REJECTED
    assert report.events[-1]["reason"] == "fok_not_fillable"
    assert report.trades == []
    assert engine.orderbook.best_ask == (10.0, 100)


def test_fok_executes_when_full_quantity_is_available():
    engine = AdvancedEngine(ticker="band6stock")
    engine.submit_limit("ask-1", is_buy=False, qty=100, price=10.0, timestamp=at(0))
    engine.submit_limit("ask-2", is_buy=False, qty=50, price=10.0, timestamp=at(1))

    report = engine.submit_limit(
        "buy-fok",
        is_buy=True,
        qty=150,
        price=10.0,
        time_in_force=TIF_FOK,
        timestamp=at(2),
    )

    assert report.events[-1]["status"] == STATUS_FILLED
    assert report.events[-1]["traded_qty"] == 150
    assert report.events[-1]["canceled_qty"] == 0
    assert len(report.trades) == 2
    assert engine.orderbook.best_ask is None


def test_modify_downsizes_in_place_but_price_change_cancel_replaces():
    engine = AdvancedEngine(ticker="band6stock")
    engine.submit_limit("bid-1", is_buy=True, qty=100, price=9.0, timestamp=at(0))

    downsize = engine.modify("bid-1", qty=60, timestamp=at(1))
    first_native_id = engine.orders["bid-1"].native_order_id
    assert downsize.events[-1]["reason"] == "priority_preserved"
    assert downsize.events[-1]["post_leaves_qty"] == 60
    assert engine.orderbook.best_bid == (9.0, 60)

    replace = engine.modify("bid-1", qty=120, price=9.5, timestamp=at(2))
    second_native_id = engine.orders["bid-1"].native_order_id
    assert replace.events[0]["reason"] == "cancel_replace"
    assert second_native_id != first_native_id
    assert engine.orderbook.best_bid == (9.5, 120)
    assert engine.orders["bid-1"].status == STATUS_ACTIVE


def test_day_and_gtd_orders_expire_without_touching_marketsimulator():
    engine = AdvancedEngine(ticker="band6stock", day_end=time(9, 0, 10))
    engine.submit_limit(
        "day-bid",
        is_buy=True,
        qty=100,
        price=9.0,
        time_in_force=TIF_DAY,
        timestamp=at(0),
    )
    engine.submit_limit(
        "gtd-bid",
        is_buy=True,
        qty=50,
        price=8.5,
        time_in_force=TIF_GTD,
        expires_at=at(5),
        timestamp=at(0),
    )

    gtd_report = engine.advance_time(at(5))
    assert gtd_report.events[-1]["client_order_id"] == "gtd-bid"
    assert gtd_report.events[-1]["status"] == STATUS_EXPIRED
    assert engine.orders["gtd-bid"].status == STATUS_EXPIRED
    assert engine.orderbook.best_bid == (9.0, 100)

    day_report = engine.advance_time(at(10))
    assert day_report.events[-1]["client_order_id"] == "day-bid"
    assert day_report.events[-1]["status"] == STATUS_EXPIRED
    assert engine.orderbook.best_bid is None


def test_stop_limit_order_triggers_from_last_trade():
    engine = AdvancedEngine(ticker="band6stock")
    engine.submit_limit("ask-11", is_buy=False, qty=10, price=11.0, timestamp=at(0))
    engine.submit_limit("bid-trigger", is_buy=True, qty=1, price=10.5, timestamp=at(0))
    engine.submit(
        OrderRequest(
            client_order_id="stop-buy",
            is_buy=True,
            qty=5,
            price=11.0,
            order_type=ORDER_TYPE_STOP_LIMIT,
            trigger_price=10.5,
            trigger_direction=TRIGGER_ABOVE,
            timestamp=at(1),
        )
    )

    report = engine.submit_limit(
        "sell-trigger",
        is_buy=False,
        qty=1,
        price=10.5,
        timestamp=at(2),
    )

    assert any(event["event_type"] == "trigger" for event in report.events)
    assert engine.orders["stop-buy"].status == STATUS_FILLED
    assert [trade["aggressor_client_order_id"] for trade in report.trades] == [
        "sell-trigger",
        "stop-buy",
    ]
    assert report.trades[-1]["passive_client_order_id"] == "ask-11"


def test_tpsl_triggers_one_side_and_cancels_the_linked_side():
    engine = AdvancedEngine(ticker="band6stock")
    engine.submit_limit("bid-12", is_buy=True, qty=10, price=12.0, timestamp=at(0))
    report = engine.submit_tpsl(
        group_id="exit-long",
        is_buy=False,
        qty=5,
        take_profit_trigger_price=12.0,
        stop_loss_trigger_price=9.0,
        timestamp=at(1),
    )

    assert [event["status"] for event in report.events[-2:]] == [
        STATUS_PENDING_TRIGGER,
        STATUS_PENDING_TRIGGER,
    ]

    trigger_report = engine.submit_limit(
        "sell-trigger",
        is_buy=False,
        qty=1,
        price=12.0,
        timestamp=at(2),
    )

    assert engine.orders["exit-long:tp"].status == STATUS_FILLED
    assert engine.orders["exit-long:sl"].status == STATUS_CANCELED
    assert any(
        event["client_order_id"] == "exit-long:sl"
        and event["reason"] == "linked_order_triggered"
        for event in trigger_report.events
    )
    assert trigger_report.trades[-1]["aggressor_client_order_id"] == "exit-long:tp"
