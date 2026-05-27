import pandas as pd

from extensions.l3_recorder import L3ReplayRecorder


def test_l3_recorder_exports_inputs_and_trade_results():
    orders = pd.DataFrame(
        [
            {
                "ordtype": "new",
                "uid": 1,
                "is_buy": False,
                "qty": 100,
                "price": 10.0,
                "timestamp": pd.Timestamp("2019-05-23 09:00:00"),
            },
            {
                "ordtype": "new",
                "uid": 2,
                "is_buy": False,
                "qty": 50,
                "price": 10.0,
                "timestamp": pd.Timestamp("2019-05-23 09:00:01"),
            },
            {
                "ordtype": "new",
                "uid": 3,
                "is_buy": True,
                "qty": 120,
                "price": 10.0,
                "timestamp": pd.Timestamp("2019-05-23 09:00:02"),
            },
            {
                "ordtype": "modif",
                "uid": 2,
                "is_buy": None,
                "qty": 10,
                "price": None,
                "timestamp": pd.Timestamp("2019-05-23 09:00:03"),
            },
            {
                "ordtype": "cancel",
                "uid": 2,
                "is_buy": None,
                "qty": None,
                "price": None,
                "timestamp": pd.Timestamp("2019-05-23 09:00:04"),
            },
        ]
    )

    recorder = L3ReplayRecorder(ticker="band6stock")
    recorder.replay_dataframe(orders)

    assert [event["event_type"] for event in recorder.input_events] == [
        "new",
        "new",
        "new",
        "modify",
        "cancel",
    ]
    assert recorder.input_events[2]["traded_qty"] == 120
    assert recorder.input_events[2]["post_leaves_qty"] == 0
    assert recorder.input_events[3]["pre_leaves_qty"] == 30
    assert recorder.input_events[3]["post_leaves_qty"] == 20
    assert recorder.input_events[4]["pre_leaves_qty"] == 20
    assert recorder.input_events[4]["post_active"] is False

    assert len(recorder.trade_events) == 2
    assert recorder.trade_events[0]["aggressor_order_id"] == 3
    assert recorder.trade_events[0]["passive_order_id"] == 1
    assert recorder.trade_events[0]["qty"] == 100
    assert recorder.trade_events[0]["aggressor_remaining_qty"] == 20
    assert recorder.trade_events[0]["passive_remaining_qty"] == 0
    assert recorder.trade_events[1]["passive_order_id"] == 2
    assert recorder.trade_events[1]["qty"] == 20
    assert recorder.trade_events[1]["aggressor_remaining_qty"] == 0
    assert recorder.trade_events[1]["passive_remaining_qty"] == 30
