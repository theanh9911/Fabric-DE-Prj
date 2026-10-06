import datetime as dt

from companya_de.config import PlatformSettings
from companya_de.env import new_run_context

SETTINGS = PlatformSettings("lh_platform", "Asia/Ho_Chi_Minh")


def test_default_load_date_is_yesterday_in_business_timezone():
    # 2026-01-01 18:30 UTC = 2026-01-02 01:30 giờ VN → hôm qua theo giờ VN là 2026-01-01
    ctx = new_run_context(settings=SETTINGS, now=dt.datetime(2026, 1, 1, 18, 30, tzinfo=dt.UTC))
    assert ctx.load_date == dt.date(2026, 1, 1)


def test_explicit_parameters_win():
    ctx = new_run_context("2026-03-15", "abc", settings=SETTINGS)
    assert (ctx.load_date, ctx.run_id) == (dt.date(2026, 3, 15), "abc")


def test_landing_path_and_batch_id():
    ctx = new_run_context("2026-01-02", "r1", settings=SETTINGS)
    batch = ctx.batch_id("retail", "orders")
    assert batch == "retail_orders_20260102_r1"
    assert ctx.landing_path("retail", "orders", batch) == (
        "Files/landing/retail/orders/load_date=2026-01-02/batch=retail_orders_20260102_r1"
    )
