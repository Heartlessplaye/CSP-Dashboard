import random

from csp_traffic.config import LimitsConfig
from csp_traffic.ratelimit import EXCEEDED, NO_LIMIT, OK, WARNING, RateLimiter

T0 = 1_743_760_800  # 2025-04-04T10:00:00Z (hour-aligned)


def limits(**clients) -> LimitsConfig:
    return LimitsConfig.model_validate(
        {"default": {"limit": 10, "window_seconds": 60}, "clients": {k: v for k, v in clients.items()}}
    )


def feed(rl: RateLimiter, cid: str, secs: dict[int, int]) -> set[str]:
    return rl.ingest({cid: secs}, max(secs))


def test_ok_warning_exceeded_and_recovery_by_sliding():
    rl = RateLimiter(limits(), warning_ratio=0.8)
    feed(rl, "a", {T0: 5})
    assert rl.status("a")["status"] == OK
    feed(rl, "a", {T0 + 10: 3})
    assert rl.status("a")["status"] == WARNING and rl.status("a")["current"] == 8
    feed(rl, "a", {T0 + 20: 3})
    s = rl.status("a")
    assert s["status"] == EXCEEDED and s["current"] == 11 and s["exceeded_since"] is not None
    assert [v["client_id"] for v in rl.violations()] == ["a"]
    # another client's traffic moves the watermark; T0's 5 requests slide out of a's window
    rl.ingest({"b": {T0 + 61: 1}}, T0 + 61)
    rl.tick()
    s = rl.status("a")
    assert s["current"] == 6 and s["status"] == OK and s["peak"] == 11
    assert s["exceeded_since"] is None and s["last_exceeded_at"] is not None


def test_per_client_overrides_and_unlimited():
    rl = RateLimiter(limits(vip={"limit": 100}, svc={"limit": None}, fast={"window_seconds": 3600}))
    feed(rl, "vip", {T0: 50})
    feed(rl, "svc", {T0: 10_000})
    feed(rl, "fast", {T0: 5})
    assert rl.status("vip") | {} == rl.status("vip")
    assert rl.status("vip")["limit"] == 100 and rl.status("vip")["status"] == OK
    assert rl.status("svc")["status"] == NO_LIMIT
    assert rl.status("fast")["window_seconds"] == 3600 and rl.status("fast")["limit"] == 10


def test_spike_inside_one_batch_is_detected():
    rl = RateLimiter(limits())
    # 20 requests in one second, then nothing for 5 minutes - all inside a single ingest call
    feed(rl, "a", {T0: 20, T0 + 300: 1})
    s = rl.status("a")
    assert s["status"] == OK and s["current"] == 1
    assert s["peak"] == 20 and s["last_exceeded_at"] is not None


def test_old_records_do_not_count_toward_window():
    rl = RateLimiter(limits())
    feed(rl, "a", {T0 + 600: 1})
    feed(rl, "a", {T0: 50})  # 10 minutes late: outside the 60 s window
    assert rl.status("a")["current"] == 1


def test_hot_reload_changes_limit_and_window():
    rl = RateLimiter(limits())
    feed(rl, "a", {T0: 8})
    rl.set_limits(limits(a={"limit": 5}))
    rl.tick()
    assert rl.status("a")["status"] == EXCEEDED
    rl.set_limits(limits(a={"limit": 5, "window_seconds": 120}))
    rl.tick()
    assert rl.status("a")["window_seconds"] == 120 and rl.status("a")["current"] == 0


def test_window_count_matches_brute_force():
    rng = random.Random(7)
    window = 3600
    rl = RateLimiter(LimitsConfig.model_validate({"default": {"limit": 10**9, "window_seconds": window}}))
    events: list[int] = []
    t = T0
    for _ in range(300):
        batch: dict[int, int] = {}
        for _ in range(rng.randint(1, 20)):
            sec = t - rng.randint(0, 1800)  # out-of-order within the last 30 min
            batch[sec] = batch.get(sec, 0) + 1
            events.append(sec)
        t += rng.randint(0, 120)
        rl.ingest({"c": batch}, max(batch))
        rl.tick()
        bucket = window // 60
        wm_bucket = int(rl.watermark // bucket)
        expected = sum(1 for e in events if wm_bucket - 60 < e // bucket <= wm_bucket)
        assert rl.status("c")["current"] == expected
