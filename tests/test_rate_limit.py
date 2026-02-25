from rate_limit import InMemoryRateLimiter


def test_rate_limiter_blocks_after_limit():
    rl = InMemoryRateLimiter(window_sec=60, rules={"default": 2, "api": 2, "search": 2, "download": 2, "login": 2})
    r1 = rl.check(identity="1.2.3.4", path="/api/search")
    r2 = rl.check(identity="1.2.3.4", path="/api/search")
    r3 = rl.check(identity="1.2.3.4", path="/api/search")
    assert r1["allowed"] is True
    assert r2["allowed"] is True
    assert r3["allowed"] is False
    assert r3["rule"] == "search"
    assert r3["retry_after"] >= 1


def test_rate_limiter_uses_separate_rules():
    rl = InMemoryRateLimiter(window_sec=60, rules={"default": 5, "api": 3, "search": 1, "download": 2, "login": 1})
    assert rl.check(identity="ip", path="/login")["rule"] == "login"
    assert rl.check(identity="ip", path="/api/download")["rule"] == "download"
    assert rl.check(identity="ip", path="/api/search/audiobooks")["rule"] == "search"
