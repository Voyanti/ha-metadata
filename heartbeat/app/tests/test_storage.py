from heartbeat.models import CheckResult
from heartbeat.storage import Outbox


def _result(name="cloudflare", success=True) -> CheckResult:
    return CheckResult(
        observed_at="2026-01-01T00:00:00+00:00",
        check="ping",
        target={"type": "public_dns", "name": name, "host": "1.1.1.1"},
        success=success,
        rc=0 if success else 1,
        error=None if success else "ping_failed",
        details={"stdout": "", "stderr": ""},
    )


def make_outbox() -> Outbox:
    ob = Outbox(":memory:")
    ob.init()
    return ob


def test_enqueue_and_count():
    ob = make_outbox()
    ob.enqueue(_result())
    ob.enqueue(_result())
    assert ob.count() == 2


def test_fetch_pending_order_and_limit():
    ob = make_outbox()
    ids = [ob.enqueue(_result(name=f"t{i}")) for i in range(5)]
    rows = ob.fetch_pending(limit=3)
    assert [r.id for r in rows] == ids[::-1][:3]  # newest first (LIFO)
    assert rows[0].target_type == "public_dns"
    assert rows[0].target_name == "t4"


def test_delete():
    ob = make_outbox()
    a = ob.enqueue(_result())
    b = ob.enqueue(_result())
    ob.delete([a])
    assert [r.id for r in ob.fetch_pending()] == [b]


def test_increment_attempts():
    ob = make_outbox()
    rid = ob.enqueue(_result())
    ob.increment_attempts([rid])
    ob.increment_attempts([rid])
    row = ob.conn.execute("SELECT attempts FROM outbox WHERE id=?", (rid,)).fetchone()
    assert row["attempts"] == 2


def test_trim_backlog_drops_oldest():
    ob = make_outbox()
    ids = [ob.enqueue(_result(name=f"t{i}")) for i in range(10)]
    dropped = ob.trim_backlog(max_rows=4)
    assert dropped == 6
    assert ob.count() == 4
    remaining = [r.id for r in ob.fetch_pending()]
    assert sorted(remaining) == ids[-4:]  # newest survive (LIFO fetch order)


def test_trim_backlog_noop_when_under_cap():
    ob = make_outbox()
    ob.enqueue(_result())
    assert ob.trim_backlog(max_rows=100) == 0
    assert ob.count() == 1


def test_record_columns_and_payload():
    ob = make_outbox()
    ob.enqueue(_result(success=False))
    row = ob.conn.execute("SELECT * FROM outbox").fetchone()
    assert row["check_kind"] == "ping"
    assert row["success"] == 0
    assert row["error"] == "ping_failed"
    assert row["target_host"] == "1.1.1.1"
    assert '"check":"ping"' in row["payload_json"]
