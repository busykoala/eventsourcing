import time

from fastapi.testclient import TestClient


def test_fastapi_roundtrip(fastapi_app, caplog):
    caplog.set_level("DEBUG")
    with TestClient(fastapi_app) as client:
        resp = client.post("/items/", json={"foo": "bar"})
        assert resp.status_code == 200

        start = time.time()
        items = []
        while time.time() - start < 2:
            items = client.get("/items/").json().get("items", [])
            if items:
                break
            time.sleep(0.05)
        assert items == [{"foo": "bar"}]

    log_text = caplog.text
    assert "Retry 1/3 for ItemCreated" in log_text
    assert "→ ItemCreated @ items.events" in log_text
    assert "✓ ItemCreated handled, produced 0" in log_text
    assert "METRICS ItemCreated" in log_text
    assert "Enqueued 1 msgs to outbox" in log_text
    assert "Delivered 1 msgs to 'items.events'" in log_text
    assert "Published 1 msgs to 'items.events'" in log_text
