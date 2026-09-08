from streamforge.processor.state_store import LocalStateStore


def test_local_state_store_persists_and_reads_values(tmp_path):
    store = LocalStateStore(tmp_path)
    store["cust_01:truck_01"] = {"window_start": 100, "sample_count": 2}

    assert store["cust_01:truck_01"]["sample_count"] == 2
    assert "cust_01:truck_01" in store

    reloaded = LocalStateStore(tmp_path)
    assert reloaded["cust_01:truck_01"]["sample_count"] == 2
    assert reloaded.get("missing", "fallback") == "fallback"
