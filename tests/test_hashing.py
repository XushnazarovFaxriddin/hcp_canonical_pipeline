from src.hcp_pipeline.utils.hashing import stable_site_id

def test_stable_site_id_deterministic():
    a = stable_site_id("100 Main St", "Austin", "TX", "78701", None)
    b = stable_site_id("100 MAIN ST", "austin", "tx", "78701", "")
    assert a == b
    assert len(a) == 16
