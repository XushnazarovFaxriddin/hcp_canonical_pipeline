from hashlib import sha1

def stable_site_id(line1: str, city: str, state: str, postal: str, line2: str|None=None) -> str:
    parts = [line1 or "", city or "", state or "", postal or "", line2 or ""]
    key = "|".join([p.strip().upper().replace("  ", " ") for p in parts if p is not None])
    return sha1(key.encode("utf-8")).hexdigest()[:16]
