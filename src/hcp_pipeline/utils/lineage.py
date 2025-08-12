from datetime import datetime

def lineage_fields(source: str, now: datetime|None=None) -> dict:
    now = now or datetime.utcnow()
    # You can expand this to include run ids, user, git sha, etc.
    return {
        "first_seen_source": source,
        "last_seen_source": source,
        "ingest_date": now.date().isoformat(),
    }
