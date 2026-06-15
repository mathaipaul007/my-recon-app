from datetime import datetime, date

def to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()
