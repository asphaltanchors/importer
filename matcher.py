# matcher.py

def normalize_company_name(name: str) -> str:
    """
    Stub for company‐name normalization.
    Right now just trims and lower‐cases;
    replace with your full matching logic later.
    """
    if not name:
        return ""
    return name.strip().lower()