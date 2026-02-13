from typing import Dict, List


def validate_required_fields(record: Dict, fields: List[str]) -> List[str]:
    errors = []
    for field in fields:
        if record.get(field) in (None, ""):
            errors.append(f"missing_{field}")
    return errors


def validate_date_range(
    record: Dict, field: str, min_year: int, max_year: int
) -> List[str]:
    errors = []
    value = record.get(field)
    if value is None:
        return errors
    year = value.year if hasattr(value, "year") else None
    if year is None or year < min_year or year > max_year:
        errors.append(f"invalid_{field}")
    return errors
