## Quality checks

Recommended checks for CI:
- Python lint and unit tests
- SQL linting for staging/mart/audit scripts
- Data quality tests in `tests/sql/`

Example command sequence:
- `python -m pytest tests/python`
- `snowsql -f tests/sql/test_incident_dates.sql`
