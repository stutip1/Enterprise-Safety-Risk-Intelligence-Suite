import json
from typing import Iterable

from src.etl.api_client import ApiClient
from src.utils.config import load_api_config
from src.utils.logging import get_logger
from src.utils.time import isoformat_utc


def fetch_reference(client: ApiClient, endpoint: str) -> Iterable[dict]:
    return client.paginate(endpoint)


def write_reference(items: Iterable[dict], output_path: str, dataset: str) -> None:
    logger = get_logger("ingest_reference")
    with open(output_path, "w", encoding="utf-8") as handle:
        for item in items:
            payload = {
                "ingestion_ts": isoformat_utc(),
                "source_system": "safety_api",
                "dataset": dataset,
                "data": item,
            }
            handle.write(json.dumps(payload) + "\n")
    logger.info("Wrote raw %s data to %s", dataset, output_path)


def main() -> None:
    config = load_api_config()
    client = ApiClient(config.base_url, config.token, config.timeout_seconds)
    write_reference(
        fetch_reference(client, "employees"), "data/raw/employees.jsonl", "employees"
    )
    write_reference(
        fetch_reference(client, "locations"), "data/raw/locations.jsonl", "locations"
    )
    write_reference(
        fetch_reference(client, "assets"), "data/raw/assets.jsonl", "assets"
    )
    write_reference(
        fetch_reference(client, "hours-worked"),
        "data/raw/hours_worked.jsonl",
        "hours_worked",
    )


if __name__ == "__main__":
    main()
