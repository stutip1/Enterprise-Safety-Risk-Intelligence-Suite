import json
from typing import Iterable

from src.etl.api_client import ApiClient
from src.utils.config import load_api_config
from src.utils.logging import get_logger
from src.utils.time import isoformat_utc


def fetch_near_miss(client: ApiClient) -> Iterable[dict]:
    return client.paginate("near-miss")


def write_raw_near_miss(items: Iterable[dict], output_path: str) -> None:
    logger = get_logger("ingest_near_miss")
    with open(output_path, "w", encoding="utf-8") as handle:
        for item in items:
            payload = {
                "ingestion_ts": isoformat_utc(),
                "source_system": "safety_api",
                "data": item,
            }
            handle.write(json.dumps(payload) + "\n")
    logger.info("Wrote raw near-miss data to %s", output_path)


def main(output_path: str = "data/raw/near_miss.jsonl") -> None:
    config = load_api_config()
    client = ApiClient(config.base_url, config.token, config.timeout_seconds)
    items = fetch_near_miss(client)
    write_raw_near_miss(items, output_path)


if __name__ == "__main__":
    main()
