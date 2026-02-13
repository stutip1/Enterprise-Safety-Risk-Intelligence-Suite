import json
from typing import Iterable

from src.etl.api_client import ApiClient
from src.utils.config import load_api_config
from src.utils.logging import get_logger
from src.utils.time import isoformat_utc


def fetch_observations(client: ApiClient) -> Iterable[dict]:
    return client.paginate("safety-observations")


def write_raw_observations(items: Iterable[dict], output_path: str) -> None:
    logger = get_logger("ingest_observations")
    with open(output_path, "w", encoding="utf-8") as handle:
        for item in items:
            payload = {
                "ingestion_ts": isoformat_utc(),
                "source_system": "safety_api",
                "data": item,
            }
            handle.write(json.dumps(payload) + "\n")
    logger.info("Wrote raw safety observations to %s", output_path)


def main(output_path: str = "data/raw/safety_observations.jsonl") -> None:
    config = load_api_config()
    client = ApiClient(config.base_url, config.token, config.timeout_seconds)
    items = fetch_observations(client)
    write_raw_observations(items, output_path)


if __name__ == "__main__":
    main()
