import argparse
import json
from typing import Iterable

from src.utils.config import load_snowflake_config
from src.utils.logging import get_logger
from src.utils.snowflake import connect, execute_batch


def read_json_lines(path: str) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def load_raw_payloads(path: str, table: str) -> None:
    logger = get_logger("load_raw")
    config = load_snowflake_config("raw")
    rows = []
    for payload in read_json_lines(path):
        rows.append(
            (
                json.dumps(payload.get("data")),
                payload.get("source_system"),
                payload.get("ingestion_ts"),
            )
        )

    if not rows:
        logger.info("No rows to load for %s", table)
        return

    statement = f"insert into raw.{table} (payload, source_system, ingestion_ts, record_hash) values (parse_json(%s), %s, %s, sha2(%s))"
    connection = connect(config)
    execute_batch(connection, statement, [(r[0], r[1], r[2], r[0]) for r in rows])
    connection.close()
    logger.info("Loaded %s rows into raw.%s", len(rows), table)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()
    load_raw_payloads(args.path, args.table)


if __name__ == "__main__":
    main()
