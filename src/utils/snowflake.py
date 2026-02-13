from typing import Iterable, Sequence
import snowflake.connector

from src.utils.config import SnowflakeConfig


def connect(config: SnowflakeConfig):
    return snowflake.connector.connect(
        account=config.account,
        user=config.user,
        password=config.password,
        role=config.role,
        warehouse=config.warehouse,
        database=config.database,
        schema=config.schema,
    )


def execute_batch(connection, statement: str, rows: Iterable[Sequence]):
    with connection.cursor() as cursor:
        cursor.executemany(statement, rows)
