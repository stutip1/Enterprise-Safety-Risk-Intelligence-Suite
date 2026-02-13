from dataclasses import dataclass
import os


@dataclass
class ApiConfig:
    base_url: str
    token: str
    timeout_seconds: int = 30


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str


def load_api_config() -> ApiConfig:
    return ApiConfig(
        base_url=os.environ.get("SAFETY_API_BASE_URL", "https://api.example.com"),
        token=os.environ.get("SAFETY_API_TOKEN", ""),
        timeout_seconds=int(os.environ.get("SAFETY_API_TIMEOUT", "30")),
    )


def load_snowflake_config(schema: str) -> SnowflakeConfig:
    return SnowflakeConfig(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        user=os.environ.get("SNOWFLAKE_USER", ""),
        password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
        database=os.environ.get("SNOWFLAKE_DATABASE", ""),
        schema=schema,
    )
