from typing import List

from src.utils.logging import get_logger


def log_quality_results(
    dataset: str, check_name: str, failures: int, details: str
) -> None:
    logger = get_logger("dq_audit")
    status = "PASS" if failures == 0 else "FAIL"
    logger.info(
        "%s %s %s failures=%s details=%s",
        dataset,
        check_name,
        status,
        failures,
        details,
    )


def summarize_errors(errors: List[str]) -> str:
    return ",".join(sorted(set(errors)))
