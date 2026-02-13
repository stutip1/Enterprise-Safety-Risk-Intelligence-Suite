from typing import Dict, Iterable, Optional
import requests

from src.utils.logging import get_logger


class ApiClient:
    def __init__(self, base_url: str, token: str, timeout_seconds: int = 30):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.logger = get_logger(self.__class__.__name__)

    def get(self, path: str, params: Optional[Dict[str, str]] = None) -> Dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(
            url, headers=headers, params=params, timeout=self.timeout_seconds
        )
        response.raise_for_status()
        return response.json()

    def paginate(
        self, path: str, params: Optional[Dict[str, str]] = None
    ) -> Iterable[Dict]:
        page = 1
        while True:
            page_params = dict(params or {})
            page_params["page"] = page
            payload = self.get(path, page_params)
            items = payload.get("data", [])
            if not items:
                break
            for item in items:
                yield item
            page += 1
