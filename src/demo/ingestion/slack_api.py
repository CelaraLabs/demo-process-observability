from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


@dataclass
class SlackClient:
    token: str
    base_url: str = "https://slack.com/api"

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def conversations_list(self, cursor: Optional[str] = None, types: str = "public_channel,private_channel", limit: int = 200) -> Dict[str, Any]:
        params = {"types": types, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(f"{self.base_url}/conversations.list", headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def conversations_history(self, channel: str, oldest: Optional[str] = None, latest: Optional[str] = None, cursor: Optional[str] = None, limit: int = 200) -> Dict[str, Any]:
        params: Dict[str, Any] = {"channel": channel, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        if oldest:
            params["oldest"] = oldest
        if latest:
            params["latest"] = latest
        resp = requests.get(f"{self.base_url}/conversations.history", headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        # Handle rate limit suggestion
        if resp.status_code == 429 and resp.headers.get("Retry-After"):
            raise RuntimeError("Rate limited")
        return resp.json()

    def users_info(self, user: str) -> Dict[str, Any]:
        params = {"user": user}
        resp = requests.get(f"{self.base_url}/users.info", headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def conversations_info(self, channel: str) -> Dict[str, Any]:
        params = {"channel": channel}
        resp = requests.get(f"{self.base_url}/conversations.info", headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
