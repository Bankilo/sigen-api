"""Sigen Northbound (Developer) API — REST endpoints.

Auth:    POST openapi/auth/login/password   (user-based)
         POST openapi/auth/login/key        (app key-based)
Board:   POST openapi/board/onboard
         POST openapi/board/offboard
Query:   GET  openapi/instruction/{systemId}/settings
Switch:  PUT  openapi/instruction/settings
"""

import base64
import json
import logging
import time

import aiohttp

from .auth import encrypt_password
from .exceptions import SigenAPIError, SigenAuthError

logger = logging.getLogger(__name__)


class NorthboundClient:
    """Manages northbound API auth and instruction endpoints."""

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.username = username
        self.password = encrypt_password(password)
        self.access_token: str | None = None
        self.token_expiry: float | None = None

    async def login(self) -> str:
        """POST openapi/auth/login/password → accessToken."""
        url = f"{self.base_url}openapi/auth/login/password"
        payload = {"username": self.username, "password": self.password}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                body = await resp.json()
                if body.get("code") != 0:
                    raise SigenAuthError(
                        f"Northbound login failed: code={body.get('code')}, "
                        f"msg={body.get('msg')}"
                    )
                # data is a JSON string (double-encoded)
                data_str = body.get("data", "{}")
                data = json.loads(data_str) if isinstance(data_str, str) else data_str
                self.access_token = data["accessToken"]
                self.token_expiry = time.time() + data.get("expiresIn", 43199)
                return self.access_token

    @classmethod
    async def from_app_key(cls, base_url: str, app_key: str, app_secret: str) -> "NorthboundClient":
        """Create a NorthboundClient authenticated via AppKey:AppSecret.

        POST openapi/auth/login/key with {"key": base64(AppKey:AppSecret)}
        """
        instance = cls.__new__(cls)
        instance.base_url = base_url
        instance.username = None
        instance.password = None
        instance.access_token = None
        instance.token_expiry = None
        instance._app_key = app_key
        instance._app_secret = app_secret
        await instance.login_with_key()
        return instance

    async def login_with_key(self) -> str:
        """POST openapi/auth/login/key → accessToken using AppKey:AppSecret."""
        key = base64.b64encode(
            f"{self._app_key}:{self._app_secret}".encode()
        ).decode()
        url = f"{self.base_url}openapi/auth/login/key"
        payload = {"key": key}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                body = await resp.json()
                if body.get("code") != 0:
                    raise SigenAuthError(
                        f"Northbound key login failed: code={body.get('code')}, "
                        f"msg={body.get('msg')}"
                    )
                data_str = body.get("data", "{}")
                data = json.loads(data_str) if isinstance(data_str, str) else data_str
                self.access_token = data["accessToken"]
                self.token_expiry = time.time() + data.get("expiresIn", 43199)
                logger.info("Northbound key-based auth successful")
                return self.access_token

    async def ensure_token(self) -> str:
        """Ensure we have a valid access token, re-login if expired."""
        if not self.access_token or (
            self.token_expiry and time.time() >= self.token_expiry
        ):
            if hasattr(self, "_app_key"):
                await self.login_with_key()
            else:
                await self.login()
        return self.access_token

    async def onboard(self, system_ids: list[str]) -> list[dict]:
        """POST openapi/board/onboard — authorize systems for this app.

        Must be called before instruction endpoints will work.
        Returns list of {systemId, result: bool, codeList: [int]}.
        """
        token = await self.ensure_token()
        url = f"{self.base_url}openapi/board/onboard"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=system_ids, headers=headers) as resp:
                body = await resp.json()
                if body.get("code") != 0:
                    raise SigenAPIError(
                        f"Onboard failed: code={body.get('code')}, msg={body.get('msg')}"
                    )
                data = body.get("data", [])
                for item in data:
                    sid = item.get("systemId")
                    ok = item.get("result")
                    codes = item.get("codeList", [])
                    if ok:
                        logger.info("Onboarded system %s", sid)
                    else:
                        logger.warning("Onboard failed for %s: codes=%s", sid, codes)
                return data

    async def offboard(self, system_ids: list[str]) -> list[dict]:
        """POST openapi/board/offboard — revoke authorization for systems."""
        token = await self.ensure_token()
        url = f"{self.base_url}openapi/board/offboard"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=system_ids, headers=headers) as resp:
                body = await resp.json()
                if body.get("code") != 0:
                    raise SigenAPIError(
                        f"Offboard failed: code={body.get('code')}, msg={body.get('msg')}"
                    )
                data = body.get("data", [])
                for item in data:
                    logger.info("Offboarded system %s: %s", item.get("systemId"), item.get("result"))
                return data

    async def query_mode(self, system_id: str) -> int:
        """GET openapi/instruction/{systemId}/settings → mode int."""
        token = await self.ensure_token()
        url = f"{self.base_url}openapi/instruction/{system_id}/settings"
        headers = {"Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                body = await resp.json()
                if body.get("code") != 0:
                    raise SigenAPIError(f"Query mode failed: {body}")
                return body["data"]["energyStorageOperationMode"]

    async def switch_mode(self, system_id: str, mode: int) -> dict:
        """PUT openapi/instruction/settings — set operating mode."""
        token = await self.ensure_token()
        url = f"{self.base_url}openapi/instruction/settings"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "systemId": system_id,
            "energyStorageOperationMode": mode,
        }
        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=payload, headers=headers) as resp:
                body = await resp.json()
                if body.get("code") != 0:
                    raise SigenAPIError(f"Switch mode failed: {body}")
                return body
