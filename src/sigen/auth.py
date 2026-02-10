"""Authentication: AES password encryption and OAuth2 token management."""

import base64
import logging
import time

import aiohttp
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from .constants import (
    OAUTH_CLIENT_ID,
    OAUTH_CLIENT_SECRET,
    PASSWORD_AES_IV,
    PASSWORD_AES_KEY,
)
from .exceptions import SigenAuthError, SigenTokenExpiredError

logger = logging.getLogger(__name__)


def encrypt_password(password: str) -> str:
    """Encrypt a password using AES-CBC for the Sigenergy OAuth flow."""
    cipher = AES.new(
        PASSWORD_AES_KEY.encode("utf-8"),
        AES.MODE_CBC,
        PASSWORD_AES_IV.encode("latin1"),
    )
    encrypted = cipher.encrypt(pad(password.encode("utf-8"), AES.block_size))
    return base64.b64encode(encrypted).decode("utf-8")


class TokenManager:
    """Manages OAuth2 access/refresh tokens for the Sigenergy API."""

    def __init__(self):
        self.access_token: str | None = None
        self.refresh_token: str | None = None
        self.token_expiry: float | None = None

    @property
    def headers(self) -> dict[str, str]:
        """Return auth headers for API requests."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    async def get_access_token(self, base_url: str, username: str, encrypted_pw: str) -> None:
        """Obtain an access token via OAuth2 password grant."""
        url = f"{base_url}auth/oauth/token"
        data = {
            "username": username,
            "password": encrypted_pw,
            "grant_type": "password",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                data=data,
                auth=aiohttp.BasicAuth(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET),
            ) as response:
                if response.status == 401:
                    raise SigenAuthError(
                        f"Failed to get access token for user '{username}': "
                        f"status={response.status}, body='{await response.text()}'"
                    )
                if response.status != 200:
                    raise SigenAuthError(
                        f"Failed to get access token for user '{username}': "
                        f"status={response.status}, body='{await response.text()}'"
                    )
                response_json = await response.json()
                response_data = response_json.get("data")
                if (
                    not response_data
                    or "access_token" not in response_data
                    or "refresh_token" not in response_data
                    or "expires_in" not in response_data
                ):
                    raise SigenAuthError(
                        f"Failed to get access token for user '{username}': "
                        f"unexpected response: {response_json}"
                    )
                self.access_token = response_data["access_token"]
                self.refresh_token = response_data["refresh_token"]
                self.token_expiry = time.time() + response_data["expires_in"]

    async def refresh_access_token(self, base_url: str) -> None:
        """Refresh the access token using the stored refresh token."""
        url = f"{base_url}auth/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                data=data,
                auth=aiohttp.BasicAuth(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET),
            ) as response:
                if response.status != 200:
                    raise SigenTokenExpiredError(
                        f"Failed to refresh access token: "
                        f"status={response.status}, body='{await response.text()}'"
                    )
                response_json = await response.json()
                response_data = response_json.get("data")
                if (
                    not response_data
                    or "access_token" not in response_data
                    or "refresh_token" not in response_data
                    or "expires_in" not in response_data
                ):
                    raise SigenTokenExpiredError(
                        f"Failed to refresh access token: unexpected response: {response_json}"
                    )
                self.access_token = response_data["access_token"]
                self.refresh_token = response_data["refresh_token"]
                self.token_expiry = time.time() + response_data["expires_in"]

    async def ensure_valid_token(self, base_url: str) -> None:
        """Refresh the token if it has expired."""
        if self.token_expiry is not None and time.time() >= self.token_expiry:
            await self.refresh_access_token(base_url)
