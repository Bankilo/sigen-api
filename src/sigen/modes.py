"""Operational mode endpoints and dynamic method creation."""

import logging

import aiohttp

from .auth import TokenManager

logger = logging.getLogger(__name__)


async def fetch_operational_modes(base_url: str, token_mgr: TokenManager, station_id: str) -> dict:
    """Fetch all available operational modes for a station.

    GET {base}/device/energy-profile/mode/all/{station_id}
    """
    await token_mgr.ensure_valid_token(base_url)
    url = f"{base_url}device/energy-profile/mode/all/{station_id}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=token_mgr.headers) as response:
            return (await response.json())["data"]


async def get_current_operational_mode(
    base_url: str, token_mgr: TokenManager, station_id: str, cached_modes: dict
) -> str:
    """Get the label of the current operational mode.

    GET {base}/device/energy-profile/mode/current/{station_id}
    """
    await token_mgr.ensure_valid_token(base_url)
    url = f"{base_url}device/energy-profile/mode/current/{station_id}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=token_mgr.headers) as response:
            response_data = (await response.json())["data"]
            current_mode = response_data["currentMode"]
            current_profile_id = response_data["currentProfileId"]

            if current_mode != 9:
                # Default mode — match by value
                for mode in cached_modes["defaultWorkingModes"]:
                    if mode["value"] == str(current_mode):
                        return mode["label"]
            else:
                # Custom energy profile — match by profileId
                for mode in cached_modes["energyProfileItems"]:
                    if mode["profileId"] == current_profile_id:
                        return mode["name"]

            return "Unknown mode"


async def set_operational_mode(
    base_url: str, token_mgr: TokenManager, station_id: str, mode: int, profile_id: int = -1
) -> dict:
    """Set the operational mode for a station.

    PUT {base}/device/energy-profile/mode
    """
    await token_mgr.ensure_valid_token(base_url)
    url = f"{base_url}device/energy-profile/mode"
    payload = {
        "stationId": station_id,
        "operationMode": mode,
        "profileId": profile_id,
    }
    async with aiohttp.ClientSession() as session:
        async with session.put(url, headers=token_mgr.headers, json=payload) as response:
            return await response.json()


def create_dynamic_mode_methods(cls, operational_modes: dict) -> list[str]:
    """Attach set_operational_mode_{name} methods to *cls* for each mode.

    Returns the list of method names created.
    """
    created: list[str] = []

    # Default working modes
    for mode in operational_modes.get("defaultWorkingModes", []):
        method_name = f"set_operational_mode_{mode['label'].lower().replace(' ', '_').replace('-', '_')}"
        mode_value = int(mode["value"])

        def _make_default(mv):
            async def _method(self):
                await self.set_operational_mode(mv, -1)
            _method.__name__ = method_name
            return _method

        setattr(cls, method_name, _make_default(mode_value))
        created.append(method_name)

    # Custom energy profile modes
    for mode in operational_modes.get("energyProfileItems", []):
        method_name = f"set_operational_mode_{mode['name'].lower().replace(' ', '_').replace('-', '_')}"
        profile_id = mode["profileId"]

        def _make_custom(pid):
            async def _method(self):
                await self.set_operational_mode(9, pid)
            _method.__name__ = method_name
            return _method

        setattr(cls, method_name, _make_custom(profile_id))
        created.append(method_name)

    return created
