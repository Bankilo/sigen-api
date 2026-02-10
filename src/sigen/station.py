"""Station info endpoint."""

import logging

import aiohttp

from .auth import TokenManager

logger = logging.getLogger(__name__)


async def fetch_station_info(base_url: str, token_mgr: TokenManager) -> dict:
    """Fetch station info (stationId, serial numbers, capabilities).

    GET {base}/device/owner/station/home
    """
    await token_mgr.ensure_valid_token(base_url)
    url = f"{base_url}device/owner/station/home"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=token_mgr.headers) as response:
            data = (await response.json())["data"]

            logger.debug("Station ID: %s", data["stationId"])
            logger.debug("Has PV: %s", data["hasPv"])
            logger.debug("Has EV: %s", data["hasEv"])
            logger.debug("hasAcCharger: %s", data["hasAcCharger"])
            logger.debug("acSnList: %s", data["acSnList"])
            logger.debug("dcSnList: %s", data["dcSnList"])
            logger.debug("On Grid: %s", data["onGrid"])
            logger.debug("PV Capacity: %s kW", data["pvCapacity"])
            logger.debug("Battery Capacity: %s kWh", data["batteryCapacity"])

            return data
