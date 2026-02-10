"""Energy flow endpoint."""

import aiohttp

from .auth import TokenManager


async def get_energy_flow(base_url: str, token_mgr: TokenManager, station_id: str) -> dict:
    """Fetch real-time energy flow for a station.

    GET {base}/device/sigen/station/energyflow?id={station_id}
    """
    await token_mgr.ensure_valid_token(base_url)
    url = f"{base_url}device/sigen/station/energyflow?id={station_id}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=token_mgr.headers) as response:
            return (await response.json())["data"]
