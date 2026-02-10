"""Smart load endpoints: list, details, consumption, and control."""

import logging

import aiohttp

from .auth import TokenManager

logger = logging.getLogger(__name__)


async def fetch_smart_load_list(base_url: str, token_mgr: TokenManager, station_id: str) -> list:
    """Fetch the basic list of smart loads for a station.

    GET {base}/device/system/device/systemDevice/card
    """
    await token_mgr.ensure_valid_token(base_url)
    url = f"{base_url}device/system/device/systemDevice/card"
    params = {"stationId": station_id, "showNewGenerator": "true"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=token_mgr.headers, params=params) as response:
            if response.status != 200:
                logger.error("Failed to get smart loads list: Status %s", response.status)
                return []
            response_json = await response.json()
            if response_json.get("code") != 0 or "data" not in response_json:
                logger.error("Invalid response when getting smart loads list: %s", response_json)
                return []
            return response_json["data"]


async def fetch_smart_load_details(
    base_url: str, token_mgr: TokenManager, station_id: str, load_path: int
) -> dict | None:
    """Fetch details for a single smart load (including smartLoadId).

    GET {base}/device/tp-device/smart-loads
    Returns the data dict, or None on failure.
    """
    url = f"{base_url}device/tp-device/smart-loads"
    params = {"stationId": station_id, "loadPath": load_path}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=token_mgr.headers, params=params) as response:
            if response.status != 200:
                return None
            body = await response.json()
            if body.get("code") != 0 or "data" not in body:
                return None
            return body["data"]


async def fetch_smart_load_consumption(
    base_url: str, token_mgr: TokenManager, station_id: str, load_path: int, smart_load_id: int
) -> dict:
    """Fetch real-time consumption stats for a smart load.

    GET {base}/data-process/sigen/station/statistics/real-time-consumption
    Returns consumption dict with todayConsumption, monthConsumption, lifetimeConsumption.
    """
    url = f"{base_url}data-process/sigen/station/statistics/real-time-consumption"
    params = {
        "stationId": station_id,
        "loadPath": load_path,
        "smartLoadId": smart_load_id,
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=token_mgr.headers, params=params) as response:
            if response.status != 200:
                return {}
            body = await response.json()
            if body.get("code") != 0 or "data" not in body:
                return {}
            return body["data"]


async def get_smart_loads_with_consumption(
    base_url: str, token_mgr: TokenManager, station_id: str, id_map: dict[int, int]
) -> tuple[list, dict[int, int]]:
    """Fetch smart loads enriched with consumption stats.

    Uses *id_map* as a cache of load_path → smartLoadId.
    Returns (smart_loads_list, updated_id_map).
    """
    await token_mgr.ensure_valid_token(base_url)
    smart_loads = await fetch_smart_load_list(base_url, token_mgr, station_id)

    for load in smart_loads:
        load["todayConsumption"] = "0.00 kWh"
        load["monthConsumption"] = "0.00 kWh"
        load["lifetimeConsumption"] = "0.00 kWh"

        if "path" not in load:
            continue

        load_path = load["path"]
        load_name = load.get("name", f"Load {load_path}")

        # Resolve smartLoadId — try cache first, then fetch
        smart_load_id = id_map.get(load_path)
        if smart_load_id is None:
            try:
                details = await fetch_smart_load_details(base_url, token_mgr, station_id, load_path)
                if details:
                    smart_load_id = details.get("smartLoadId")
                    if smart_load_id is not None:
                        id_map[load_path] = smart_load_id
                        logger.debug(
                            "Retrieved smartLoadId %s for load %s (path: %s)",
                            smart_load_id, load_name, load_path,
                        )
            except Exception as e:
                logger.error("Error fetching smartLoadId for load %s: %s", load_name, e)

        if smart_load_id is None:
            continue

        load["smartLoadId"] = smart_load_id

        # Fetch consumption
        try:
            consumption = await fetch_smart_load_consumption(
                base_url, token_mgr, station_id, load_path, smart_load_id
            )
            if consumption.get("todayConsumption"):
                load["todayConsumption"] = consumption["todayConsumption"]
            if consumption.get("monthConsumption"):
                load["monthConsumption"] = consumption["monthConsumption"]
            if consumption.get("lifetimeConsumption"):
                load["lifetimeConsumption"] = consumption["lifetimeConsumption"]
            logger.debug(
                "Added consumption for %s: today=%s, month=%s, lifetime=%s",
                load_name, load["todayConsumption"], load["monthConsumption"], load["lifetimeConsumption"],
            )
        except Exception as e:
            logger.error("Error fetching consumption for load %s: %s", load_name, e)

    return smart_loads, id_map


async def set_smart_load_state(
    base_url: str, token_mgr: TokenManager, station_id: str, load_path: int, state: int
) -> dict:
    """Turn a smart load on (1) or off (0).

    PATCH {base}/device/tp-device/smart-loads/control-mode/manual/switch?stationId=...&loadPath=...&manualSwitch=...
    """
    if state not in (0, 1):
        raise ValueError("Smart load state must be 0 (off) or 1 (on)")

    await token_mgr.ensure_valid_token(base_url)
    url = (
        f"{base_url}device/tp-device/smart-loads/control-mode/manual/switch"
        f"?stationId={station_id}&loadPath={load_path}&manualSwitch={state}"
    )
    async with aiohttp.ClientSession() as session:
        async with session.patch(url, headers=token_mgr.headers) as response:
            logger.debug("PATCH %s → %s", url, response.status)
            return await response.json()
