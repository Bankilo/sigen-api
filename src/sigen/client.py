"""Main Sigen client — public interface and orchestration."""

import logging

from .auth import TokenManager, encrypt_password
from .constants import REGION_BASE_URLS
from .energy import get_energy_flow as _get_energy_flow
from .modes import (
    create_dynamic_mode_methods,
    fetch_operational_modes as _fetch_operational_modes,
    get_current_operational_mode,
    set_operational_mode as _set_operational_mode,
)
from .smart_loads import (
    fetch_smart_load_details,
    fetch_smart_load_list,
    get_smart_loads_with_consumption,
    set_smart_load_state as _set_smart_load_state,
)
from .station import fetch_station_info as _fetch_station_info

logger = logging.getLogger(__name__)


class Sigen:
    """Async client for the Sigenergy cloud API.

    Usage::

        api = Sigen("user@example.com", "password", region="eu")
        await api.async_initialize()
        flow = await api.get_energy_flow()
    """

    def __init__(self, username: str, password: str, region: str = "eu"):
        if region not in REGION_BASE_URLS:
            raise ValueError(
                f"Unsupported region '{region}'. "
                f"Supported regions: {', '.join(REGION_BASE_URLS)}"
            )

        self.username = username
        self._raw_password = password
        self.password = encrypt_password(password)
        self.base_url = REGION_BASE_URLS[region]

        self._token_mgr = TokenManager()
        self._nb_client = None  # NorthboundClient, initialized on demand

        # Populated by async_initialize / fetch_station_info
        self.station_id: str | None = None
        self.ac_sn: str | None = None
        self.dc_sn: str | None = None

        # Populated lazily
        self.operational_modes: dict | None = None
        self.smart_loads: list = []
        self.smart_load_id_map: dict[int, int] = {}

    async def async_initialize(self) -> None:
        """Full init: authenticate → station info → smart load IDs → dynamic methods."""
        await self._token_mgr.get_access_token(self.base_url, self.username, self.password)
        await self.fetch_station_info()
        await self._fetch_smart_load_ids()
        await self._create_dynamic_methods()

    # ── Station ──────────────────────────────────────────────────────────

    async def fetch_station_info(self) -> dict:
        """Fetch station info and populate station_id, ac_sn, dc_sn."""
        data = await _fetch_station_info(self.base_url, self._token_mgr)
        self.station_id = data["stationId"]

        if data.get("hasAcCharger"):
            self.ac_sn = data["acSnList"][0] if data.get("acSnList") else None

        self.dc_sn = data["dcSnList"][0] if data.get("dcSnList") else None
        return data

    # ── Energy ───────────────────────────────────────────────────────────

    async def get_energy_flow(self) -> dict:
        """Return real-time energy flow data."""
        await self._token_mgr.ensure_valid_token(self.base_url)
        return await _get_energy_flow(self.base_url, self._token_mgr, self.station_id)

    # ── Operational modes ────────────────────────────────────────────────

    async def get_operational_modes(self) -> dict:
        """Return all operational modes (lazy-fetches on first call)."""
        if not self.operational_modes:
            await self.fetch_operational_modes()
        return self.operational_modes

    async def fetch_operational_modes(self) -> dict:
        """Fetch operational modes from the API and cache them."""
        self.operational_modes = await _fetch_operational_modes(
            self.base_url, self._token_mgr, self.station_id
        )
        return self.operational_modes

    async def get_operational_mode(self) -> str:
        """Return the label/name of the current operational mode."""
        if self.operational_modes is None:
            await self.get_operational_modes()
        return await get_current_operational_mode(
            self.base_url, self._token_mgr, self.station_id, self.operational_modes
        )

    async def set_operational_mode(self, mode: int, profile_id: int = -1) -> dict:
        """Set the station's operational mode."""
        return await _set_operational_mode(
            self.base_url, self._token_mgr, self.station_id, mode, profile_id
        )

    # ── Northbound API ────────────────────────────────────────────────────

    async def init_northbound(self) -> None:
        """Initialize northbound API client and authenticate."""
        from .northbound import NorthboundClient
        self._nb_client = NorthboundClient(
            self.base_url, self.username, self._raw_password
        )
        await self._nb_client.login()
        logger.info("Northbound API authenticated")

    async def nb_query_mode(self) -> int:
        """Query current operating mode via northbound API."""
        if not self._nb_client:
            raise RuntimeError(
                "Northbound client not initialized. Call init_northbound() first."
            )
        return await self._nb_client.query_mode(self.station_id)

    async def nb_switch_mode(self, mode: int) -> dict:
        """Switch operating mode via northbound API."""
        if not self._nb_client:
            raise RuntimeError(
                "Northbound client not initialized. Call init_northbound() first."
            )
        return await self._nb_client.switch_mode(self.station_id, mode)

    # ── Smart loads ──────────────────────────────────────────────────────

    async def get_smart_loads(self) -> list:
        """Return smart loads enriched with consumption stats."""
        self.smart_loads, self.smart_load_id_map = await get_smart_loads_with_consumption(
            self.base_url, self._token_mgr, self.station_id, self.smart_load_id_map
        )
        return self.smart_loads

    async def set_smart_load_state(self, load_path: int, state: int) -> dict:
        """Turn a smart load on (1) or off (0)."""
        return await _set_smart_load_state(
            self.base_url, self._token_mgr, self.station_id, load_path, state
        )

    # ── Internal helpers ─────────────────────────────────────────────────

    async def _fetch_smart_load_ids(self) -> None:
        """Build the load_path → smartLoadId cache."""
        await self._token_mgr.ensure_valid_token(self.base_url)
        loads = await fetch_smart_load_list(self.base_url, self._token_mgr, self.station_id)

        for load in loads:
            if "path" not in load:
                continue
            load_path = load["path"]
            load_name = load.get("name", f"Load {load_path}")
            try:
                details = await fetch_smart_load_details(
                    self.base_url, self._token_mgr, self.station_id, load_path
                )
                if details:
                    smart_load_id = details.get("smartLoadId")
                    if smart_load_id is not None:
                        self.smart_load_id_map[load_path] = smart_load_id
                        logger.debug(
                            "Cached smartLoadId %s for load %s (path: %s)",
                            smart_load_id, load_name, load_path,
                        )
            except Exception as e:
                logger.error("Error fetching smartLoadId for load %s: %s", load_name, e)

        logger.info("Cached %d smart load IDs", len(self.smart_load_id_map))

    async def _create_dynamic_methods(self) -> None:
        """Create set_operational_mode_* and enable/disable_smart_load_* methods."""
        # Mode methods
        await self.get_operational_modes()
        created = create_dynamic_mode_methods(Sigen, self.operational_modes)
        logger.debug("Created dynamic mode methods: %s", created)

        # Smart load methods
        await self.get_smart_loads()
        if self.smart_loads:
            for load in self.smart_loads:
                if "path" not in load or "name" not in load:
                    continue
                safe_name = load["name"].lower().replace(" ", "_").replace("-", "_")
                load_path = load["path"]

                # enable
                enable_name = f"enable_smart_load_{safe_name}"

                def _make_enable(path):
                    async def _method(self):
                        return await self.set_smart_load_state(path, 1)
                    _method.__name__ = enable_name
                    return _method

                setattr(Sigen, enable_name, _make_enable(load_path))

                # disable
                disable_name = f"disable_smart_load_{safe_name}"

                def _make_disable(path):
                    async def _method(self):
                        return await self.set_smart_load_state(path, 0)
                    _method.__name__ = disable_name
                    return _method

                setattr(Sigen, disable_name, _make_disable(load_path))
