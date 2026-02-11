"""Sigenergy Cloud MQTT client for real-time telemetry.

Broker: mqtts://mqtt-eu.sigencloud.com:8883 (TLS)
Auth:   Key-based via NorthboundClient.login_with_key()
Topics: openapi/period/{appId}/{systemId}  — periodic telemetry (5min)
        openapi/change/{appId}/{systemId}  — on-change system data
        openapi/alarm/{appId}/{systemId}   — alarms
"""

import asyncio
import json
import logging
import ssl
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

import aiomqtt

from .northbound import NorthboundClient

logger = logging.getLogger(__name__)

# Default MQTT brokers per region
MQTT_BROKERS = {
    "eu": "mqtt-eu.sigencloud.com",
    "cn": "mqtt-cn.sigencloud.com",
    "apac": "mqtt-apac.sigencloud.com",
    "us": "mqtt-us.sigencloud.com",
}

# Token refresh 1 hour before expiry (tokens valid ~12h)
TOKEN_REFRESH_MARGIN_S = 3600


@dataclass
class TelemetryData:
    """Parsed telemetry from MQTT periodic messages.

    All power values are in kW, converted from Sigen's Watts.
    Sign conventions match the VPP app:
      - pv_power_kw: always >= 0
      - battery_power_kw: positive = discharging, negative = charging
        (Sigen sends +charge/-discharge, we invert)
      - grid_power_kw: positive = importing, negative = exporting
        (Sigen sends negative = export, which matches)
      - load_power_kw: always >= 0
    """

    timestamp: str = ""
    system_id: str = ""
    device_type: str = ""
    pv_power_kw: float = 0.0
    battery_power_kw: float = 0.0
    soc_percent: float = 0.0
    grid_power_kw: float = 0.0
    load_power_kw: float = 0.0
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_mqtt_payload(cls, data: dict) -> "TelemetryData":
        """Parse a single device entry from MQTT telemetry JSON.

        Sigen sends values as strings; this converts to float.
        Sigen sign convention for battery: +charge/-discharge
        Our convention: +discharge/-charge → invert.
        """
        values = data.get("value", {})

        def _float(key: str, default: float = 0.0) -> float:
            v = values.get(key)
            if v is None:
                return default
            try:
                return float(v)
            except (ValueError, TypeError):
                return default

        # Battery: Sigen +charge/-discharge → our +discharge/-charge
        sigen_battery_w = _float("storageChargeDischargePowerW")
        battery_kw = -sigen_battery_w / 1000.0

        return cls(
            timestamp=data.get("statisticsTime", ""),
            system_id=data.get("systemId", ""),
            device_type=data.get("deviceType", ""),
            pv_power_kw=_float("pvPowerW") / 1000.0,
            battery_power_kw=battery_kw,
            soc_percent=_float("storageSOC%"),
            grid_power_kw=_float("gridActivePowerW") / 1000.0,
            load_power_kw=_float("loadActivePowerW") / 1000.0,
            raw=values,
        )


# Type alias for callbacks
TelemetryCallback = Callable[[TelemetryData], Awaitable[None]]
MessageCallback = Callable[[dict], Awaitable[None]]


class SigenMQTT:
    """Async MQTT client for Sigenergy Cloud telemetry.

    Usage::

        mqtt = SigenMQTT(
            app_key="...", app_secret="...",
            app_identifier="...", system_ids=["..."],
            ca_cert_path="/path/to/ca.pem",
        )
        await mqtt.connect()
        await mqtt.listen(on_telemetry=my_callback)
    """

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        app_identifier: str,
        system_ids: list[str],
        ca_cert_path: str,
        client_cert_path: str | None = None,
        client_key_path: str | None = None,
        broker: str = "mqtt-eu.sigencloud.com",
        port: int = 8883,
        base_url: str = "https://api-eu.sigencloud.com/",
    ):
        self.app_key = app_key
        self.app_secret = app_secret
        self.app_identifier = app_identifier
        self.system_ids = system_ids
        self.ca_cert_path = ca_cert_path
        self.client_cert_path = client_cert_path
        self.client_key_path = client_key_path
        self.broker = broker
        self.port = port
        self.base_url = base_url

        self._nb_client: NorthboundClient | None = None
        self._mqtt_client: aiomqtt.Client | None = None
        self._connected = False
        self._listen_task: asyncio.Task | None = None
        self._telemetry_logged = False

    @property
    def connected(self) -> bool:
        return self._connected

    async def authenticate(self) -> str:
        """Authenticate via key-based northbound API and return access token."""
        self._nb_client = await NorthboundClient.from_app_key(
            self.base_url, self.app_key, self.app_secret
        )
        return self._nb_client.access_token

    async def _ensure_token(self) -> str:
        """Ensure we have a valid access token."""
        if not self._nb_client:
            return await self.authenticate()
        return await self._nb_client.ensure_token()

    def _build_tls_context(self) -> ssl.SSLContext:
        """Build TLS context from certificate paths."""
        ctx = ssl.create_default_context(cafile=self.ca_cert_path)
        if self.client_cert_path and self.client_key_path:
            ctx.load_cert_chain(self.client_cert_path, self.client_key_path)
        return ctx

    async def connect(self) -> None:
        """Authenticate and prepare for MQTT connection.

        Actual MQTT connection is established in listen() via aiomqtt's
        async context manager.
        """
        await self._ensure_token()
        logger.info("MQTT authenticated, ready to connect to %s:%d", self.broker, self.port)

    async def _subscribe(self, client: aiomqtt.Client) -> None:
        """Subscribe to telemetry by publishing subscription requests."""
        token = await self._ensure_token()

        # Subscribe to periodic telemetry
        sub_payload = json.dumps({
            "accessToken": token,
            "systemIdList": self.system_ids,
        })
        await client.publish("openapi/subscription/period", sub_payload)
        logger.info("Subscribed to periodic telemetry for systems: %s", self.system_ids)

        # Subscribe to on-change system data
        await client.publish("openapi/subscription/change", sub_payload)
        logger.info("Subscribed to system change events")

        # Subscribe to alarms
        await client.publish("openapi/subscription/alarm", sub_payload)
        logger.info("Subscribed to alarm events")

        # Subscribe to the allocated topic channels (topics use app_key)
        for system_id in self.system_ids:
            topics = [
                f"openapi/period/{self.app_key}/{system_id}",
                f"openapi/change/{self.app_key}/{system_id}",
                f"openapi/alarm/{self.app_key}/{system_id}",
            ]
            for t in topics:
                await client.subscribe(t)
            logger.info("Subscribed to topics for %s (app_key=%s)", system_id, self.app_key)

    async def listen(
        self,
        on_telemetry: TelemetryCallback | None = None,
        on_system: MessageCallback | None = None,
        on_alarm: MessageCallback | None = None,
    ) -> None:
        """Connect to MQTT and listen for messages indefinitely.

        Handles reconnection and token refresh automatically.
        """
        tls_ctx = self._build_tls_context()

        while True:
            try:
                token = await self._ensure_token()
                async with aiomqtt.Client(
                    hostname=self.broker,
                    port=self.port,
                    tls_context=tls_ctx,
                    username=self.app_key,
                    password=token,
                ) as client:
                    self._mqtt_client = client
                    self._connected = True
                    logger.info("MQTT connected to %s:%d", self.broker, self.port)

                    await self._subscribe(client)

                    async for message in client.messages:
                        topic = str(message.topic)
                        logger.debug("MQTT message on topic: %s (%d bytes)", topic, len(message.payload or b""))
                        try:
                            payload = json.loads(message.payload)
                        except (json.JSONDecodeError, TypeError):
                            logger.warning("Non-JSON MQTT message on %s", topic)
                            continue

                        if "/period/" in topic and on_telemetry:
                            await self._handle_telemetry(payload, on_telemetry)
                        elif "/change/" in topic and on_system:
                            await on_system(payload)
                        elif "/alarm/" in topic and on_alarm:
                            await on_alarm(payload)

            except aiomqtt.MqttError as e:
                self._connected = False
                self._mqtt_client = None
                logger.error("MQTT connection lost: %s — reconnecting in 30s", e)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self._connected = False
                self._mqtt_client = None
                logger.info("MQTT listen task cancelled")
                raise
            except Exception as e:
                self._connected = False
                self._mqtt_client = None
                logger.error("MQTT unexpected error: %s — reconnecting in 60s", e)
                await asyncio.sleep(60)

    async def _handle_telemetry(
        self, payload: Any, callback: TelemetryCallback
    ) -> None:
        """Parse telemetry payload and invoke callback for each device entry."""
        # Payload can be a list of device entries or a single dict
        entries = payload if isinstance(payload, list) else [payload]
        for entry in entries:
            try:
                # Log raw fields on first message for diagnostics
                if not self._telemetry_logged:
                    self._telemetry_logged = True
                    values = entry.get("value", {})
                    logger.info("Telemetry fields (%d): %s", len(values), ", ".join(sorted(values.keys())))
                data = TelemetryData.from_mqtt_payload(entry)
                await callback(data)
            except Exception as e:
                logger.error("Error processing telemetry entry: %s", e)

    async def send_battery_commands(self, commands: list[dict]) -> None:
        """Publish battery commands to openapi/instruction/command.

        See: developer.sigencloud.com/user/api/document/59
        Max 24 commands per batch.
        """
        if not self._mqtt_client or not self._connected:
            raise RuntimeError("MQTT not connected")
        token = await self._ensure_token()
        payload = json.dumps({
            "accessToken": token,
            "commands": commands,
        })
        await self._mqtt_client.publish("openapi/instruction/command", payload)

    async def disconnect(self) -> None:
        """Gracefully disconnect."""
        self._connected = False
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        self._mqtt_client = None
        logger.info("MQTT disconnected")
