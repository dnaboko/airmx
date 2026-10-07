import asyncio
import logging
from typing import Any, Callable, Coroutine

from homeassistant.core import HassJob, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
import paho.mqtt.client as mqtt

_LOGGER = logging.getLogger(__name__)


class MQTTClient:
    def __init__(
        self,
        hass: HomeAssistant,
        host: str,
        port: int = 1883,
        username: str | None = None,
        password: str | None = None,
    ):
        self._hass = hass
        self._client = mqtt.Client()
        self._client.on_connect = self._mqtt_on_connect
        self._client.on_message = self._mqtt_on_message
        self._client.on_disconnect = self._mqtt_on_disconnect
        self._host = host
        self._port = port
        self._lock = asyncio.Lock()

        self.subscribe_topics: list[str] = []
        self._on_message_job: HassJob[[mqtt.MQTTMessage], Coroutine[Any, Any, None]] | None = None
        self._on_connect_job: HassJob[[], Coroutine[Any, Any, None]] | None = None
        self._on_disconnect_job: HassJob[[], Coroutine[Any, Any, None]] | None = None

        if username and password:
            self._client.username_pw_set(username, password)

    async def async_connect(self) -> None:
        result: int | None = None

        try:
            result = await self._hass.async_add_executor_job(self._client.connect, self._host, self._port)
        except OSError as err:
            _LOGGER.error(f"Failed to connect to MQTT server due to exception: {err}")

        if result is not None and result != 0:
            _LOGGER.error("Failed to connect to MQTT server: %s", mqtt.error_string(result))

        self._client.loop_start()

    async def async_disconnect(self) -> None:
        async with self._lock:
            await self._hass.async_add_executor_job(lambda: self._client.disconnect())

    async def async_publish(self, topic: str, payload: bytes) -> None:
        async with self._lock:
            msg_info = await self._hass.async_add_executor_job(self._client.publish, topic, payload)

        _LOGGER.debug(f"Transmitting message on {topic}: {payload!r}")
        self._raise_on_error(msg_info.rc)

    @property
    def connected(self) -> bool:
        return self._client.is_connected()

    @property
    def on_message(self) -> Callable[[mqtt.MQTTMessage], Coroutine[Any, Any, None]] | None:
        return self._on_message_job.target if self._on_message_job else None

    @on_message.setter
    def on_message(self, target: Callable[[mqtt.MQTTMessage], Coroutine[Any, Any, None]] | None) -> None:
        self._on_message_job = HassJob(target, "airmx mqtt on_message") if target else None

    @property
    def on_connect(self) -> Callable[[], Coroutine[Any, Any, None]] | None:
        return self._on_connect_job.target if self._on_connect_job else None

    @on_connect.setter
    def on_connect(self, target: Callable[[], Coroutine[Any, Any, None]] | None) -> None:
        self._on_connect_job = HassJob(target, "airmx mqtt on_connect") if target else None

    @property
    def on_disconnect(self) -> Callable[[], Coroutine[Any, Any, None]] | None:
        return self._on_disconnect_job.target if self._on_disconnect_job else None

    @on_disconnect.setter
    def on_disconnect(self, target: Callable[[], Coroutine[Any, Any, None]] | None) -> None:
        self._on_disconnect_job = HassJob(target, "airmx mqtt on_disconnect") if target else None

    def _mqtt_on_connect(
        self,
        _mqttc: mqtt.Client,
        _userdata: None,
        _flags: dict[str, int],
        result_code: int,
        _properties: mqtt.Properties | None = None,
    ) -> None:
        _LOGGER.info(f"Connected to MQTT server ({result_code})")

        for topic in self.subscribe_topics:
            _LOGGER.info(f"Subscribe to {topic}")
            self._client.subscribe(topic)

        # Called from the paho network thread: hand control over to the event loop
        self._hass.loop.call_soon_threadsafe(self._async_handle_connect)

    @callback
    def _async_handle_connect(self) -> None:
        if self._on_connect_job is not None:
            self._hass.async_run_hass_job(self._on_connect_job)

    def _mqtt_on_disconnect(
        self,
        _mqttc: mqtt.Client,
        _userdata: None,
        result_code: int,
        _properties: mqtt.Properties | None = None,
    ) -> None:
        _LOGGER.info(f"Disconnected from MQTT server ({result_code})")

        self._hass.loop.call_soon_threadsafe(self._async_handle_disconnect)

    @callback
    def _async_handle_disconnect(self) -> None:
        if self._on_disconnect_job is not None:
            self._hass.async_run_hass_job(self._on_disconnect_job)

    def _mqtt_on_message(self, _mqttc: mqtt.Client, _userdata: None, msg: mqtt.MQTTMessage) -> None:
        _LOGGER.debug(f"Received from MQTT: {msg.payload!r}")

        self._hass.loop.call_soon_threadsafe(self._async_handle_message, msg)

    @callback
    def _async_handle_message(self, msg: mqtt.MQTTMessage) -> None:
        if self._on_message_job is not None:
            self._hass.async_run_hass_job(self._on_message_job, msg)

    @staticmethod
    def _raise_on_error(result_code: int) -> None:
        if result_code and (message := mqtt.error_string(result_code)):
            raise HomeAssistantError(f"Error talking to MQTT: {message}")
