"""HTTP connector class to Bosch thermostat."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from asyncio import TimeoutError as AsyncTimeout
from collections import deque
from datetime import datetime

from aiohttp.client_exceptions import (
    ClientConnectorError,
    ClientError,
    ClientResponseError,
)

from bosch_thermostat_client.const import APP_JSON, GET, PUT
from bosch_thermostat_client.const.ivt import HTTP_HEADER, IVT
from bosch_thermostat_client.exceptions import DeviceException, ResponseException

_LOGGER = logging.getLogger(__name__)

# Embedded Bosch/IVT gateways have a tiny HTTP stack. Space requests out and,
# when the gateway stops answering, stop talking to it for a while instead of
# queueing up timeouts against it.
MIN_REQUEST_GAP = 0.5  # seconds between the end of one request and the next
FAILURES_TO_OPEN = 3  # consecutive transport failures before backing off
COOLDOWN_START = 120  # seconds of silence after the first trip
COOLDOWN_MAX = 1800  # cap for the doubling cooldown
SLOW_RESPONSE = 5.0  # seconds; responses slower than this are logged
HISTORY_SIZE = 300  # requests kept in the flight recorder


class _GatewayHealth:
    """Per-host request history and back-off state.

    Kept at module level, keyed by host, so it survives Home Assistant
    re-creating the gateway object on every setup retry.
    """

    def __init__(self) -> None:
        self.history: deque = deque(maxlen=HISTORY_SIZE)
        self.consecutive_failures = 0
        self.open_until = 0.0
        self.cooldown = COOLDOWN_START
        self.last_request_end = 0.0
        self.last_success: str | None = None
        self.total_requests = 0
        self.total_failures = 0
        self.lock = asyncio.Lock()

    def record(self, method: str, path: str, outcome: str, elapsed: float) -> None:
        self.total_requests += 1
        self.history.append(
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                method,
                path,
                outcome,
                round(elapsed * 1000),
            )
        )

    def dump(self, reason: str) -> None:
        recent = list(self.history)
        _LOGGER.warning(
            "Bosch gateway flight recorder (%s). Last success: %s. "
            "Total requests since start: %s, failures: %s. "
            "Last %s requests (time | method | path | outcome | ms):\n%s",
            reason,
            self.last_success,
            self.total_requests,
            self.total_failures,
            len(recent),
            "\n".join(" | ".join(str(f) for f in row) for row in recent),
        )


_HEALTH: dict[str, _GatewayHealth] = {}


def _health_for(host: str) -> _GatewayHealth:
    if host not in _HEALTH:
        _HEALTH[host] = _GatewayHealth()
    return _HEALTH[host]


class _TransportFailure(DeviceException):
    """Gateway did not answer at all (timeout, refused, reset)."""


class HttpConnector:
    """HTTP connector to Bosch thermostat."""

    def __init__(self, host, encryption, device_type=IVT, **kwargs):
        """Init of HTTP connector."""
        self._host = host
        self._websession = kwargs.get("loop")
        self._request_timeout = 10
        self._encryption = encryption
        self.device_type = device_type
        self._health = _health_for(host)
        # One request at a time per gateway, even across connector instances.
        self._lock = self._health.lock

    @property
    def encryption_key(self):
        return self._encryption.key

    @property
    def health(self) -> _GatewayHealth:
        return self._health

    def dump_history(self, reason: str = "requested") -> None:
        """Log the recent request history."""
        self._health.dump(reason)

    async def _request(self, method, path, **kwargs):
        async def get_response(method_name, res):
            if method_name == PUT:
                data = await res.text()
                if not data and res.status == 204:
                    return True
                return data
            if (
                method_name == GET
                and res.status == 200
                and res.content_type == APP_JSON
            ):
                data = await res.json(loads=self._encryption.json_decrypt)
                return data
            raise ResponseException(res)

        try:
            async with method(self._format_url(path), **kwargs) as res:
                return await get_response(method.__name__, res)
        except ClientResponseError as err:
            raise DeviceException(f"URI {path} doesn not exist: {err}")
        except ClientConnectorError as err:
            raise _TransportFailure(f"Can't connect to {path}: {err}")
        except ResponseException as err:
            raise DeviceException(f"Error requesting data from {path}: {err}")
        except AsyncTimeout:
            raise _TransportFailure(f"Connection timed out for {path}.")
        except ClientError as err:
            # Disconnects, resets, payload errors: the gateway is struggling.
            raise _TransportFailure(f"Error connecting to client {path}: {err}")

    async def _guarded(self, method_name, path, **kwargs):
        """Pace, time, record and circuit-break a single request."""
        health = self._health
        async with self._lock:
            now = time.monotonic()
            if now < health.open_until:
                raise DeviceException(
                    f"Gateway {self._host} is backing off for another "
                    f"{int(health.open_until - now)}s after repeated failures; "
                    f"skipped {method_name.upper()} {path}."
                )
            wait = MIN_REQUEST_GAP - (now - health.last_request_end)
            if wait > 0:
                await asyncio.sleep(wait)

            method = getattr(self._websession, method_name)
            _LOGGER.debug("Sending %s request to %s", method_name.upper(), path)
            start = time.monotonic()

            def finish(outcome: str) -> None:
                elapsed = time.monotonic() - start
                health.last_request_end = time.monotonic()
                health.record(method_name.upper(), path, outcome, elapsed)
                if elapsed > SLOW_RESPONSE:
                    _LOGGER.warning(
                        "Bosch gateway slow: %s %s took %.1fs (%s).",
                        method_name.upper(),
                        path,
                        elapsed,
                        outcome,
                    )
                else:
                    _LOGGER.debug(
                        "%s %s finished in %dms (%s).",
                        method_name.upper(),
                        path,
                        elapsed * 1000,
                        outcome,
                    )

            try:
                result = await self._request(method, path, **kwargs)
            except _TransportFailure as err:
                finish(f"FAIL: {err}")
                self._on_transport_failure(method_name, path, err)
                raise
            except DeviceException as err:
                # The gateway answered (404, wrong content type...): it is alive.
                finish(f"error: {err}")
                self._on_alive()
                raise
            except Exception as err:
                # E.g. decryption errors. Record, but don't count as an outage.
                finish(f"error: {err.__class__.__name__}: {err}")
                raise
            finish("ok")
            self._on_alive()
            return result

    def _on_alive(self) -> None:
        health = self._health
        if health.consecutive_failures >= FAILURES_TO_OPEN:
            _LOGGER.warning(
                "Bosch gateway %s is answering again after back-off.", self._host
            )
        health.consecutive_failures = 0
        health.cooldown = COOLDOWN_START
        health.last_success = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _on_transport_failure(self, method_name, path, err) -> None:
        health = self._health
        health.consecutive_failures += 1
        health.total_failures += 1
        if health.consecutive_failures == 1:
            _LOGGER.warning(
                "Bosch gateway %s did not answer %s %s: %s",
                self._host,
                method_name.upper(),
                path,
                err,
            )
        if health.consecutive_failures == FAILURES_TO_OPEN:
            # First trip of this outage: the history leading up to it is
            # what we need to see.
            health.dump(
                f"{FAILURES_TO_OPEN} consecutive failures, gateway {self._host}"
            )
        if health.consecutive_failures >= FAILURES_TO_OPEN:
            health.open_until = time.monotonic() + health.cooldown
            _LOGGER.warning(
                "Bosch gateway %s unreachable (%s consecutive failures). "
                "Pausing all requests for %ss.",
                self._host,
                health.consecutive_failures,
                health.cooldown,
            )
            health.cooldown = min(health.cooldown * 2, COOLDOWN_MAX)

    def _format_url(self, path):
        """Format URL to make requests to gateway."""
        return f"http://{self._host}{path}"

    def set_timeout(self, timeout=10):
        """Set timeout for API calls."""
        self._request_timeout = timeout

    async def get(self, path):
        """Get message from API with given path."""
        data = await self._guarded(
            "get",
            path,
            headers=HTTP_HEADER,
            timeout=self._request_timeout,
            skip_auto_headers=["Accept-Encoding", "Accept"],
            raise_for_status=True,
        )
        _LOGGER.debug("Response to GET request %s: %s", path, json.dumps(data))
        return data

    async def put(self, path, value):
        """Send message to API with given path."""
        return await self._guarded(
            "put",
            path,
            data=self._encryption.encrypt(json.dumps({"value": value})),
            headers=HTTP_HEADER,
            timeout=self._request_timeout,
        )

    async def close(self, force=False):
        if force:
            await self._websession.close()
