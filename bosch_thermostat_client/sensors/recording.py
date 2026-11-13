from __future__ import annotations
import asyncio
import logging
from bosch_thermostat_client.const import (
    RESULT,
    URI,
    VALUE,
    INTERVAL,
    RECORDING,
    TEMP_CELSIUS,
    ENERGY_KILO_WATT_HOUR,
    ENERGY_WATT_HOUR,
)
from .sensor import Sensor
from bosch_thermostat_client.exceptions import DeviceException

from datetime import datetime, timedelta

_LOGGER = logging.getLogger(__name__)

MAX_FETCH_RANGE = timedelta(days=31)


class RecordingSensor(Sensor):
    def __init__(self, path: str, **kwargs) -> None:
        super().__init__(path=path, **kwargs)
        self._lock = asyncio.Lock()
        self._past_data = {}

        def unit_chooser():
            if any(x in path.lower() for x in ["solar"]):
                return ENERGY_WATT_HOUR
            if any(x in path.lower() for x in ["energy", "power"]):
                return ENERGY_KILO_WATT_HOUR
            if any(x in path.lower() for x in ["temp", "outdoor"]):
                return TEMP_CELSIUS
            return None

        self._unit_of_measurement = unit_chooser()

    @property
    def kind(self) -> str:
        return RECORDING

    @property
    def unit_of_measurement(self) -> str:
        return self._unit_of_measurement

    def process_results(self, result: dict, time: datetime) -> None:
        """Convert multi-level json object to one level object."""

        if result and RECORDING in result:
            result_interval = result[INTERVAL]
            time_interval = time.strftime("%Y-%m-%d")
            self._data[self.attr_id][RESULT][VALUE] = []
            if result_interval != time_interval:
                _LOGGER.warn(
                    "Different time returned by API than expected, exiting."
                )
                return
            last_hour = time.replace(minute=0, second=0, microsecond=0)
            for idx, recording in enumerate(result[RECORDING]):
                val = (
                    0
                    if recording["y"] == 0 or recording["c"] == 0
                    else round((recording["y"] / recording["c"]), 1)
                )
                if val == 0:
                    continue
                self._data[self.attr_id][RESULT][VALUE].append(
                    {
                        "d": last_hour.replace(hour=idx),
                        VALUE: round((recording["y"] / recording["c"]), 1),
                    }
                )

    async def fetch_range(
        self, start_time: datetime, stop_time: datetime
    ) -> dict:
        async with self._lock:
            if stop_time - start_time > MAX_FETCH_RANGE:
                _LOGGER.warning(
                    "Limiting recording fetch for %s to the last %s days.",
                    self.name,
                    MAX_FETCH_RANGE.days,
                )
                start_time = stop_time - MAX_FETCH_RANGE
            current_date = start_time
            while current_date < stop_time:
                uri = self.build_uri(time=current_date)
                data = await self._connector.get(uri)
                day = current_date
                # Always move on: an empty answer used to retry the same day
                # forever, flooding the gateway.
                current_date += timedelta(days=1)
                if not data:
                    _LOGGER.debug("Empty recording for %s at %s.", self.name, uri)
                    continue
                if RECORDING in data:
                    for idx, recording in enumerate(data[RECORDING]):
                        _d = day.replace(
                            hour=idx, minute=0, second=0, microsecond=0
                        )
                        val = (
                            0
                            if recording["y"] == 0 or recording["c"] == 0
                            else round((recording["y"] / recording["c"]), 1)
                        )
                        if val == 0:
                            continue
                        if start_time <= _d <= stop_time:
                            self._past_data[_d] = {
                                "d": _d,
                                VALUE: val,
                            }
            return self._past_data

    @property
    def state(self) -> str | None:
        """Retrieve state of the record sensor."""
        return self._data[self.attr_id].get(RESULT, {}).get(VALUE)

    def build_uri(self, time: datetime) -> str:
        interval = time.strftime("%Y-%m-%d")
        return f"{self._data[self.attr_id][URI]}?{INTERVAL}={interval}"

    async def update(self, time: datetime = datetime.utcnow()) -> None:
        """Update info about Recording Sensor asynchronously."""
        try:
            if time.hour < 1:
                time = time - timedelta(hours=12)
            uri = self.build_uri(time)
            result = await self._connector.get(uri)
            _LOGGER.debug("Fetching uri for recording sensor %s", uri)
            self.process_results(result, time)
        except DeviceException as err:
            _LOGGER.error(
                f"Can't update data for {self.name}. Trying uri: {self._data[URI]}. Error message: {err}"
            )
            self._extra_message = f"Can't update data. Error: {err}"
