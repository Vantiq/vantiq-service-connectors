from typing import Any

from typing_extensions import override
from vantiqservicesdk import BaseVantiqServiceConnector, LoggerConfig, system_only


class TestServiceConnector(BaseVantiqServiceConnector):

    def __init__(self):
        super().__init__()
        self.__logger_config = LoggerConfig(monitor_interval=1)
        self.__logger_config.configure_logging()

    def _status(self):
        return {**super()._status(), "my_status": "Good"}

    @property
    def service_name(self):
        return 'TestServiceConnector'

    async def test_procedure(self):
        return "This is a test"

    async def test_asynciter_procedure(self):
        for i in range(0, 9):
            yield i

    @system_only
    async def system_only_proc(self):
        return "This better be from the system namespace"

    async def conditionally_system_only_proc(self, system_required):
        return f"Must be system NS? {system_required}"

    @override
    def check_system_required(self, procedure_name: str, params: dict) -> bool:
        if procedure_name == "conditionally_system_only_proc" and params.get("system_required", False):
            return True
        return False

    async def get_config(self):
        return await self._get_client_config()

    def get_config_direct(self):
        return self._client_config

    # noinspection PyMethodMayBeStatic
    def echo_x(self, size: int):
        return "x" * size

    # noinspection PyMethodMayBeStatic
    async def stream_x(self, size: int, count: int = 3):
        for _ in range(count):
            yield "x" * size

    # noinspection PyMethodMayBeStatic
    async def stream_list(self, size: int):
        # Yields a non-string oversized result, which reduce_oversized_result declines to trim.
        yield list(range(size))

    @override
    def reduce_oversized_result(self, result: Any, max_size: int) -> Any:
        # Trim oversized string results to fit, accounting for the surrounding JSON quotes.
        # Returning None (the default) leaves the result unchanged so the caller raises.
        if isinstance(result, str) and max_size > 2:
            return result[:max_size - 2]
        return None

    # noinspection PyMethodMayBeStatic
    def key_error(self):
        return {}["key"]


app = TestServiceConnector().app
