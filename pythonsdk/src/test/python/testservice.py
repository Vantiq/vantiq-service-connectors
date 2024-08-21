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


app = TestServiceConnector().app
