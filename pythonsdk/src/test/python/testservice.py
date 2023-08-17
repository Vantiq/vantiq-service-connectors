from loggerconfig import LoggerConfig
from vantiqservicesdk import BaseVantiqServiceConnector


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

    async def get_config(self):
        return await self._get_client_config()


app = TestServiceConnector().app
