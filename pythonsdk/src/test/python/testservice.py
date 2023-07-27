from vantiqservicesdk import BaseVantiqServiceConnector


class TestServiceConnector(BaseVantiqServiceConnector):
    def _status(self):
        return {**super()._status(), "my_status": "Good"}

    @property
    def service_name(self):
        return 'TestServiceConnector'

    async def test_procedure(self):
        return "This is a test"

    async def get_config(self):
        return await self._get_client_config()