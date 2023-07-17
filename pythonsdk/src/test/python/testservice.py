from vantiqservicesdk import BaseVantiqServiceConnector


class TestServiceConnector(BaseVantiqServiceConnector):
    @property
    def service_name(self):
        return 'TestServiceConnector'

    async def test_procedure(self):
        return "This is a test"
