from starlette.testclient import TestClient

from testservice import TestServiceConnector

# Prevent pytest from trying to collect TestServiceConnector as tests:
TestServiceConnector.__test__ = False

app = TestServiceConnector().app
client = TestClient(app)


def test_health_check():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == "TestServiceConnector is healthy"


def test_ping():
    with client.websocket_connect("/wsock/websocket") as websocket:
        websocket.send_bytes(b'ping')
        pong = websocket.receive_bytes()
        assert pong == b'pong'


def test_invoke():
    with client.websocket_connect("/wsock/websocket") as websocket:
        request = {"procName": "test_procedure", "requestId": "123"}
        websocket.send_json(request, 'binary')
        response : dict = websocket.receive_json('binary')
        assert response['requestId'] == request['requestId']
        assert response['isEOF']
        assert response['result'] == 'This is a test'
