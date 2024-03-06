import logging
import os

from starlette.testclient import TestClient

import testservice
from testservice import TestServiceConnector
from vantiqservicesdk import SET_CLIENT_CONFIG_MSG, CLEAR_CLIENT_CONFIG_MSG

# Prevent pytest from trying to collect TestServiceConnector as tests:
TestServiceConnector.__test__ = False

app = testservice.app
client = TestClient(app)
config_set = False


def test_health_check():
    logging.critical(f"CWD: {os.getcwd()}")
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == "TestServiceConnector is healthy"


def test_status():
    response = client.get("/status")
    assert response.status_code == 200
    assert response.json() == {"my_status": "Good"}


def test_ping():
    with client.websocket_connect("/wsock/websocket") as websocket:
        __handle_set_config(websocket)
        websocket.send_bytes(b'ping')
        pong = websocket.receive_bytes()
        assert pong == b'pong'


def test_invoke():
    with client.websocket_connect("/wsock/websocket") as websocket:
        __handle_set_config(websocket)
        response = __invoke_procedure(websocket, "test_procedure", "123")
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['result'] == 'This is a test'
        response = client.get("/metrics")
        assert response.status_code == 200
        result = response.text
        assert "active_requests 0.0" in result
        assert "webSockets_active 1.0" in result
        exec_count = 'resources_executions_count{id="TestServiceConnector",resource="system.serviceconnectors",' \
                     'serviceProcedure="test_procedure"} 1.0'
        assert exec_count in result


def test_get_config():
    with client.websocket_connect("/wsock/websocket") as websocket:
        # Start by simulating the client setting the config
        config = {"test": "config"}
        __handle_set_config(websocket, config)
        response = __invoke_procedure(websocket, "get_config", "123")
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['result'] == config

        # Now simulate a request to clear the config
        __invoke_procedure(websocket, CLEAR_CLIENT_CONFIG_MSG, "12345", {})
        response = __invoke_procedure(websocket, "get_config_direct", "321")
        assert response['requestId'] == "321"
        assert response['result'] is None


def test_invoke_errors():
    with client.websocket_connect("/wsock/websocket") as websocket:
        __handle_set_config(websocket)

        # Test missing procedure name
        response = __invoke_procedure(websocket, None, "123")
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['errorMsg'] == "No procedure name provided"

        # Test procedure not found
        response = __invoke_procedure(websocket, "no_such_proc", "123")
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['errorMsg'] == "Procedure no_such_proc does not exist"

        # Test private procedure
        response = __invoke_procedure(websocket, "_status", "123")
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['errorMsg'] == "Procedure _status is not visible"

        # Test member that is not a procedure
        response = __invoke_procedure(websocket, "service_name", "123")
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['errorMsg'] == "Procedure service_name is not callable"


def __handle_set_config(websocket, config=None):
    global config_set
    if config_set:
        return
    response: dict = websocket.receive_json('binary')
    assert response['requestId'] == SET_CLIENT_CONFIG_MSG
    assert response['isControlRequest']
    if config is not None:
        __invoke_procedure(websocket, SET_CLIENT_CONFIG_MSG, "1234", {"config": config})
        config_set = True


def __invoke_procedure(websocket, proc_name, request_id, params=None) -> dict:
    request = {"procName": proc_name, "requestId": request_id}
    if params is not None:
        request['params'] = params
    websocket.send_json(request, 'binary')
    response: dict = websocket.receive_json('binary')
    return response
