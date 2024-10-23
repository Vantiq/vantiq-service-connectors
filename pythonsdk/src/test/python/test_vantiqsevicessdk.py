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


def test_streamed_invoke():
    with client.websocket_connect("/wsock/websocket") as websocket:
        __handle_set_config(websocket)
        response = __invoke_procedure(websocket, "test_asynciter_procedure", "123")
        assert isinstance(response, list)
        assert len(response) == 10
        for i in range(0, 9):
            assert response[i]['result'] == i
        assert response[9]['isEOF']
        response = client.get("/metrics")
        assert response.status_code == 200
        result = response.text
        assert "active_requests 0.0" in result
        assert "webSockets_active 1.0" in result
        exec_count = 'resources_executions_count{id="TestServiceConnector",resource="system.serviceconnectors",' \
                     'serviceProcedure="test_asynciter_procedure"} 1.0'
        assert exec_count in result


def test_fire_forget():
    with client.websocket_connect("/wsock/websocket") as websocket:
        __handle_set_config(websocket)
        response = __invoke_procedure(websocket, "test_procedure", "123", options={"fire_forget": True})
        assert response['isEOF']
        response = client.get("/metrics")
        assert response.status_code == 200
        result = response.text
        assert "fire_forget_requests 1.0" in result


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
        global config_set
        config_set = False


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


def test_invoke_system_only():
    with client.websocket_connect("/wsock/websocket") as websocket:
        __handle_set_config(websocket)

        # Test @system_only procedure
        response = __invoke_procedure(websocket, "system_only_proc", "123", None, False)
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['errorMsg'] == "Procedure system_only_proc is only available to the system namespace"
        response = __invoke_procedure(websocket, "system_only_proc", "123", None, None)
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['errorMsg'] == "Procedure system_only_proc is only available to the system namespace"
        response = __invoke_procedure(websocket, "system_only_proc", "123", None, True)
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['result'] == "This better be from the system namespace"
        response = client.get("/metrics")
        assert response.status_code == 200

        # Test with the "__system" syntax
        response = __invoke_procedure(websocket, "system_only_proc", "123__system", None, None)
        assert response['requestId'] == "123__system"
        assert response['isEOF']
        assert response['result'] == "This better be from the system namespace"
        response = client.get("/metrics")
        assert response.status_code == 200

        # Test conditionally system-only procedure
        response = __invoke_procedure(websocket, "conditionally_system_only_proc", "123",
                                      {"system_required": True}, False)
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['errorMsg'] == ("Procedure conditionally_system_only_proc is only available to the system "
                                        "namespace with the parameters given")
        response = __invoke_procedure(websocket, "conditionally_system_only_proc", "123",
                                      {"system_required": True}, None)
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['errorMsg'] == ("Procedure conditionally_system_only_proc is only available to the system "
                                        "namespace with the parameters given")
        response = __invoke_procedure(websocket, "conditionally_system_only_proc", "123",
                                      {"system_required": True}, True)
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['result'] == "Must be system NS? True"
        response = client.get("/metrics")
        assert response.status_code == 200
        response = __invoke_procedure(websocket, "conditionally_system_only_proc", "123",
                                      {"system_required": False}, False)
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['result'] == "Must be system NS? False"
        response = client.get("/metrics")
        assert response.status_code == 200
        response = __invoke_procedure(websocket, "conditionally_system_only_proc", "123",
                                      {"system_required": False}, True)
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['result'] == "Must be system NS? False"
        response = client.get("/metrics")
        assert response.status_code == 200

def test_message_size():
    with client.websocket_connect("/wsock/websocket") as websocket:
        # Set the config to limit the message size
        max_size = 1000
        config = {"maxMessageSize": max_size}
        __handle_set_config(websocket, config)

        try:
            # Invoke procedure that returns a legal message size
            size = int(max_size / 2)
            response = __invoke_procedure(websocket, "echo_x", "123", {"size": size})
            assert response['requestId'] == "123"
            assert response['isEOF']
            assert len(response['result']) == size

            # Invoke the procedure again with a larger response
            size = max_size + 1
            response = __invoke_procedure(websocket, "echo_x", "123", {"size": size})
            assert response['requestId'] == "123"
            assert response['isEOF']
            assert response['errorMsg'] == "Message size 1045 exceeds maximum size 1000"
        finally:
            __handle_clear_config(websocket)


def test_default_message_size():
    with client.websocket_connect("/wsock/websocket") as websocket:
        # Set default config (no message size limit)
        __handle_set_config(websocket)

        # Invoke procedure that returns a very large message
        size = 100000
        response = __invoke_procedure(websocket, "echo_x", "123", {"size": size})
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert len(response['result']) == size

def test_key_error():
    with client.websocket_connect("/wsock/websocket") as websocket:
        # Set default config
        __handle_set_config(websocket)

        # Invoke procedure that generates a key error
        response = __invoke_procedure(websocket, "key_error", "123", {})
        assert response['requestId'] == "123"
        assert response['isEOF']
        assert response['errorMsg'] == "Failed to find expected dict key: 'key'"


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

def __handle_clear_config(websocket):
    __invoke_procedure(websocket, CLEAR_CLIENT_CONFIG_MSG, "12345", {})
    global config_set
    config_set = False

def __invoke_procedure(websocket, proc_name, request_id, params=None, from_system=None, options=None) -> dict:
    request = {"procName": proc_name, "requestId": request_id}
    if params is not None:
        request['params'] = params
    if from_system is not None:
        request['isSystemRequest'] = from_system
    if options is not None:
        request['options'] = options
    websocket.send_json(request, 'binary')
    done = False
    result = None
    is_list = False
    while not done:
        response: dict = websocket.receive_json('binary')
        done = response.get('isEOF', False)
        if result is None:
            result = response
        else:
            if not is_list:
                result = [result]
                is_list = True
            result.append(response)
    return result
