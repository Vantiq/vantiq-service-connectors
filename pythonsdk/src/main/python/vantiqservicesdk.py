import asyncio
import inspect
import json
import logging
import logging.config
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from os.path import exists
from threading import Thread
from typing import Union, Any, Set, TypeVar, List, Callable

import yaml
from fastapi import FastAPI, APIRouter, WebSocket, WebSocketDisconnect
from prometheus_client import make_asgi_app, Gauge, Counter, Summary
from vantiqsdk import Vantiq

SET_CLIENT_CONFIG_MSG = '_setClientConfig'
CLEAR_CLIENT_CONFIG_MSG = '_clearClientConfig'


class BaseVantiqServiceConnector:
    """
    Base class for Vantiq service connectors.  This class provides the following:
    - A FastAPI app with the following routes:
        - /healthz - Health check endpoint used by Kubernetes to determine liveness and readiness.
        - /status - Status endpoint that can be used to determine if the service is running.
        - /wsock/websocket - Websocket endpoint used to communicate with Vantiq.
        - /metrics - Prometheus metrics endpoint.
    - A websocket endpoint that will invoke procedures based on messages received from Vantiq.
    - A procedure that can be used to get the Vantiq client, configured to communicate with the Vantiq
        namespace that is running the service.
    """

    def __init__(self):
        # Create FastAPI and add routes
        self._api = FastAPI(lifespan=self.__lifespan)
        self._router = APIRouter()
        self._router.add_api_route("/healthz", self._health_check, methods=["GET"])
        self._router.add_api_route("/status", self._status, methods=["GET"])
        self._router.add_api_websocket_route("/wsock/websocket", self.__websocket_endpoint)

        # Add the router to the app
        self._api.include_router(self._router)

        # Add prometheus asgi middleware to route /metrics requests
        metrics_app = make_asgi_app()
        self._api.mount("/metrics", metrics_app)

        # Set up prometheus metrics -- these are aligned with the Vantiq metrics to the extent possible
        self._websocket_count = Gauge('webSockets_active', 'Current number of websockets')
        self._active_requests = Gauge('active_requests', 'Current number of active requests')
        self._fire_forget_requests = Gauge('fire_forget_requests', 'Total count of fire and forget requests')
        self._resources_executions = Summary('resources_executions', 'Resource execution metrics',
                                             ['resource', 'id', 'serviceProcedure'])
        self._failed_requests = Counter('resources_executions_failed', 'Number of failed requests',
                                        ['resource', 'id', 'serviceProcedure'])

        # Set up the client config
        self._client_config: Union[dict, None] = None
        self._config_set = asyncio.Condition()

        # Set up the logger
        interval_str = os.environ.get('LOG_CONFIG_INTERVAL')
        interval = int(interval_str) if interval_str is not None else None
        self.__logger_config = LoggerConfig(['./config/logger.yaml'], interval)
        self.__logger_config.configure_logging()

        # Create logger
        cur_class = self.__class__
        self._logger = logging.getLogger(f"{cur_class.__module__}.{cur_class.__name__}")

    # noinspection PyUnusedLocal
    @asynccontextmanager
    async def __lifespan(self, app):
        await self._startup()
        yield
        await self._shutdown()

    async def _startup(self):
        pass

    async def _shutdown(self):
        pass

    @property
    def service_name(self) -> str:
        return 'BasePythonService'

    @property
    def app(self) -> FastAPI:
        return self._api

    async def _get_client_config(self) -> dict:
        async with self._config_set:
            if self._client_config is None:
                await self._config_set.wait()
            return self._client_config

    async def _set_client_config(self, config: dict) -> bool:
        async with self._config_set:
            self._client_config = config
            self._config_set.notify_all()
        return True

    async def _clear_client_config(self) -> bool:
        async with self._config_set:
            self._client_config = None
        return True

    async def _get_vantiq_client(self) -> Vantiq:
        config = await self._get_client_config()
        client = Vantiq(config['uri'])
        try:
            await client.set_access_token(config['accessToken'])
        except Exception as e:
            await client.close()
            raise e
        return client

    async def _health_check(self) -> str:
        return f"{self.service_name} is healthy"

    # noinspection PyMethodMayBeStatic
    def _status(self) -> dict:
        return {}

    async def __websocket_endpoint(self, websocket: WebSocket):
        with self._websocket_count.track_inprogress():
            await websocket.accept()
            active_requests: Set = set()

            # Define a callback to remove the task from the active requests
            def __complete_request(completed_task):
                active_requests.discard(completed_task)
                self._active_requests.dec(1)

            try:
                # Start by asking for our configuration (if we don't have it)
                if self._client_config is None:
                    config_request = {"requestId": SET_CLIENT_CONFIG_MSG, "isControlRequest": True}
                    await websocket.send_json(config_request, "binary")

                while True:
                    # Get the message in bytes and see if it is a ping
                    msg_bytes = await websocket.receive_bytes()
                    if msg_bytes == b'ping':
                        await websocket.send_bytes('pong'.encode("utf-8"))
                        continue

                    # Decode the message as JSON, but make sure to catch any errors
                    try:
                        request = json.loads(msg_bytes.decode("utf-8"))
                    except Exception as e:
                        # couldn't deserialize the request, send back an error, but there's no way to know the requestId
                        response = {"isEOF": True}
                        # noinspection PyBroadException
                        try:
                            await websocket.send({"type": "websocket.send",
                                                  "bytes": self._handle_exception(e, {}, response)})
                        except Exception:
                            self._logger.error("Caught error sending error response", exc_info=True)
                        continue

                    options = request.pop('options', {})

                    # Spawn a task to process the request and possibly send the response
                    if options.get('fire_forget', False):
                        asyncio.create_task(self.__fire_forget(websocket, request))
                        self._fire_forget_requests.inc(1)
                    else:
                        # Add the task to the set of active requests and remove when done.
                        # See https://docs.python.org/3/library/asyncio-task.html#creating-tasks
                        task = asyncio.create_task(self.__request_response(websocket, request))
                        active_requests.add(task)
                        self._active_requests.inc(1)
                        task.add_done_callback(__complete_request)

            except WebSocketDisconnect:
                pass

            finally:
                # Cancel all active requests
                for task in active_requests:
                    self._active_requests.dec(1)
                    task.cancel()

    async def __process_message(self, request: dict) -> Any:
        # Get the procedure name and parameters
        self._logger.debug('Request was: %s', request)
        procedure_name: str = request.get("procName", "unknown")
        params = request.get("params")

        # In the future, it should be set by an explicit value isSystemRequest. For compatibility reasons, we
        # must accept appending "__system" to the requestId as equivalent
        is_system_request = request.get("isSystemRequest", False) or request.get("requestId").endswith("__system")

        # Invoke the procedure and store the result
        request_timer = self.__get_resource_metric(self._resources_executions, procedure_name)
        with request_timer.time():
            result = await self.__invoke(procedure_name, params, is_system_request)
            return result

    async def __fire_forget(self, websocket: WebSocket, request: dict) -> None:
        # Send a response to indicate that the request was received
        response = self.encode_response({"requestId": request.get("requestId"), "isEOF": True})
        await websocket.send({"type": "websocket.send", "bytes": response})

        # Process the message and ignore any exceptions
        try:
            await self.__process_message(request)
        except Exception as e:
            self._handle_exception(e, request, None)

    async def __request_response(self, websocket: WebSocket, request: dict) -> None:
        # Set up default response and invoke the procedure
        response = {"requestId": request.get("requestId"), "isEOF": True}
        try:
            result = await self.__process_message(request)
            if isinstance(result, AsyncIterator):
                # Send back the results as they are received
                async for data in result:
                    response = {"requestId": request.get("requestId"), "result": data}
                    encoded_response = self.encode_response(response)
                    self._check_message_size(encoded_response)
                    await websocket.send({"type": "websocket.send", "bytes": encoded_response})

                # Initialize the final response
                response = {"requestId": request.get("requestId")}
            else:
                # Send back the single result
                response["result"] = result

            # Mark this as the final response and encode
            response["isEOF"] = True
            encoded_response = self.encode_response(response)
            self._check_message_size(encoded_response)

        except Exception as e:
            encoded_response = self._handle_exception(e, request, response)

        # Send the response
        await websocket.send({"type": "websocket.send", "bytes": encoded_response})

    def _handle_exception(self, e: Exception, request: dict, response: dict) -> bytes:
        # Log and record error
        procedure_name = request.get("procName", "unknown")
        self._logger.debug(f"Error invoking procedure {procedure_name}", exc_info=e)
        self.__get_resource_metric(self._failed_requests, procedure_name).inc()
        if response is None:
            return b''

        # Remove the result (if any) and add the error message
        error_str = str(e)
        response.pop("result", None)
        if isinstance(e, KeyError):
            error_str = f"Failed to find expected dict key: {error_str}"
        response["errorMsg"] = error_str

        # Serialize to JSON and encode (duplicated here to record JSON serialization errors)
        return json.dumps(response, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    def _check_message_size(self, encoded_message: bytes) -> None:
        config = self._client_config or {}
        max_size = config.get("maxMessageSize", -1)
        if (max_size > 0) and (len(encoded_message) > max_size):
            raise Exception(f"Message size {len(encoded_message)} exceeds maximum size {max_size}")

    def encode_response(self, response: dict) -> bytes:
        text = json.dumps(response, separators=(",", ":"), ensure_ascii=False,
                          default=lambda obj: self.to_json(obj))
        return text.encode("utf-8")

    def to_json(self, obj: Any) -> Any:
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

    async def __invoke(self, procedure_name: str, params: dict, is_system_request: bool) -> Any:
        # Confirm that we have a procedure name
        if procedure_name is None:
            raise Exception("No procedure name provided")

        # Are we being given our configuration?
        if procedure_name == SET_CLIENT_CONFIG_MSG:
            return await self._set_client_config(params.pop("config", {}))

        # Are we being asked to clear our configuration?
        if procedure_name == CLEAR_CLIENT_CONFIG_MSG:
            return await self._clear_client_config()

        # Confirm that the procedure exists
        if not hasattr(self, procedure_name):
            raise Exception(f"Procedure {procedure_name} does not exist")

        # Confirm that the procedure is not private/protected
        if procedure_name.startswith('_'):
            raise Exception(f"Procedure {procedure_name} is not visible")

        # Confirm that the procedure is a coroutine
        func = getattr(self, procedure_name)
        if not callable(func):
            raise Exception(f"Procedure {procedure_name} is not callable")

        if not is_system_request:
            if self.__is_system_only(func):
                raise Exception(f"Procedure {procedure_name} is only available to the system namespace")
            elif self.check_system_required(procedure_name, params):
                raise Exception(f"Procedure {procedure_name} is only available to the system namespace with the " +
                                "parameters given")

        # Invoke the function (possibly using await)
        params = params or {}
        if inspect.iscoroutinefunction(func):
            return await func(**params)
        else:
            return func(**params)

    T = TypeVar('T')

    def __get_resource_metric(self, metric: T, procedure_name: str) -> T:
        return metric.labels(resource='system.serviceconnectors', id=self.service_name,
                             serviceProcedure=procedure_name)

    # noinspection PyMethodMayBeStatic
    def __is_system_only(self, func: Callable) -> bool:
        return getattr(func, "__is_system_only__", False)

    def check_system_required(self, procedure_name: str, params: dict) -> bool:
        """Returns True if the procedure call with the given parameters is only available to calls from the system
        namespace. If the decision does not depend on the parameters, use the decorator @system_only instead."""
        return False


def system_only(func: Callable):
    setattr(func, "__is_system_only__", True)
    return func


class LoggerConfig:
    """
    This class is used to configure logging for a service.  It will look for the YAML files specified by the
    "config_files" list until one of them is found.  If none of those are found it will look for the file named
    "logger.yaml".  Once a configuration file is found, it will be used to configure logging via "dictConfig".
    If none of them are found, it will use the default logging configuration.

    If the "monitor_interval" is greater than 0, it will start a background thread to monitor the chosen configuration
    file.  This check will occur every "monitor_interval" seconds.  If the file is modified, it will be reloaded.
    """
    DEFAULT_MONITORING_INTERVAL = 2 * 60

    def __init__(self, config_files: List[str] = None, monitor_interval: int = None):
        self._logger = logging.getLogger(__name__)
        self._config_files = config_files or []
        self._config_files.append('logger.yaml')
        self._monitor_interval = monitor_interval or self.DEFAULT_MONITORING_INTERVAL
        self._active_config_file = None
        self._monitor_thread = None
        self._stopped = False

    def configure_logging(self) -> None:
        for config_file in self._config_files:
            if exists(config_file):
                config = yaml.load(open(config_file), Loader=yaml.SafeLoader)
                logging.config.dictConfig(config)
                self._active_config_file = config_file
                logging.getLogger().info("Logging configured using %s", config_file)
                break

        if self._active_config_file is not None and self._monitor_interval > 0:
            # Start a background task to monitor the log files
            logging.getLogger().info("Checking %s for changes every %ss", self._active_config_file,
                                     self._monitor_interval)
            self._monitor_thread = Thread(target=self._monitor_config_file, daemon=True, name='Log Config Monitor')
            self._monitor_thread.start()

    def stop(self) -> None:
        self._stopped = True
        if self._monitor_thread is not None:
            self._monitor_thread.join()

    def _monitor_config_file(self) -> None:
        last_timestamp = os.path.getmtime(self._active_config_file)
        while not self._stopped:
            current_timestamp = os.path.getmtime(self._active_config_file)
            if current_timestamp != last_timestamp:
                config = yaml.load(open(self._active_config_file), Loader=yaml.SafeLoader)
                logging.config.dictConfig(config)
                last_timestamp = current_timestamp
            time.sleep(self._monitor_interval)
