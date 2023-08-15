import asyncio
import inspect
import json
import logging
from typing import Union, Any, Set

from fastapi import FastAPI, APIRouter, WebSocket, WebSocketDisconnect
from vantiqsdk import Vantiq

CLIENT_CONFIG_MSG = '_setClientConfig'


class BaseVantiqServiceConnector:

    def __init__(self):
        self._api = FastAPI()
        self._router = APIRouter()
        self._router.add_api_route("/healthz", self._health_check, methods=["GET"])
        self._router.add_api_route("/status", self._status, methods=["GET"])
        self._router.add_api_websocket_route("/wsock/websocket", self.__websocket_endpoint)
        self._api.include_router(self._router)
        self._client_config: Union[dict, None] = None
        self._config_set = asyncio.Condition()

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

    def _status(self) -> dict:
        return {}

    async def __websocket_endpoint(self, websocket: WebSocket):
        await websocket.accept()
        active_requests: Set = set()
        try:
            # Start by asking for our configuration (if we don't have it)
            if self._client_config is None:
                config_request = {"requestId": CLIENT_CONFIG_MSG, "isControlRequest": True}
                await websocket.send_json(config_request, "binary")

            while True:
                # Get the message in bytes and see if it is a ping
                msg_bytes = await websocket.receive_bytes()
                if msg_bytes == b'ping':
                    await websocket.send_bytes('pong'.encode("utf-8"))
                    continue

                # Spawn a task to process the message and send the response
                task = asyncio.create_task(self.__process_message(websocket, msg_bytes))

                # Add the task to the set of active requests and remove when done.
                # See https://docs.python.org/3/library/asyncio-task.html#creating-tasks
                active_requests.add(task)
                task.add_done_callback(active_requests.discard)

        except WebSocketDisconnect:
            pass

        finally:
            # Cancel all active requests
            for task in active_requests:
                task.cancel()

    async def __process_message(self, websocket: WebSocket, msg_bytes: bytes) -> None:
        # Decode the message as JSON
        request = json.loads(msg_bytes.decode("utf-8"))
        logging.debug('Request was: %s', request)

        # Set up default response and invoke the procedure
        response = {"requestId": request.get("requestId"), "isEOF": True}
        try:
            # Get the procedure name and parameters
            procedure_name = request.get("procName")
            params = request.get("params")

            # Invoke the procedure and store the result
            result = await self.__invoke(procedure_name, params)
            response["result"] = result

        except Exception as e:
            response["errorMsg"] = str(e)

        await websocket.send_json(response, "binary")

    async def __invoke(self, procedure_name: str, params: dict) -> Any:
        # Confirm that we have a procedure name
        if procedure_name is None:
            raise Exception("No procedure name provided")

        # Are we being given our configuration?
        if procedure_name == CLIENT_CONFIG_MSG:
            async with self._config_set:
                self._client_config = params.pop("config", None)
                self._config_set.notify()
            return True

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

        # Invoke the function (possibly using await)
        params = params or {}
        if inspect.iscoroutinefunction(func):
            return await func(**params)
        else:
            return func(**params)
