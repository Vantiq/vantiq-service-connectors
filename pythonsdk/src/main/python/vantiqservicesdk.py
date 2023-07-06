import inspect
import json
import logging

from fastapi import FastAPI, APIRouter, WebSocket, WebSocketDisconnect


class BaseVantiqServiceConnector:

    def __init__(self):
        self.api = FastAPI()
        self.router = APIRouter()
        self.router.add_api_route("/healthz", self.__health_check, methods=["GET"])
        self.router.add_api_websocket_route("/wsock/websocket", self.__websocket_endpoint)
        self.api.include_router(self.router)

    @property
    def service_name(self):
        return 'BasePythonService'

    @property
    def app(self):
        return self.api

    async def __health_check(self):
        return f"{self.service_name} is healthy"

    async def __websocket_endpoint(self, websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                # Get the message in bytes and see if it is a ping
                msg_bytes = await websocket.receive_bytes()
                if msg_bytes == b'ping':
                    await websocket.send_bytes('pong'.encode("utf-8"))
                    continue

                # Decode the message as JSON
                request = json.loads(msg_bytes.decode("utf-8"))
                logging.debug('Request was: %s', request)

                # Get the procedure name and parameters
                procedure_name = request.get("procName")
                params = request.get("params")

                # Set up default response and invoke the procedure
                response = {"requestId": request.get("requestId"), "isEOF": True}
                try:
                    result = await self.__invoke(procedure_name, params)
                    response["result"] = result

                except Exception as e:
                    response["errorMsg"] = str(e)

                await websocket.send_json(response, "binary")

        except WebSocketDisconnect:
            pass

    async def __invoke(self, procedure_name: str, params: dict):
        # Confirm that the procedure exists
        if not hasattr(self, procedure_name):
            raise Exception(f"Procedure {procedure_name} does not exist")

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
