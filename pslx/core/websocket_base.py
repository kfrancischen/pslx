import asyncio
import json
import websockets
from pslx.core.base import Base
from pslx.util.dummy_util import DummyUtil


class WebsocketBase(Base):
    def __init__(self, ws_url, params, logger=DummyUtil.dummy_logging()):
        super().__init__()
        self._ws_url = ws_url
        self._params = params
        self._logger = logger
        self._ws_connection = None
        self._op = None
        try:
            self._loop = asyncio.get_event_loop()
        except Exception as err:
            self._logger.warning("Getting the loop with error: " + str(err) + '.')
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

    def start(self):
        self._loop.run_until_complete(self._connect())
        if isinstance(self._params['ws_subscription'], list):
            for subscription_str in self._params['ws_subscription']:
                asyncio.ensure_future(self._ws_connection.send(json.dumps(subscription_str)))
        else:
            asyncio.ensure_future(self._ws_connection.send(json.dumps(self._params['ws_subscription'])))
        asyncio.ensure_future(self._receive_msg())
        self._loop.run_forever()

    def bind_to_op(self, op):
        self._op = op

    async def _connect(self):
        self._ws_connection = await websockets.client.connect(self._ws_url)
        self._logger.info("Connected to websocket..")
        return self._ws_connection

    async def _close_stream(self):
        await self._ws_connection.close()
        await self._loop.shutdown_asyncgens()
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._loop.is_closed():
            self._logger.info('Event loop was closed.')
        else:
            self._logger.warning('Event loop was not closed.')

    async def _parse_message(self, message):
        if not self._op:
            self._logger.error("Please bind to operator to parse message.")
        else:
            self._op.msg_parser(message=message)

    async def _receive_msg(self):
        while True:
            try:
                message = await self._ws_connection.recv()
                await self._parse_message(message=message)
            except websockets.exceptions.ConnectionClosed:
                self._logger.error("Websocket closed..")
                await self._close_stream()
