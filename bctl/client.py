import asyncio
import logging
import sys
import json
from logging import Logger
from .common import load_config
from .config import Conf


class Client(object):
    """Client to send commands over socket to the daemon"""

    def __init__(self, debug=False):
        self.logger: Logger = logging.getLogger(__name__)
        log_lvl = logging.DEBUG if debug else logging.INFO
        logging.basicConfig(stream=sys.stdout, level=log_lvl)
        self.conf: Conf = load_config()

    async def _open_write_socket(self, cmd: list):
        try:
            reader, writer = await asyncio.open_unix_connection(self.conf.get('socket_path'))
        except FileNotFoundError:
            self.logger.error('daemon is not running')
            sys.exit(1)
        logging.debug(f'sending command {cmd}')
        writer.write(json.dumps(cmd).encode())
        await writer.drain()
        writer.write_eof()
        return reader, writer

    async def _close_socket(self, writer):
        logging.debug('closing the connection')
        writer.close()
        await writer.wait_closed()

    async def _send_receive(self, cmd: list):
        reader, writer = await self._open_write_socket(cmd)

        data = await reader.read()
        data = json.loads(data.decode())
        self.logger.debug(f'received response {data} from daemon')
        [code, *rest] = data
        outf = sys.stdout if code == 0 else sys.stderr
        for i in rest:
            print(i, file=outf)
        await self._close_socket(writer)
        sys.exit(code)

    async def _send(self, cmd: list):
        reader, writer = await self._open_write_socket(cmd)
        await self._close_socket(writer)

    def send_cmd(self, cmd):
        asyncio.run(self._send(cmd))

    def send_receive_cmd(self, cmd: list):
        asyncio.run(self._send_receive(cmd))

