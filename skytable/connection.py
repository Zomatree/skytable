import asyncio
import time
from typing import List, Tuple, Any, Optional, Union

from .protocol import Protocol
from .query import build, parse


class Connection:
    def __init__(self, host: str, port: int, timeout: int = 100):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.protocol: Optional[Protocol] = None
        self.transport = None

    async def connect(self):
        loop = asyncio.get_running_loop()

        connected = loop.create_future()
        factory = lambda: Protocol(self.host, connected)

        connector = loop.create_connection(factory, self.host, self.port)
        connector = asyncio.ensure_future(connector)

        timeout = self.timeout
        before = time.monotonic()
        
        transport, protocol = await asyncio.wait_for(connector, timeout=timeout)
        
        timeout -= time.monotonic() - before

        try:
            if timeout <= 0:
                raise asyncio.TimeoutError
            await asyncio.wait_for(connected, timeout=timeout)
        except:
            transport.close()
        
        self.protocol = protocol  # type: ignore
        self.transport = transport

        return self

    def set(self, key: str, value: Any):
        return self.query([("SET", key, value)])

    def get(self, key: str):
        return self.query([("GET", key)])

    async def query(self, querys: List[Tuple[str, ...]]):
        assert self.protocol

        data = build(querys).encode()
        response = await self.protocol.execute(data)
        resp = parse(response)
        output: Union[List[Tuple[str, str]], List[List[Tuple[str, str]]]]

        if len(resp) == 1:
            output, = resp
        else:
            output = resp

        return output

async def connect(host, *, port=2003, timeout=100):
    con = Connection(host, port, timeout)
    await con.connect()
    return con
