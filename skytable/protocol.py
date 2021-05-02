import asyncio
from typing import Optional, List


class Protocol(asyncio.BaseProtocol):
    def __init__(self, host: str, connected: asyncio.Future):
        super().__init__()
        self.host = host
        self.connected = connected
        self.buffer: asyncio.Queue[bytes] = asyncio.Queue()
        self.transport: Optional[asyncio.Transport] = None

    def connection_made(self, transport: asyncio.Transport):
        self.transport = transport
        self.connected.set_result(None)

    def data_received(self, data: bytes):
        self.buffer.put_nowait(data)

    def eof_received(self):
        pass

    def writelines(self, lines: List[bytes]):
        assert self.transport
        self.transport.writelines(lines)

    def write(self, line: bytes):
        assert self.transport
        self.transport.write(line)

    async def execute(self, query: bytes) -> bytes:
        self.write(query)
        return await self.buffer.get()
