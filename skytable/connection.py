import asyncio
import time
from typing import List, Tuple, Any, Optional, Union

from .protocol import Protocol
from .query import build, parse


class Connection:
    """A Skytable connection.

    This is made from the :func:`skytable.connect` function.
    """
    def __init__(self, host: str, port: int, timeout: int = 100):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.protocol: Optional[Protocol] = None
        self.transport = None

    async def connect(self):
        """Connects to the Skytable database, you do not need to run this yourself if the connection was made though :func:`skytable.connect`."""
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

    async def set(self, key: str, value: Any):
        """Sets a key-value pair into the database.

        Parameters
        -----------
        key: :class:`str`
            the key for the pair.
        value: Any
            the value for the pair.
        """

        return await self.query([("SET", key, value)])

    async def get(self, key: str) -> Any:
        """Gets a value from the database via its key.

        Parameters
        -----------
        key: :class:`str`
            The key of the value
        
        Returns
        -------
        Any
            The value.
        """
        return await self.query([("GET", key)])

    async def query(self, querys: List[Tuple[str, ...]]) -> Union[List[Tuple[str, str]], List[List[Tuple[str, str]]]]:
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
    """Main function to connect to the Skytable database.

    Parameters
    -----------
    host: :class:`str`
        The host of the Skytable database.
    port: :class:`int`
        The port Skytable is running on.
    timeout: :class:`int`
        How long to wait for to try connect before timing out.
    
    Returns
    --------
    :class:`Connection`
        The connection
    """
    con = Connection(host, port, timeout)
    await con.connect()
    return con
