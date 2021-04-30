from __future__ import annotations

from itertools import takewhile
from typing import List, Tuple, Any

from .enums import DataTypes
from .skytable import parse as initial_parse

"#2\n*1\n#2\n&1\n+1\nb\n"

def build(segments: List[Tuple[str, ...]]) -> str:
    lines = []
    n = f"*{len(segments)}"
    m = f"#{len(n)}"
    lines.extend((m, n))

    for query in segments:
        n = f"&{len(query)}"
        m = f"#{len(n)}"
        lines.extend((m, n))
        for item in query:
            m = f"#{len(item)}"
            lines.extend((m, item))
    
    return "\n".join(lines)

def parse(buf: bytes):
    responses: List[List[Tuple[str, str]]] = initial_parse(buf)
    output: List[List[Any]] = []
    for response in responses:
        out: List[Any] = []
        for item in response:
            value = convert(*item)
            out.append(value)
        output.extend(out)

    return output

def convert(type: str, value: str) -> Any:
    if type == "+":
        return value
    elif type == "!":
        return int(value)
    elif type == ":":
        return int(value)
    else:
        raise NotImplementedError
    
