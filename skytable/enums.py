from enum import Enum

class DataTypes(Enum):
    string = "+"
    response_code = "!"
    json = "$"
    smallint = "-"
    smallint_signed = "_"
    int = ":"
    int_signed = ";"
    float = "%"
    binary = "?"

class ResponseCodes(Enum):
    okay = 0
    nil = 1
    overwrite_error = 2
    action_error = 3
    packet_error = 4
    server_error = 5
    other_error = 7
