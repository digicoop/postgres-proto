from io import BytesIO
from contextlib import contextmanager
import struct


POSTGRES_TYPE_MAPPING = {
    int: (23, 4),
    str: (25, -1)
}


class ColumnDef:
    def __init__(self, name, pytype=None, type_id=None, type_size=None):
        self.name = name
        if pytype:
            self.type_id, self.type_size = POSTGRES_TYPE_MAPPING[pytype]
        else:
            self.type_id = type_id
            self.type_size = type_size


class PostgresBuffer(object):
    """Utilities to write on the stream"""

    def __init__(self, stream=None):
        if not stream:
            self.stream = BytesIO()
        else:
            self.stream = stream

    def getvalue(self):
        return self.stream.getvalue()

    def read(self, n):
        return self.stream.read(n)

    def read_int16(self):
        data = self.read(2)
        return struct.unpack("!h", data)[0]

    def read_int32(self):
        data = self.read(4)
        return struct.unpack("!i", data)[0]

    def read_string(self):
        data = bytes()
        while True:
            char = self.read(1)
            if char == b'\x00':
                return data.decode()
            data += char

    def read_payload(self):
        msglen = self.read_int32()
        return PostgresBuffer(BytesIO(self.read(msglen - 4)))

    def write(self, value):
        self.stream.write(value)

    def write_int16(self, value):
        self.stream.write(struct.pack("!h", value))

    def write_int32(self, value):
        self.stream.write(struct.pack("!i", value))

    def write_string(self, value):
        self.stream.write(value.encode())
        self.stream.write(b'\x00')

    def write_response(self, code, msg_stream=None):
        self.write(code)
        if msg_stream:
            payload = msg_stream.getvalue()
            self.write_int32(4 + len(payload))
            self.write(payload)
        else:
            self.write_int32(4)

    @contextmanager
    def response(self, code):
        buf = PostgresBuffer()
        yield buf
        self.write_response(code, buf)


class PostgresStream(object):
    """
        Implements reading and writing commands over file objects
        Message formats: https://www.postgresql.org/docs/current/protocol-message-formats.html
    """
    def __init__(self, rfile, wfile):
        self.rfile = PostgresBuffer(rfile)
        self.wfile = PostgresBuffer(wfile)

    def read_startup_message_header(self):
        msglen = self.rfile.read_int32()
        version = self.rfile.read_int32()
        return msglen, version

    def read_startup_message(self, msglen, version):
        maj_version = version >> 16
        min_version = version & 0xffff
        params = self.rfile.read(msglen - 8).rstrip(b'\x00').split(b'\x00')
        startup_params = {params[i].decode(): params[i+1].decode() for i in range(0, len(params), 2)}
        return (maj_version, min_version), startup_params

    def send_ssl_request_response(self, perform=False):
        self.wfile.write(b'S' if perform else b'N')

    def send_gssapi_request_response(self, perform=False):
        self.wfile.write(b'G' if perform else b'N')

    def send_authentication_request(self):
        self.wfile.write(struct.pack(b"!cii", b'R', 8, 3)) # AuthenticationCleartextPassword
        type_code = self.rfile.read(1)
        if type_code != b"p":
            return
        return self.rfile.read_payload().read_string()

    def send_authentication_ok(self):
        self.wfile.write(struct.pack(b"!cii", b'R', 8, 0)) # AuthenticationOk

    def send_parameters_status(self, params):
        for key, value in params.items():
            with self.wfile.response(b'S') as r: # ParameterStatus
                r.write_string(key)
                r.write_string(value)

    def send_ready_for_query(self, status=b'I'): # idle transaction
        with self.wfile.response(b'Z') as r: # ReadyForQuery
            r.write(status)

    def send_command_complete(self, tag):
        with self.wfile.response(b'C') as r: # CommandComplete
            r.write_string(tag)

    def read_command(self):
        return self.rfile.read(1).decode()

    def read_query(self):
        return self.rfile.read_payload().read_string() # Query

    def send_empty_query_response(self):
        self.wfile.write_response(b'I') # EmptyQueryResponse

    def read_parse(self):
        data = self.rfile.read_payload() # Parse
        name = data.read_string()
        query = data.read_string()
        param_types = [data.read_int32() for i in range(data.read_int16())]
        return name, query, param_types

    def send_parse_complete(self):
        self.wfile.write_response(b'1') # ParseComplete

    def read_bind(self):
        data = self.rfile.read_payload() # Bind
        portal = data.read_string()
        stmt = data.read_string()
        param_formats = [data.read_int16() for i in range(data.read_int16())]
        params = []
        for i in range(data.read_int16()):
            paramlen = data.read_int32()
            if paramlen == -1:
                params.append(None)
            if paramlen:
                params.append(data.read(paramlen))
        result_cols = [data.read_int16() for i in range(data.read_int16())]
        return portal, stmt, param_formats, params, result_cols

    def send_bind_complete(self):
        self.wfile.write_response(b'2') # BindComplete

    def read_execute(self):
        data = self.rfile.read_payload() # Execute
        portal = data.read_string()
        max_rows = data.read_int32()
        return portal, max_rows

    def read_describe(self):
        data = self.rfile.read_payload() # Describe
        describe_type = data.read(1)
        name = data.read_string()
        return describe_type, name

    def read_sync(self):
        self.rfile.read_int32() # Sync

    def read_flush(self):
        self.rfile.read_int32() # Flush

    def read_close(self):
        data = self.rfile.read_payload() # Close
        close_type = data.read(1)
        name = data.read_string()
        return close_type, name

    def send_close_complete(self):
        self.wfile.write_response(b'3') # CloseComplete

    def send_no_data(self):
        self.wfile.write_response(b'n')

    def send_row_description(self, cols):
        with self.wfile.response(b'T') as r: # RowDescription
            r.write_int16(len(cols))
            for col in cols:
                if not isinstance(col, ColumnDef):
                    col = ColumnDef(col, str)
                r.write_string(col.name)
                r.write_int32(0)
                r.write_int16(0)
                r.write_int32(col.type_id)
                r.write_int16(col.type_size)
                r.write_int32(-1)
                r.write_int16(0)

    def send_row_data(self, rows):
        for row in rows:
            with self.wfile.response(b'D') as r: # DataRow
                r.write_int16(len(row))
                for field in row:
                    v = str(field).encode()
                    r.write_int32(len(v))
                    r.write(v)

    def send_error(self, message, severity="ERROR", code="0"):
        with self.wfile.response(b'E') as r: # ErrorResponse
            r.write(b'S')
            r.write_string(severity)
            r.write(b'C')
            r.write_string(code)
            r.write(b'M')
            r.write_string(message)
            r.write(b'\x00')
