from io import BytesIO
import struct
from contextlib import contextmanager
from .sql import split_sql_queries


EXPECTED_PARAMETERS_STATUS = {
    # As defined here: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC
    # using postgres defaults
    "server_version": "130000", # v13
    "server_encoding": "UTF8",
    "client_encoding": "UTF8",
    "is_superuser": "off",
    "session_authorization": "off",
    "DateStyle": "ISO, MDY",
    "IntervalStyle": "postgres",
    "TimeZone": "GMT",
    "integer_datetimes": "on",
    "standard_conforming_strings": "on"
}


ENCRYPTION_REQUESTS = {
    80877104: 'GSSENCRequest',
    80877103: 'SSLRequest'
}


def is_encrypted_request(msglen, version):
    if msglen == 8 and version in ENCRYPTION_REQUESTS:
        return ENCRYPTION_REQUESTS[version]
    return False


class PostgresError(Exception):
    def __init__(self, message, severity="ERROR", code="0"):
        self.message = message
        self.severity = severity
        self.code = code


@contextmanager
def catch_all_as_postgres_error_context():
    try:
        yield
    except Exception as e:
        raise PostgresError(str(e))


def pgcommand(name):
    def decorator(func):
        func.__pgcommand__ = name
        return func
    return decorator


class PostgresServerFlowMixin(object):
    application_name = 'postgres-proto'

    def perform_session_init(self):
        version, startup_params = self.perform_startup_flow()
        user = self.perform_authentication_flow(startup_params)
        self.send_parameters_status()
        self.stream.send_ready_for_query()
        return version, startup_params, user

    def perform_startup_flow(self):
        msglen, version = self.stream.read_startup_message_header()
        encreq = is_encrypted_request(msglen, version)

        if encreq == 'SSLRequest':
            self.perform_ssl_handshake()
        elif encreq == 'GSSENCRequest':
            self.perform_gssapi_handshake()
        elif self.must_use_encryption():
            raise PostgresError('must use encryption', 'FATAL')

        if encreq:
            msglen, version = self.stream.read_startup_message_header()
        return self.stream.read_startup_message(msglen, version)

    def perform_ssl_handshake(self):
        self.stream.send_ssl_request_response(perform=False)

    def perform_gssapi_handshake(self):
        self.stream.send_gssapi_request_response(perform=False)

    def must_use_encryption(self):
        return False

    def perform_authentication_flow(self, startup_params):
        if self.is_authentication_needed(startup_params['user'], startup_params.get('database')):
            password = self.stream.send_authentication_request()
            user = self.authenticate(startup_params['user'], password, startup_params.get('database'))
            if not user:
                raise PostgresError("authentication failure", "FATAL", "28000")
        else:
            user = startup_params['user']
        self.stream.send_authentication_ok()
        return user

    def is_authentication_needed(self, username, database):
        return False

    def authenticate(self, username, password, database):
        return username

    def send_parameters_status(self):
        self.stream.send_parameters_status(EXPECTED_PARAMETERS_STATUS)
        self.stream.send_parameters_status({'application_name': self.application_name})

    def get_supported_commands(self):
        return {getattr(self, f).__pgcommand__: getattr(self, f) for f in dir(self) if hasattr(getattr(self, f), '__pgcommand__')}

    def get_command_handler(self, code):
        return self.get_supported_commands().get(code)

    def execute_command(self, code):
        with self.error_context():
            handler = self.get_command_handler(code)
            if not handler:
                raise PostgresError('unsupported command')
            handler()

    def read_and_execute_command(self):
        code = self.stream.read_command()
        if not code or code == 'X': # no data or Terminate
            return False
        self.execute_command(code)
        return True

    @pgcommand('Q')
    def perform_simple_query_flow(self):
        queries = self.stream.read_query().rstrip('\x00').strip().rstrip(';')
        for query in split_sql_queries(queries):
            self.perform_query_flow(query)
        self.stream.send_ready_for_query()

    def perform_query_flow(self, query, send_row_description=True):
        if not query:
            self.stream.send_empty_query_response()
            return
        with self.error_context():
            command, rows, cols = self.execute_query(query)
            self.send_query_results(command, rows, cols, send_row_description)

    def send_query_results(self, command, rows, cols, send_row_description=True):
        if cols and send_row_description:
            self.stream.send_row_description(cols)
        if rows:
            self.stream.send_row_data(rows)
        self.stream.send_command_complete(command)

    def execute_query(self):
        """Must return a tuple as follow: (command_name, rows, columns)
           Where:
            - command_name is the SQL command that was executed (eg: "SELECT"), see https://www.postgresql.org/docs/current/protocol-message-formats.html#commandcomplete
            - rows: a list where each item is a a tuple of the same length as the columns with the cell value
            - columns: a list of column names or ColumnDef objects
        """
        raise NotImplementedError()

    @pgcommand('P')
    def perform_prepared_statement_flow(self):
        self.create_prepared_statement(*self.stream.read_parse())
        self.stream.send_parse_complete()

    def create_prepared_statement(self, name, query, param_types):
        raise NotImplementedError()

    @pgcommand('B')
    def perform_bind_prepared_statement_flow(self):
        self.bind_prepared_statement(*self.stream.read_bind())
        self.stream.send_bind_complete()

    def bind_prepared_statement(self, portal, stmt, param_formats, params, result_cols):
        raise NotImplementedError()

    @pgcommand('E')
    def perform_execute_prepared_statement_flow(self):
        self.execute_prepared_statement(*self.stream.read_execute())

    def execute_prepared_statement(self, portal, max_rows):
        raise NotImplementedError()

    @pgcommand('F')
    def perform_flush_prepared_statements_flow(self):
        self.stream.read_flush()
        with self.error_context():
            self.flush_prepared_statement()

    def flush_prepared_statements(self):
        raise NotImplementedError()

    @pgcommand('S')
    def perform_sync_flow(self):
        self.stream.read_sync()
        with self.error_context():
            self.sync_prepared_statement()
            self.stream.send_ready_for_query()

    def sync_prepared_statement(self):
        raise NotImplementedError()

    @pgcommand('D')
    def perform_describe_flow(self):
        describe_type, name = self.stream.read_describe()
        if describe_type == b'P':
            self.describe_portal(name)
        elif describe_type == b'S':
            self.describe_prepared_statement(name)
        else:
            raise PostgresError("invalid type")

    def describe_prepared_statement(self, name):
        raise NotImplementedError()

    def describe_portal(self, name):
        raise NotImplementedError()

    @pgcommand('C')
    def perform_close_flow(self):
        close_type, name = self.stream.read_close()
        if close_type == b'P':
            self.close_portal(name)
        elif close_type == b'S':
            self.close_prepared_statement(name)
        else:
            raise PostgresError("invalid type")
        self.stream.send_close_complete()

    def close_prepared_statement(self, name):
        raise NotImplementedError()

    def close_portal(self, name):
        raise NotImplementedError()

    @contextmanager
    def error_context(self, catch_all=False):
        try:
            try:
                yield
            except Exception as e:
                if catch_all:
                    raise PostgresError(str(e))
                raise
        except PostgresError as e:
            self.stream.send_error(e.message, e.severity, e.code)
