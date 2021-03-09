import socketserver
import ssl
from ..stream import PostgresStream
from ..flow import PostgresServerFlowMixin, PostgresError


class BasePostgresStreamRequestHandler(PostgresServerFlowMixin, socketserver.StreamRequestHandler):
    def handle(self):
        self.stream = PostgresStream(self.rfile, self.wfile)
        with self.error_context():
            self.version, self.startup_params, self.user = self.perform_session_init()
            self.handle_session_ready()
            while True:
                if not self.read_and_execute_command():
                    break

    def perform_ssl_handshake(self):
        ssl_context = getattr(self.server, 'ssl_context', None)
        self.stream.send_ssl_request_response(perform=bool(ssl_context))
        if ssl_context:
            try:
                self.ssl_connection = ssl_context.wrap_socket(self.connection, server_side=True)
            except:
                raise PostgresError("failed establishing ssl connection", "FATAL")
            self.stream = PostgresStream(self.ssl_connection.makefile('rb', self.rbufsize),
                socketserver._SocketWriter(self.ssl_connection))

    def handle_session_ready(self):
        pass
