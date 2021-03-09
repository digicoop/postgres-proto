import socketserver
import ssl
import argparse
from importlib import import_module


class ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.nb_connected_clients = 0

    def finish_request(self, request, client_address):
        self.nb_connected_clients += 1
        max_connected_clients = getattr(self, 'max_connected_clients', None)
        if max_connected_clients is not None and self.nb_connected_clients > max_connected_clients:
            raise Exception('max number of clients reached')
        return super().finish_request(request, client_address)

    def close_request(self, request):
        self.nb_connected_clients -= 1


def create_ssl_context(certfile, keyfile):
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return context


def create_server(request_handler, port, listen_addr='0.0.0.0', ssl_context=None, max_clients=None):
    server = ThreadingTCPServer((listen_addr, port), request_handler)
    server.ssl_context = ssl_context
    server.max_connected_clients = max_clients
    return server


def start_server(request_handler, port, listen_addr='0.0.0.0', ssl_cert=None, ssl_key=None, max_clients=None, **server_properties):
    ssl_context = create_ssl_context(ssl_cert, ssl_key) if ssl_cert and ssl_key else None
    server = create_server(request_handler, port, listen_addr, ssl_context, max_clients)
    for prop, value in server_properties.items():
        setattr(server, prop, value)
    print(f"Serving on {listen_addr}:{port}")
    if server.ssl_context:
        print("SSL is enabled")
    try:
        server.serve_forever()
    except:
        server.shutdown()


cli_arg_parser = argparse.ArgumentParser()
cli_arg_parser.add_argument('--listen-addr', default='127.0.0.1')
cli_arg_parser.add_argument('--port', type=int, default=55432)
cli_arg_parser.add_argument('--ssl-cert')
cli_arg_parser.add_argument('--ssl-key')
cli_arg_parser.add_argument('--max-clients', type=int, default=100)


class RequestHandlerArgAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, [import_lib(v) for v in values])


if __name__ == '__main__':
    cli_arg_parser.add_argument('request_handler', action=RequestHandlerArgAction)
    start_server(**vars(cli_arg_parser.parse_args()))
