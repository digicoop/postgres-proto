from postgres_proto.socket_handler import PostgresRequestHandler
from postgres_proto.flow import PostgresError, catch_all_as_postgres_error_context
from postgres_proto.sql import tokenize_where_expr
import urllib.request
import urllib.parse
import json


class WebRequestRequestHandler(PostgresRequestHandler):
    def query_tables(self, table_names, where_cond):
        with catch_all_as_postgres_error_context():
            url = next(iter(table_names))
            if where_cond:
                query_params = []
                for left_expr, op, right_expr in tokenize_where_expr(where_cond):
                    right_expr = urllib.parse.quote_plus(right_expr)
                    query_params.append(f"{left_expr}={right_expr}")
                url += '?' + '&'.join(query_params)

            data = urllib.request.urlopen(url).read().decode()
            try:
                data = json.loads(data)
            except:
                return [{'response': data}], ['response']

            if not data:
                return [], []
            if isinstance(data, dict):
                return [{'key': k, 'value': v} for k, v in data.items()], ['key', 'value']
            if isinstance(data, list) and isinstance(data[0], dict):
                return data, data[0].keys()
            return [{'item': i} for i in data], ['item']


if __name__ == '__main__':
    from postgres_proto.server import start_server, cli_arg_parser
    start_server(WebRequestRequestHandler, **vars(cli_arg_parser.parse_args()))
