from postgres_proto.socket_handler import PostgresRequestHandler
from postgres_proto.flow import PostgresError


DATABASE = {
    'table1': [
        {'id': 1, 'title': 'hello world'},
        {'id': 2, 'title': 'my second row'}
    ],
    'table2': [
        {'id': 1, 'name': 'first row, second table'}
    ]
}


class StaticRequestHandler(PostgresRequestHandler):
    def query_tables(self, stmt_info):
        if len(stmt_info.tables) != 1:
            raise PostgresError('can query only one table at a time')
        table_name = stmt_info.tables[0].name
        if table_name not in DATABASE:
            raise PostgresError('unknown table')
        return DATABASE[table_name], DATABASE[table_name][0].keys()

    def list_tables(self):
        return DATABASE.keys()

    def describe_table(self, table_name):
        return DATABASE[table_name][0].keys()


if __name__ == '__main__':
    from postgres_proto.server import start_server, cli_arg_parser
    start_server(StaticRequestHandler, **vars(cli_arg_parser.parse_args()))
