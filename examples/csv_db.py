from postgres_proto.socket_handler import PostgresRequestHandler
from postgres_proto.flow import PostgresError, catch_all_as_postgres_error_context
from contextlib import contextmanager
import csv


class CSVRequestHandler(PostgresRequestHandler):
    @contextmanager
    def csv_reader(self):
        with catch_all_as_postgres_error_context(), open(self.server.csv_filename, newline='') as csvfile:
            yield csv.DictReader(csvfile)

    def query_tables(self, stmt_info):
        if len(stmt_info.tables) != 1 or stmt_info.tables[0].name != 'csv':
            raise PostgresError('unknown table')
        with self.csv_reader() as csvreader:
            return list(csvreader), csvreader.fieldnames

    def list_tables(self):
        return ['csv']

    def describe_table(self, table_name):
        with self.csv_reader() as csvreader:
            return csvreader.fieldnames


if __name__ == '__main__':
    from postgres_proto.server import start_server, cli_arg_parser
    cli_arg_parser.add_argument('csv_filename')
    start_server(CSVRequestHandler, **vars(cli_arg_parser.parse_args()))
