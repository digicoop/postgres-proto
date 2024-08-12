from functools import cached_property
from .base import BasePostgresStreamRequestHandler
from .prepared_stmts import PostgresPreparedStatementsRequestHandlerMixin
from .builtins import QueryPostgresBuiltinsMixin
from .info_schema import QueryInformationSchemaMixin
from .helpers import format_select_results
from ..flow import PostgresError
from ..sql import Parser


def stmt_handler(name):
    def decorator(func):
        func.__stmt_handler__ = name
        return func
    return decorator


class PostgresRequestHandler(QueryInformationSchemaMixin, QueryPostgresBuiltinsMixin,
                             PostgresPreparedStatementsRequestHandlerMixin, BasePostgresStreamRequestHandler):

    sql_parser_class = Parser
    ignore_missing_statement_types = ('SET', 'BEGIN', 'COMMIT', 'ROLLBACK', 'DEALLOCATE', 'DISCARD')

    @cached_property
    def sql_parser(self):
        return self.sql_parser_class()

    def parse_query(self, query):
        try:
            return self.sql_parser.parse(query.rstrip('\x00').rstrip(';'))
        except SyntaxError as e:
            raise PostgresError("Syntax error: %s" % e)

    def execute_query(self, query):
        stmt_type, stmt_info = self.parse_query(query)

        if self.is_postgres_builtins_query(stmt_type, stmt_info):
            return self.handle_postgres_builtins_query(stmt_type, stmt_info)

        if self.is_information_schema_query(stmt_type, stmt_info):
            return self.handle_information_schema_query(stmt_type, stmt_info)

        handler = self.get_stmt_handler(stmt_type)
        if not handler:
            if stmt_type in self.ignore_missing_statement_types:
                return stmt_type, None, None
            raise PostgresError('statement type not supported')
        rows, cols = handler(stmt_info)
        return stmt_type, rows, cols

    def get_stmt_handler(self, stmt_type):
        handlers = {getattr(self, f).__stmt_handler__: getattr(self, f) for f in dir(self) if hasattr(getattr(self, f), '__stmt_handler__')}
        return handlers.get(stmt_type)

    @stmt_handler('SELECT')
    def handle_select(self, stmt_info):
        if '*' in [c.name for c in stmt_info.columns] and (len(stmt_info.columns) > 1 or stmt_info.columns[0].alias):
            raise PostgresError('select * cannot be aliased or used with other columns')

        data, cols = self.query_tables(stmt_info)
        return format_select_results(data, cols, stmt_info)

    def query_tables(self, stmt_info):
        raise NotImplementedError()
