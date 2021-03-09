from ..flow import PostgresError
from .helpers import format_select_results


class QueryPostgresBuiltinsMixin(object):
    """Implements the minimum to avoid triggering errors for clients querying these tables and functions on start
    """
    pg_builtin_tables = {'pg_matviews', 'pg_type', 'pg_index', 'pg_attribute', 'pg_settings',
        'pg_database', 'pg_roles', 'pg_user', 'pg_enum', 'pg_class', 'pg_namespace'}

    pg_builtin_functions = {
        'current_schema()': 'public',
        'version()': 'PostgreSQL 13.1 (Kantree Tranlation Layer)',
        'pg_backend_pid()': 0
    }

    def is_postgres_builtins_query(self, stmt_type, stmt_info):
        if stmt_type != 'SELECT':
            return False
        return self.is_postgres_builtins_function_query(stmt_info) or \
            stmt_info.tables and {t.name for t in stmt_info.tables} <= set(self.pg_builtin_tables)

    def is_postgres_builtins_function_query(self, stmt_info):
        return not stmt_info.tables and not stmt_info.where and stmt_info.columns and \
            {c.name for c in stmt_info.columns} <= set(self.pg_builtin_functions.keys()) 

    def handle_postgres_builtins_query(self, stmt_type, stmt_info):
        if stmt_type != 'SELECT':
            raise PostgresError('only SELECT statements are supported for postgres builtins')
        if self.is_postgres_builtins_function_query(stmt_info):
            rows, cols = self.handle_postgres_builtin_function_calls(stmt_info)
        else:
            rows, cols = self.handle_postgres_builtin_tables_query(stmt_info)
        rows, cols = format_select_results(rows, cols, stmt_info)
        return stmt_type, rows, cols

    def handle_postgres_builtin_function_calls(self, stmt_info):
        return [self.pg_builtin_functions], self.pg_builtin_functions.keys()

    def handle_postgres_builtin_tables_query(self, stmt_info):
        return [], []
