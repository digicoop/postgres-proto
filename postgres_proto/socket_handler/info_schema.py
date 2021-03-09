from ..flow import PostgresError
from ..sql import extract_value_from_where_comparison
from .helpers import format_select_results


class QueryInformationSchemaMixin(object):
    information_schema_namespace = 'information_schema'
    
    def is_information_schema_query(self, stmt_type, stmt_info):
        return stmt_type == 'SELECT' and stmt_info.tables and self.information_schema_namespace in [t.schema for t in stmt_info.tables]

    def handle_information_schema_query(self, stmt_type, stmt_info):
        if stmt_type != 'SELECT':
            raise PostgresError('only SELECT statements are supported for information_schema')

        table_name = stmt_info.tables[0].name
        if table_name == 'tables':
            rows, cols = self.handle_information_schema_tables_query(stmt_info)
        elif table_name == 'character_sets':
            rows, cols = self.handle_information_schema_character_sets_query(stmt_info)
        elif table_name == 'columns':
            rows, cols = self.handle_information_schema_columns_query(stmt_info)
        else:
            rows = []
            cols = []

        rows, cols = format_select_results(rows, cols, stmt_info)
        return stmt_type, rows, cols

    def handle_information_schema_tables_query(self, stmt_info):
        rows = [{'table_schema': 'public', 'table_name': t, 'table_type': 'BASE TABLE'} for t in self.list_tables()]
        cols = ['table_schema', 'table_name', 'table_type']
        return rows, cols

    def handle_information_schema_character_sets_query(self, stmt_info):
        return [{'character_set_name': 'UTF8'}], ['character_set_name']

    def handle_information_schema_columns_query(self, stmt_info):
        if not stmt_info.where:
            return [], []
        table_name = extract_value_from_where_comparison(stmt_info.where, 'table_name')
        if not table_name:
            return [], []
        rows = []
        for i, col in enumerate(self.describe_table(table_name)):
            rows.append({'column_name': col, 'ordinal_position': i+1, 'is_nullable': 't', 'data_type': 'text'})
        return rows, ['column_name', 'ordinal_position', 'is_nullable', 'data_type']

    def list_tables(self):
        """Override to return a list of table names
        """
        return []

    def describe_table(self, table_name):
        """Override to return a list of column names for the specified table
        """
        return []
