from .tokenizer import tokenize, split_sql, tokenize_where_expr, tokenize_comma_separated_list
from collections import namedtuple


SelectStmt = namedtuple('SelectStmt', ['columns', 'cols_aliases', 'tables', 'where', 'group_by', 'order_by' 'limit', 'offset'])
SelectColumnExpr = namedtuple('SelectColumnExpr', ['name', 'alias'])
FromTableExpr = namedtuple('FromTableExpr', ['name', 'schema', 'alias', 'joins', 'subquery'])


def parse_sql(sql, stmt_type_delimiters=None):
    stmt_type, parts = split_sql(sql, stmt_type_delimiters)
    stmt_types = {
        'SELECT': transform_select_stmt
    }
    return stmt_type, stmt_types[stmt_type](parts) if stmt_type in stmt_types else parts


def transform_select_stmt(parts):
    return SelectStmt(
        columns=list(parse_select_cols(parts.pop('SELECT'))),
        tables=list(parse_from_tables(parts.pop('FROM'))),
        **{k.replace(' ', '_').lower(): v for k, v in parts.items()})


def parse_select_cols(sql):
    for col_expr, _ in tokenize_comma_separated_list(sql):
        if not col_expr:
            continue
        tokens = tokenize(col_expr, remove_quotes=True, group_delimiters=(('(', ')'), ('CASE ', ' END')))
        name = tokens[0][0].lower()
        alias = None
        if '(' in name:
            alias = name.split('(', 1)[0]
        elif '.' in name:
            alias = name.split('.', 1)[1]
        if len(tokens) > 1:
            alias = tokens[-1][0]
        yield SelectColumnExpr(name, alias)


def parse_from_tables(sql):
    for table_expr, _ in tokenize_comma_separated_list(sql):
        if not table_expr:
            continue
        tokens = tokenize(table_expr, remove_quotes=True)
        name = tokens[0][0].lower()
        schema = None
        alias = None
        if '.' in name:
            schema, name = name.split('.', 1)
        if len(tokens) > 1:
            alias = tokens[-1][0]
        yield FromTableExpr(name, schema, alias)


def extract_value_from_where_comparison(where_cond, col):
    for left_expr, op, right_expr in tokenize_where_expr(where_cond):
        if left_expr == col:
            return right_expr


def parse_sql_func(sql):
    parenth_start = sql.index('(')
    name = sql[:parenth_start]
    args = tokenize(sql, parenth_start + 1, True, False)[0]
    return name, args
