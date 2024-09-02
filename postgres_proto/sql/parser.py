from .tokenizer import tokenize, split_sql, tokenize_list, unquote, minify_sql
from dataclasses import dataclass
import typing as t
import re


@dataclass
class SelectColumnInfo:
    name: str
    schema: t.Optional[str] = None
    alias: t.Optional[str] = None


@dataclass
class TableInfo:
    name: str
    schema: t.Optional[str] = None
    alias: t.Optional[str] = None
    join: t.Optional["JoinInfo"] = None


@dataclass
class JoinInfo:
    table: TableInfo
    on: str


@dataclass
class SelectInfo:
    columns: t.List[SelectColumnInfo]
    tables: t.List[TableInfo]
    where: t.Optional[str] = None
    group_by: t.Optional[str] = None
    order_by: t.Optional[str] = None
    distinct: t.Optional[bool] = False
    limit: t.Optional[str] = None
    offset: t.Optional[str] = None


class Parser:
    def parse(self, sql):
        sql = minify_sql(sql)
        stmt_type = sql.split(' ', 1)[0].upper()
        method = f"parse_{stmt_type.replace(' ', '_').lower()}"
        return stmt_type, (getattr(self, method)(sql) if hasattr(self, method) else sql)

    def parse_select(self, sql):
        parts = split_sql(sql, ('SELECT', 'DISTINCT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET'))
        if 'DISTINCT' in parts:
            parts['SELECT'] = parts.pop('DISTINCT')
            parts['DISTINCT'] = True
        return SelectInfo(
            columns=list(filter(bool, self.parse_select_cols(parts.pop('SELECT')))),
            tables=list(self.parse_from(parts.pop('FROM'))) if 'FROM' in parts else [],
            **{k.replace(' ', '_').lower(): v for k, v in parts.items()})

    def parse_select_cols(self, sql):
        for col_expr, _ in tokenize_list(sql):
            if not col_expr:
                continue
            yield SelectColumnInfo(*self.parse_identifier(col_expr, group_delimiters=(('(', ')'), ('CASE ', ' END'))))

    def parse_identifier(self, sql, **kwargs):
        name, alias = self.split_alias(tokenize(sql, **kwargs))
        if re.match("[a-z0-9_]+\(", name, re.I):
            return name, None, alias
        path = [t[0] for t in tokenize_list(name, ".", remove_quotes=True)]
        if len(path) > 1:
            return path[1], path[0], alias
        return path[0], None, alias

    def parse_from(self, sql):
        for table_expr, _ in tokenize_list(sql):
            if not table_expr:
                continue
            yield self.parse_from_entry(table_expr)

    def parse_from_entry(self, table_expr):
        source, *joins = tokenize_list(table_expr, " JOIN ")
        source = self.parse_from_table(source[0])
        current = source
        for join in joins:
            table, on = tokenize_list(join[0], " ON ")
            table = self.parse_from_table(table[0])
            current.join = JoinInfo(table, on[0])
            current = current.join
        return source
    
    def parse_from_table(self, sql):
        return TableInfo(*self.parse_identifier(sql))
    
    def parse_where(self, sql):
        expr = self.parse_where_expr(sql)
        if not isinstance(expr, dict):
            return {"$and": [expr]}
        return expr
    
    def parse_where_expr(self, sql):
        if sql.startswith('(') and sql.endswith(')'):
            sql = sql[1:-1]
        ors = tokenize_list(sql, " OR ", remove_group_delimiters=True)
        if len(ors) > 1:
            return {"$or": [self.parse_where_expr(expr) for expr, _ in ors]}
        ands = tokenize_list(sql, " AND ", remove_group_delimiters=True)
        if len(ands) > 1:
            return {"$and": [self.parse_where_expr(expr) for expr, _ in ands]}
        return self.parse_condition(sql)

    def parse_condition(self, sql):
        tokens = [t[0] for t in tokenize(sql, split_delimiters=('=', '!=', '<>', '<', '>', '<=', '>=', '~', '!~', ' in ', ' not in ', ' like ', ' not like ', ' ilike ', ' not ilike '),
                                         split_delimiters_as_tokens=True, max_splits=1)]
        if len(tokens) != 3:
            raise SyntaxError(f"unhandled comparison: {sql}")
        return tokens

    def parse_sql_func(self, sql):
        parenth_start = sql.index('(')
        name = sql[:parenth_start]
        args = [t[0] for t in tokenize_list(sql[parenth_start+1:-1])]
        return name, args

    def split_alias(self, tokens):
        if (len(tokens) > 2 and tokens[-1][0].upper() == "AS") or len(tokens) > 1:
            return tokens[0][0], unquote(tokens[-1][0])
        return tokens[0][0], None


def extract_value_from_where_comparison(parsed_where_info, col):
    for left_expr, op, right_expr in parsed_where_info["$and"]:
        if left_expr == col:
            return unquote(right_expr)