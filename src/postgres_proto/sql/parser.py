from dataclasses import dataclass
from sqlparse import sql as S, tokens as T
from sqlparse.exceptions import SQLParseError
from sqlparse.utils import remove_quotes
import sqlparse
import typing as t


SELECT_CLAUSES = (
    "SELECT",
    "DISTINCT",
    "FROM",
    "WHERE",
    "GROUP BY",
    "HAVING",
    "WINDOW",
    "ORDER BY",
    "LIMIT",
    "OFFSET",
)

# keywords which sqlparse does not group as part of a Comparison
CONDITION_OPERATOR_KEYWORDS = ("NOT", "IN", "IS", "LIKE", "ILIKE")


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
    on: t.Optional[str]


@dataclass
class SelectInfo:
    columns: t.List[SelectColumnInfo]
    tables: t.List[TableInfo]
    where: t.Optional[str] = None
    group_by: t.Optional[str] = None
    having: t.Optional[str] = None
    window: t.Optional[str] = None
    order_by: t.Optional[str] = None
    distinct: t.Optional[bool] = False
    limit: t.Optional[str] = None
    offset: t.Optional[str] = None


class Parser:
    def parse(self, sql):
        stmt = self.parse_statement(sql)
        first = stmt.token_first(skip_cm=True)
        stmt_type = first.value.split(None, 1)[0].upper() if first else ""
        method = f"parse_{stmt_type.replace(' ', '_').lower()}"
        return stmt_type, (getattr(self, method)(stmt) if hasattr(self, method) else str(stmt))

    def parse_statement(self, sql):
        try:
            statements = sqlparse.parse(sqlparse.format(sql, strip_comments=True).strip())
        except SQLParseError as e:
            raise SyntaxError(str(e)) from e
        return statements[0] if statements else S.Statement([])

    def parse_select(self, stmt):
        clauses = self.split_clauses(stmt, SELECT_CLAUSES)
        distinct = "DISTINCT" in clauses
        columns = clauses.pop("SELECT", []) + clauses.pop("DISTINCT", [])
        return SelectInfo(
            columns=list(self.parse_select_cols(columns)),
            tables=list(self.parse_from(clauses.pop("FROM"))) if "FROM" in clauses else [],
            distinct=distinct,
            **{k.replace(" ", "_").lower(): tokens_to_str(v) for k, v in clauses.items()},
        )

    def split_clauses(self, stmt, keywords):
        clauses = {}
        current = None
        for group in stmt.tokens:
            # the where group also contains the clauses that sqlparse does not consider as closing it
            for token in group.tokens if isinstance(group, S.Where) else [group]:
                keyword = " ".join(token.normalized.split()) if token.is_keyword else None
                if keyword in keywords:
                    current = clauses[keyword] = []
                elif current is not None:
                    current.append(token)
        return clauses

    def parse_select_cols(self, tokens):
        for col_tokens in split_list(tokens):
            yield SelectColumnInfo(*self.parse_identifier(col_tokens))

    def parse_identifier(self, tokens):
        expr, alias = self.split_alias(tokens)
        path = self.parse_identifier_path(expr)
        if path:
            return path[-1], (path[-2] if len(path) > 1 else None), alias
        return tokens_to_str(expr), None, alias

    def split_alias(self, tokens):
        tokens = strip_whitespace(tokens)
        if len(tokens) != 1 or not isinstance(tokens[0], S.Identifier):
            return tokens, None
        identifier = tokens[0]
        alias = identifier.get_alias()
        if alias is None:
            return identifier.tokens, None
        expr = strip_whitespace(strip_whitespace(identifier.tokens)[:-1])
        if expr and expr[-1].match(T.Keyword, "AS"):
            expr = expr[:-1]
        return strip_whitespace(expr), alias

    def parse_identifier_path(self, tokens):
        """Returns the parts of a dotted identifier (eg: schema.table) or None if the expression is not one"""
        tokens = [token for token in tokens if not token.is_whitespace]
        path = []
        for i, token in enumerate(tokens):
            if i % 2:
                if not token.match(T.Punctuation, "."):
                    return None
            elif any(
                token.ttype in ttype for ttype in (T.Name, T.String.Symbol, T.Wildcard, T.Keyword)
            ):
                path.append(remove_quotes(token.value))
            elif isinstance(token, S.Function) and i == len(tokens) - 1:
                path.append(str(token))
            else:
                return None
        return path if len(tokens) % 2 else None

    def parse_from(self, tokens):
        for table_tokens in split_list(tokens):
            yield self.parse_from_entry(table_tokens)

    def parse_from_entry(self, tokens):
        source, *joins = split_tokens(
            tokens, lambda t: t.is_keyword and t.normalized.endswith("JOIN")
        )
        source = self.parse_from_table(source)
        current = source
        for join in joins:
            i = next((i for i, t in enumerate(join) if t.match(T.Keyword, ("ON", "USING"))), None)
            table = self.parse_from_table(join[:i])
            on = tokens_to_str(join[i + 1 :]) if i and join[i].match(T.Keyword, "ON") else None
            current.join = JoinInfo(table, on)
            current = table
        return source

    def parse_from_table(self, tokens):
        return TableInfo(*self.parse_identifier(tokens))

    def parse_where(self, sql):
        expr = self.parse_where_expr(self.parse_statement(sql).tokens)
        if not isinstance(expr, dict):
            return {"$and": [expr]}
        return expr

    def parse_where_expr(self, tokens):
        tokens = strip_whitespace(tokens)
        if len(tokens) == 1 and isinstance(tokens[0], S.Parenthesis):
            return self.parse_where_expr(tokens[0].tokens[1:-1])
        for op in ("OR", "AND"):
            exprs = split_tokens(tokens, lambda t: t.match(T.Keyword, op))
            if len(exprs) > 1:
                return {f"${op.lower()}": [self.parse_where_expr(expr) for expr in exprs]}
        return self.parse_condition(tokens)

    def parse_condition(self, tokens):
        tokens = strip_whitespace(tokens)
        if len(tokens) == 1 and isinstance(tokens[0], S.Comparison):
            tokens = tokens[0].tokens
        ops = [i for i, token in enumerate(tokens) if is_condition_operator(token)]
        if not ops:
            raise SyntaxError(f"unhandled comparison: {tokens_to_str(tokens)}")
        start = end = ops[0]
        for i in ops[1:]:
            if any(not token.is_whitespace for token in tokens[end + 1 : i]):
                break
            end = i
        left = tokens_to_str(tokens[:start])
        op = " ".join(" ".join(token.value.lower().split()) for token in tokens[start : end + 1])
        right = tokens_to_str(tokens[end + 1 :])
        if not left or not right:
            raise SyntaxError(f"unhandled comparison: {tokens_to_str(tokens)}")
        return [left, op, right]

    def parse_sql_func(self, sql):
        token = self.parse_statement(sql).token_first()
        while isinstance(token, S.Identifier):
            token = token.tokens[-1]
        if not isinstance(token, S.Function):
            raise SyntaxError(f"not a function call: {sql}")
        return token.get_name(), [str(param) for param in token.get_parameters()]


def is_condition_operator(token):
    return token.ttype in T.Operator.Comparison or (
        token.is_keyword and token.normalized in CONDITION_OPERATOR_KEYWORDS
    )


def tokens_to_str(tokens):
    return "".join(str(token) for token in tokens).strip()


def strip_whitespace(tokens):
    tokens = list(tokens)
    while tokens and tokens[0].is_whitespace:
        tokens.pop(0)
    while tokens and tokens[-1].is_whitespace:
        tokens.pop()
    return tokens


def split_tokens(tokens, predicate):
    parts = [[]]
    for token in tokens:
        if predicate(token):
            parts.append([])
        else:
            parts[-1].append(token)
    return parts


def split_list(tokens):
    """Splits a comma separated list of expressions, flattening sqlparse's identifier lists"""
    parts = split_tokens(flatten_identifier_lists(tokens), lambda t: t.match(T.Punctuation, ","))
    return [part for part in map(strip_whitespace, parts) if part]


def flatten_identifier_lists(tokens):
    for token in tokens:
        if isinstance(token, S.IdentifierList):
            yield from flatten_identifier_lists(token.tokens)
        else:
            yield token


def iter_tables(tables):
    """Iterates over tables including joined ones"""
    for table in tables:
        while table:
            yield table
            table = table.join.table if table.join else None


def extract_value_from_where_comparison(parsed_where_info, col):
    for cond in parsed_where_info.get("$and", []):
        if isinstance(cond, list) and cond[0] == col:
            return remove_quotes(cond[2])
