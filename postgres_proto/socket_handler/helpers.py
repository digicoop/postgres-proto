from ..stream import ColumnDef


def filter_selected_cols(cols, select_cols):
    if select_cols[0] == '*':
        return cols, cols
    return [col.lower().split('.', 1)[-1] for col in select_cols], select_cols


def format_rows(data, cols):
    return [list([item.get(c, '') for c in cols]) for item in data]


def format_result_cols(cols, cols_aliases=None):
    if not cols_aliases:
        cols_aliases = {}
    return [ColumnDef(cols_aliases.get(c, c), str) for c in cols]


def format_select_results(data, cols, stmt_info):
    select_cols, col_names = filter_selected_cols(cols, [c.name for c in stmt_info.columns])
    rows = format_rows(data, select_cols)
    cols = format_result_cols(col_names, {c.name: c.alias for c in stmt_info.columns if c.alias})
    return rows, cols
