import re


def minify_sql(sql):
    sql = re.sub("^--.*$", '', sql, flags=re.MULTILINE)
    sql = re.sub("/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/", '', sql)
    return sql.replace("\n", " ").replace("\r", " ").strip()


def split_sql_queries(sql):
    return [t[0] for t in tokenize_list(sql, ";")]


def split_sql(sql, keywords):
    parts = {}
    tokens = tokenize(sql)
    last_part = None
    last_part_pos = 0
    i = 0
    while True:
        part, pos, i = search_next_token(tokens, keywords, i)
        if last_part:
            value = sql[last_part_pos:pos].strip()
            if last_part in parts:
                if not isinstance(parts[last_part], list):
                    parts[last_part] = [parts[last_part]]
                parts[last_part].append(value)
            else:
                parts[last_part] = value
        if not part:
            break
        last_part = part
        last_part_pos = pos + len(part)
    return parts


def tokenize(sql, pos=0, split_delimiters=(',', ' '), group_delimiters=(('(', ')'),), string_delimiters=('"', "'"),
             tokenize_nested=False, remove_quotes=False, split_delimiters_as_tokens=False, in_group=False,
             remove_group_delimiters=False, max_splits=None):

    group_delimiters = dict(group_delimiters)
    open_group_delimiters = tuple(group_delimiters.keys())
    close_group_delimiters = tuple(group_delimiters.values())
    delimiters = string_delimiters + open_group_delimiters + close_group_delimiters + split_delimiters
    tokens = []
    current = ''
    current_pos = 0
    splits_count = 0

    while True:
        delim, delim_pos = find_next_delimiter(sql, pos, delimiters)
        if delim is None:
            if in_group:
                raise SyntaxError('expecting closing group, end of string reached')
            current += sql[pos:]
            if current.strip():
                tokens.append((current.strip(), current_pos))
            break
        current += sql[pos:delim_pos]
        if delim in string_delimiters:
            end_pos = sql.find(delim, delim_pos+1)
            if end_pos == -1:
                raise SyntaxError('expecting closing quote, none found')
            pos = end_pos + 1
            current += sql[delim_pos+1:end_pos] if remove_quotes else sql[delim_pos:pos]
        elif delim in open_group_delimiters:
            if current.strip() or not tokenize_nested:
                end_pos = find_next_unnested_delim(sql, delim_pos + len(delim), delim, group_delimiters[delim])
                pos = end_pos + len(group_delimiters[delim])
                current += sql[delim_pos+1:pos-1] if remove_group_delimiters else sql[delim_pos:pos]
            else:
                # nested group
                group, pos = tokenize(sql, delim_pos + len(delim), split_delimiters, group_delimiters,
                    string_delimiters, True, remove_quotes, split_delimiters_as_tokens, in_group=group_delimiters[delim])
                tokens.append(group)
                current = ''
                current_pos = pos
        elif delim in close_group_delimiters:
            if not in_group or delim != in_group:
                current += delim
                pos = delim_pos + len(delim)
            else:
                if current.strip():
                    tokens.append((current.strip(), current_pos))
                return tokens, delim_pos + len(delim)
        elif splits_count == max_splits:
            current += delim
            pos = delim_pos + len(delim)
        else:
            if current.strip():
                tokens.append((current.strip(), current_pos))
            if split_delimiters_as_tokens:
                tokens.append((delim.strip(), delim_pos))
            pos = delim_pos + len(delim)
            current = ''
            current_pos = pos
            splits_count += 1
    return tokens


def tokenize_list(sql, separator=",", pos=0, **kwargs):
    return tokenize(sql, pos, split_delimiters=(separator,), **kwargs)


def search_next_token(tokens, search, from_idx=0):
    i = from_idx
    while i < len(tokens):
        token, pos = tokens[i]
        for lookup in search:
            if lookup == ' '.join([t[0] for t in tokens[i:i+1+lookup.count(' ')]]).upper():
                return lookup, pos, i+1
        i += 1
    return None, None, i


def find_next_delimiter(string, pos, delimiters):
    next_delim = None
    next_delim_pos = len(string)
    string = string.lower()
    for delim in delimiters:
        delim_pos = string.find(delim.lower(), pos)
        if delim_pos >= 0 and delim_pos < next_delim_pos:
            next_delim = delim
            next_delim_pos = delim_pos
    return next_delim, next_delim_pos


def find_next_unnested_delim(string, pos, open_delim, close_delim):
    next_open_delim = string.lower().find(open_delim.lower(), pos)
    next_close_delim = string.lower().find(close_delim.lower(), pos)
    if next_open_delim >= 0 and next_open_delim < next_close_delim:
        return find_next_unnested_delim(
            string,
            find_next_unnested_delim(string, next_open_delim + len(open_delim), open_delim, close_delim) + len(close_delim),
            open_delim,
            close_delim)
    elif next_close_delim == -1:
        raise SyntaxError(f"missing closing delimiter '{close_delim}'")
    return next_close_delim


def unquote(string, string_delimiters=('"', "'")):
    for delim in string_delimiters:
        if string.startswith(delim) and string.endswith(delim):
            return string[1:-1]
    return string