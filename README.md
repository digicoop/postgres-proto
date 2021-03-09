# Postgres-Proto

Utilities to implement a socket server that speaks the PostgreSQL protocol.

Add support for the PostgreSQL protocol to your app and instantly add compatibility with hundreds of existing apps (eg: business intelligence tools, reporting tools, no-code app makers, etc...)

Includes a simple and forgiving SQL parser to implement custom SQL statements.

Features:

 - Super easy to implement handling of your custom statements
 - Authentication
 - SSL
 - Information schema discovery
 - Handling of prepared statements
 - Ensures minimum compatibility with most tools that perform some queries on start
 - High level customizability and extension capabilities

WARNING: This is not a full implementation of the protocol (COPY is notably missing). Howerver, it provides enough to ensure compatibility with many clients.

## Example

```python
from postgres_proto.socket_handler import PostgresRequestHandler
from postgres_proto.flow import PostgresError

DATABASE = {'table1': [{'id': 1, 'title': 'hello world'}]}

class MyRequestHandler(PostgresRequestHandler):
    def query_tables(self, stmt_info):
        rows = DATABASE.get(stmt_info.tables[0].name, [])
        return rows, rows[0].keys()

    def list_tables(self):
        return DATABASE.keys()

    def describe_table(self, table_name):
        return DATABASE.get(table_name, [[]])[0].keys()
```

Then start a server using this request handler: `$ python -m postgres_proto.server your_file.MyRequestHandler`.
(For more options, use `python -m postgres_proto.server --help`)

Finally connect with any postgres client:

```
$ psql -h localhost -p 55432
> select * from information_schema.tables;
> select * from information_schema.columns where table_name = 'table1';
> select * from table1;
```

(Note the default port: 55432)

Check out the examples folder for more!

## Architecture

The protocol handling is implemented independently from sockets.

 - Reading & writing protocol messages is implemented in `postgres_proto.stream`
 - Protocol flow is implemented in `postgres_proto.flow` (ensuring the correct order of reading/writing messages)

Use `PostgresServerFlowMixin` in your own class and override the methods marked as not implemented.
You can also override some encryption and authentication methods to add support for those.

`postgres_proto.socket_handler.base.BasePostgresStreamRequestHandler` is a socket request handler that uses `PostgresServerFlowMixin`
and provide support for SSL. Up to you to override the other protocol flow methods.

`postgres_proto.socket_handler.PostgresRequestHandler` is a ready to use socket request handler where you can focus on handling SQL statements.
It uses the various utility mixins to support queries about the information schema or specific postgres tables. In most cases you want to
subclass this one as it will ensure compatibility with many existing clients.

## Subclassing `PostgresRequestHandler`

`PostgresRequestHandler` overrides the `execute_query()` method from `PostgresServerFlowMixin` to provide SQL statement handling.
It integrates with `QueryInformationSchemaMixin` and `QueryPostgresBuiltinsMixin` to make sure these tables are handled properly.

When `execute_query()` is called, the SQL query is parsed using `postgres_proto.sql.parser.parse_sql()` which provides `stmt_type` and `stmt_info`.
See `parse_sql()` for more details.

After parsing, the handler associated to the statement type will be called.

SELECT statements are already handled. When a SELECT statement is received, `query_tables()` will be called. You MUST override this function.
`query_tables()` must return a tuple where the first item is a list of dicts (rows) and the second a list of names (column names).

For other statement types, add a method to your request handler class and decorate it with `postgres_proto.socket_handler.stmt_handler`.
Your handler will receive the `stmt_info` object.

```python
from postgres_proto.socket_handler import PostgresRequestHandler, stmt_handler

class MyRequestHandler(PostgresRequestHandler):
    def query_tables(self, stmt_info):
        return [], []

    @stmt_handler('INSERT')
    def handle_insert(self, stmt_info):
        return None, None # no results
```

If a statement type has no handler, an error will be triggered unless it is listed in the `PostgresRequestHandler.ignore_missing_statement_types` property.

## Error handling

Raise exception of type `postgres_proto.flow.PostgresError` for them to be communicated as errors to clients. Any other exception types won't be intercepted and will result in socket termination.

You can use `postgres_proto.flow.catch_all_as_postgres_error_context()` to create a context where all exceptions are wrapped as `PostgresError`.

```python
from postgres_proto.flow import catch_all_as_postgres_error_context
with catch_all_as_postgres_error_context():
    raise Exception('my error')
```

## Handling information schema queries

Information schema queries can be handled using `postgres_proto.socket_handler.info_schema.QueryInformationSchemaMixin` which `PostgresRequestHandler` already uses.

Override the following method to provide the schema info:

 - `list_tables()`: return a list of table names
 - `describe_table(table_name)`: return a list of column names for the specified table

## Handling prepared statements

Prepared statements can be handled using `postgres_proto.socket_handler.prepared_stmts.PostgresPreparedStatementsRequestHandlerMixin` which `PostgresRequestHandler` already uses.

This provides basic handling and considers prepared statements as normal queries, executed through `execute_query()`.

To handle describe requests, these statements may be executed before an actual execute command and their results saved. This allows to use the result of `execute_query()` (ie. the column list) to send back the row description data. Thus, handling of prepared statements is completely transparent.

## Enabling SSL support

`BasePostgresStreamRequestHandler` has support for SSL when an `ssl_context` property exists on the socket server object.

You can easily create an ssl context using `postgres_proto.server.create_ssl_context()`. You will need a private key and certificate. Self-signing works fine.

When using the CLI, provide the key and certificate using `--ssl-key` and `--ssl-cert` respectively.
