from ..flow import PostgresError


class PostgresPreparedStatementsRequestHandlerMixin(object):
    @property
    def prepared_statements(self):
        return self.__dict__.setdefault('_prepared_statements', {})

    @property
    def portals(self):
        return self.__dict__.setdefault('_portals', {})

    @property
    def portal_results(self):
        return self.__dict__.setdefault('_portal_results', {})

    def create_prepared_statement(self, name, query, param_types):
        self.prepared_statements[name] = (query, param_types)

    def bind_prepared_statement(self, portal, stmt, param_formats, params, result_cols):
        if stmt not in self.prepared_statements:
            raise PostgresError("unknown statement")
        self.portals[portal] = (stmt, param_formats, params, result_cols)
        self.portal_results.pop(portal, None)

    def execute_prepared_statement(self, portal, max_rows):
        if portal not in self.portals:
            raise PostgresError("unknown portal")
        results = self.execute_portal(portal)
        if not results:
            self.stream.send_empty_query_response()
            return
        self.send_query_results(*results, send_row_description=False)

    def execute_portal(self, portal):
        if portal in self.portal_results:
            return self.portal_results[portal]
        query = self.prepared_statements[self.portals[portal][0]][0]
        for i, value in enumerate(self.portals[portal][2]):
            query = query.replace('$%s' % (i+1), value.decode())
        try:
            self.portal_results[portal] = self.execute_query(query) if query else None
        except PostgresError as e:
            self.portal_results[portal] = None
        return self.portal_results[portal]

    def describe_prepared_statement(self, name):
        if name not in self.prepared_statements:
            raise PostgresError("unknown statement")
        # override to send RowDescription
        self.stream.send_no_data()

    def describe_portal(self, name):
        if name not in self.portals:
            raise PostgresError("unknown portal")
        results = self.execute_portal(name)
        if results:
            self.stream.send_row_description(results[2])
        else:
            self.stream.send_no_data()

    def flush_prepared_statements(self):
        pass

    def sync_prepared_statement(self):
        pass

    def close_prepared_statement(self, name):
        if name not in self.prepared_statements:
            raise PostgresError("unknown statement")
        del self.prepared_statements[name]

    def close_portal(self, name):
        if name not in self.portals:
            raise PostgresError("unknown portal")
        del self.portals[name]
        self.portal_results.pop(name, None)
