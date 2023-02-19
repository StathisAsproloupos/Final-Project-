class Table:
    def select_rows(self, where_clause):
        # select rows based on the given where_clause
        selected_rows = []
        for row in self.rows:
            if self._where_clause_eval(row, where_clause):
                selected_rows.append(row)
        return selected_rows

    def _where_clause_eval(self, row, where_clause):
        # evaluate the where_clause for a single row
        if where_clause is None:
            return True
        if isinstance(where_clause, tuple):
            # handle nested where clauses
            left = self._where_clause_eval(row, where_clause[0])
            right = self._where_clause_eval(row, where_clause[2])
            if where_clause[1] == 'AND':
                return left and right
            elif where_clause[1] == 'OR':
                return left or right
        else:
            column_name, operator, value = where_clause
            column = self.get_column(column_name)
            if column.data_type == 'int':
                value = int(value)
            elif column.data_type == 'float':
                value = float(value)
            elif column.data_type == 'bool':
                value = bool(value)
            row_value = row[column_name]
            if operator == '=':
                return row_value == value
            elif operator == '<':
                return row_value < value
            elif operator == '>':
                return row_value > value
            elif operator == '<=':
                return row_value <= value
            elif operator == '>=':
                return row_value >= value
            elif operator == '<>':
                return row_value != value
            elif operator == 'NOT':
                return not self._where_clause_eval(row, value)
            elif operator == 'BETWEEN':
                return row_value >= value[0] and row_value <= value[1]
        return False
      # Added functionality for the keywords "NOT" and "BETWEEN"
    

class MiniDB:
    def execute_select(self, table_name, where_clause=None):
        # execute a SELECT statement and return the selected rows
        table = self.tables[table_name]
        selected_rows = table.select_rows(where_clause)
        return selected_rows
