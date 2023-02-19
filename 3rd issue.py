#1

from typing import List, Dict, Union

Table = List[Dict[str, Union[str, int]]]

def project(table: Table, columns: List[str]) -> Table:
    return [{c: row[c] for c in columns} for row in table]

def select(table: Table, condition: str) -> Table:
    return [row for row in table if eval(condition, {}, row)]

def join(left: Table, right: Table, join_condition: str) -> Table:
    result = []
    for lrow in left:
        for rrow in right:
            if eval(join_condition, {}, {**lrow, **rrow}):
                result.append({**lrow, **rrow})
    return result

def get_column_stats(table: Table, column: str) -> Dict[str, Union[int, List[Union[str, int]]]]:
    distinct_values = set(row[column] for row in table)
    return {
        'distinct_values': len(distinct_values),
        'min_value': min(distinct_values),
        'max_value': max(distinct_values),
        'histogram': [len([row for row in table if row[column] == value]) for value in distinct_values]
    }

def estimate_query_cost(query_plan: List[Dict]) -> float:
    # We will use a simple cost model that estimates the cost of each operator based on column statistics.
    # The cost of a SELECT operation is proportional to the number of rows that satisfy the condition.
    # The cost of a PROJECT operation is negligible.
    # The cost of a JOIN operation is proportional to the product of the number of rows in the two tables that are being joined.
    # This is a very simple cost model that ignores many important factors, such as disk access time and network latency.
    # A real query optimizer would use a much more sophisticated cost model.
    cost = 0
    for op in query_plan:
        if op['type'] == 'SELECT':
            column_stats = op['column_stats']
            num_rows = column_stats['histogram'][column_stats['distinct_values'] // 2]  # estimate based on median histogram value
            cost += num_rows
        elif op['type'] == 'PROJECT':
            pass
        elif op['type'] == 'JOIN':
            left_stats = op['left_column_stats']
            right_stats = op['right_column_stats']
            cost += left_stats['distinct_values'] * right_stats['distinct_values']
    return cost

#2

from typing import List, Dict
from table import Table

def generate_query_plans(table: Table, select_conditions: List[str], project_columns: List[str], join_conditions: List[str]) -> List[List[Dict]]:
    # Start with a single query plan that selects all rows from the table.
    query_plan = [{'type': 'SELECT', 'condition': 'True', 'column_stats': {}}]
    # Apply select conditions.
    for condition in select_conditions:
        new_plans = []
        for plan in query_plan:
            if plan['type'] == 'SELECT':
                new_plans.append({'type': 'SELECT', 'condition': f'({condition}) and ({plan["condition"]})', 'column_stats': plan['column_stats']})
        query_plan = new_plans
    # Apply projection.
    new_plans = []
    for plan in query_plan:
        if plan['type'] == 'SELECT':
            column_stats = {column: plan['column_stats'][column] for column in project_columns}
            new_plans.append({'type': 'SELECT', 'condition': plan['condition'], 'column_stats': column_stats})
        elif plan['type'] == 'JOIN':
            column_stats = {column: plan['column_stats'][column] for column in project_columns}
            new_plans.append({'type': 'PROJECT', 'columns': project_columns, 'column_stats': column_stats, 'child': plan})
    query_plan = new_plans
    # Apply joins.
    for condition in join_conditions:
        new_plans = []
        for plan in query_plan:
            if plan['type'] == 'SELECT':
                new_plans.append({'type': 'SELECT', 'condition': plan['condition'], 'column_stats': plan['column_stats']})
            elif plan['type'] == 'PROJECT':
                new_plans.append({'type': 'PROJECT', 'columns': plan['columns'], 'column_stats': plan['column_stats'], 'child': plan['child']})
            elif plan['type'] == 'JOIN':
                left_stats = plan['left']['column_stats']
                right_stats = plan['right']['column_stats']
                # Apply the join condition to both children.
                left_condition = f'({condition}) and ({plan["condition"]})'
                right_condition = f'({condition}) and ({plan["condition"]})'
                # Merge column stats from both children.
                column_stats = {}
                for column in left_stats:
                    if column in right_stats:
                        column_stats[column] = {
                            'num_distinct': min(left_stats[column]['num_distinct'], right_stats[column]['num_distinct']),
                            'min_value': min(left_stats[column]['min_value'], right_stats[column]['min_value']),
                            'max_value': max(left_stats[column]['max_value'], right_stats[column]['max_value']),
                            'histogram': None
                        }
                    else:
                        column_stats[column] = left_stats[column]
                for column in right_stats:
                    if column not in left_stats:
                        column_stats[column] = right_stats[column]
                new_plans.append({'type': 'JOIN', 'left': plan['left'], 'right': plan['right'], 'condition': condition, 'left_condition': left_condition, 'right_condition': right_condition, 'column_stats': column_stats})
        query_plan = new_plans
    # Return the list of equivalent query plans.
    return query_plan


#3

from typing import Dict, List
import math

class QueryPlan:
    def __init__(self, root: Node):
        self.root = root
        self.cost = None

    def execute(self, db: Dict[str, Dict[str, any]]) -> Dict[str, any]:
        result = self.root.execute(db)
        return result

    def estimate_cost(self, stats: Dict[str, ColumnStats]) -> int:
        self.cost = self.root.estimate_cost(stats)
        return self.cost


class Node:
    def execute(self, db: Dict[str, Dict[str, any]]) -> Dict[str, any]:
        pass

    def estimate_cost(self, stats: Dict[str, ColumnStats]) -> int:
        pass


class ScanNode(Node):
    def __init__(self, relation_name: str):
        self.relation_name = relation_name

    def execute(self, db: Dict[str, Dict[str, any]]) -> Dict[str, any]:
        return db[self.relation_name]

    def estimate_cost(self, stats: Dict[str, ColumnStats]) -> int:
        relation_name = self.relation_name
        relation_size = stats[relation_name].num_rows
        return relation_size


class SelectNode(Node):
    def __init__(self, condition: Expression, child: Node):
        self.condition = condition
        self.child = child
        self.relation_name = child.relation_name

    def execute(self, db: Dict[str, Dict[str, any]]) -> Dict[str, any]:
        child_table = self.child.execute(db)
        condition_func = self.condition.get_func()

        result = []
        for row in child_table:
            if condition_func(row):
                result.append(row)

        return result

    def estimate_cost(self, stats: Dict[str, ColumnStats]) -> int:
        relation_name = self.relation_name
        relation_size = stats[relation_name].num_rows
        selectivity = self.condition.estimate_selectivity(stats)
        filtered_size = relation_size * selectivity
        return relation_size + filtered_size


class JoinNode(Node):
    def __init__(self, left_child: Node, right_child: Node):
        self.left_child = left_child
        self.right_child = right_child
        self.relation_name = f'{left_child.relation_name}_{right_child.relation_name}'

    def execute(self, db: Dict[str, Dict[str, any]]) -> Dict[str, any]:
        left_table = self.left_child.execute(db)
        right_table = self.right_child.execute(db)
        join_cols = set(left_table[0]) & set(right_table[0])

        result = []
        for left_row in left_table:
            for right_row in right_table:
                match = True
                for join_col in join_cols:
                    if left_row[join_col] != right_row[join_col]:
                        match = False
                        break

                if match:
                    result.append({**left_row, **right_row})

        return result

    def estimate_cost(self, stats: Dict[str, ColumnStats]) -> int:
        left_relation_name = self.left_child.relation_name
        right_relation_name = self.right_child.relation_name
        left_relation_size = stats[left_relation_name].num_rows
        right_relation_size = stats[right_relation_name].num_rows
        return left_relation_size * right_relation_size


class ProjectNode(Node):
    def __init__(self, child: Node, cols: List[str]):
        self.child = child
        self.cols = cols
class ProjectNode(Node):
def init(self, child: Node, cols: List[str]):
self.child = child
self.cols = cols
self.relation_name = child.relation_name

def execute(self, db: Dict[str, Dict[str, any]]) -> Dict[str, any]:
    child_table = self.child.execute(db)
    result = [{col: row[col] for col in self.cols} for row in child_table]
    return result

def estimate_cost(self, stats: Dict[str, ColumnStats]) -> int:
    child_relation_name = self.child.relation_name
    child_relation_size = stats[child_relation_name].num_rows
    return child_relation_size
class Expression:
def init(self):
pass

def estimate_selectivity(self, stats: Dict[str, ColumnStats]) -> float:
    pass

def get_func(self):
    pass
class EqExpression(Expression):
def init(self, col: str, val: any):
self.col = col
self.val = val

def estimate_selectivity(self, stats: Dict[str, ColumnStats]) -> float:
    col_stats = stats[self.col]
    if self.val in col_stats.value_counts:
        return 1 / col_stats.value_counts[self.val]
    else:
        return 0

def get_func(self):
    def func(row):
        return row[self.col] == self.val

    return func
class AndExpression(Expression):
def init(self, left: Expression, right: Expression):
self.left = left
self.right = right

def estimate_selectivity(self, stats: Dict[str, ColumnStats]) -> float:
    return self.left.estimate_selectivity(stats) * self.right.estimate_selectivity(stats)

def get_func(self):
    left_func = self.left.get_func()
    right_func = self.right.get_func()

    def func(row):
        return left_func(row) and right_func(row)

    return func
class ColumnStats:
def init(self, num_rows: int, value_counts: Dict[any, int]):
self.num_rows = num_rows
self.value_counts = value_counts

def choose_best_plan(plans: List[QueryPlan], stats: Dict[str, ColumnStats]) -> QueryPlan:
best_plan = None
min_cost = math.inf
for plan in plans:
    cost = plan.estimate_cost(stats)
    if cost < min_cost:
        min_cost = cost
        best_plan = plan

return best_plan



# Implement a simple query evaluator that uses per column statistics to evaluate the cost of each query plan

#4

from typing import List
from database import MiniDB, Table

from typing import Dict, List

class QueryOptimizer:
    def __init__(self, stats: Dict[str, ColumnStats]):
        self.stats = stats

    def optimize(self, root: Node) -> QueryPlan:
        best_plan = self._find_best_plan(root, {})
        return best_plan

    def _find_best_plan(self, node: Node, memo: Dict[Node, QueryPlan]) -> QueryPlan:
        if node in memo:
            return memo[node]

        if isinstance(node, ScanNode):
            plan = QueryPlan(node)
            cost = plan.estimate_cost(self.stats)
            plan.cost = cost
            memo[node] = plan
            return plan

        best_plan = None
        best_cost = math.inf

        for child in node.children():
            child_plan = self._find_best_plan(child, memo)
            plan = QueryPlan(node)
            plan.root.set_child(child_plan.root)
            cost = plan.estimate_cost(self.stats)
            plan.cost = cost + child_plan.cost

            if plan.cost < best_cost:
                best_plan = plan
                best_cost = plan.cost

        memo[node] = best_plan
        return best_plan
