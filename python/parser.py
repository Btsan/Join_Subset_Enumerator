"""
SQL Parser using SQLglot

This module handles SQL parsing and extraction of tables, joins, and predicates.
It also detects constant-equality joins where multiple tables constrain the same
column to the same single value.
"""

import re
from typing import List, Tuple, Dict, Set, Optional
import sqlglot
from sqlglot import parse_one, exp

from constants import (
    JoinCondition,
    ConstantValue,
    ConstantEqualityJoin,
    ParsedSQL
)


def parse_sql(sql: str, dialect: str = 'postgres') -> ParsedSQL:
    """
    Parse SQL query and extract all information needed for enumeration
    
    Args:
        sql: SQL query string
        dialect: SQL dialect for SQLglot parsing
    
    Returns:
        ParsedSQL with tables, aliases, join graph, and classifier
    
    Raises:
        ValueError: If query cannot be parsed or has no tables
    """
    # Import here to avoid circular dependencies
    from join_graph import JoinGraph
    from predicates import PredicateClassifier
    
    # Parse SQL with SQLglot
    try:
        ast = parse_one(sql, dialect=dialect)
    except Exception as e:
        raise ValueError(f"Failed to parse SQL: {e}")
    
    # Extract tables and aliases
    tables, aliases = _extract_tables(ast)
    
    if not tables:
        raise ValueError("No tables found in query")
    
    # Initialize data structures
    join_graph = JoinGraph()
    classifier = PredicateClassifier()
    
    # Store table aliases in join graph
    for alias, base_name in aliases.items():
        join_graph.table_aliases[alias] = base_name
    
    # Extract join conditions from JOIN clauses and WHERE clause
    join_conditions = _extract_joins_from_ast(ast)
    
    # Add explicit joins to join graph
    for join_cond in join_conditions:
        join_graph.add_join(
            join_cond.left_table,
            join_cond.right_table,
            join_cond.left_column,
            join_cond.right_column,
            is_original=True
        )
    
    # Extract all predicates from WHERE clause
    predicates = _extract_predicates_from_where(ast)
    
    # Add predicates to classifier
    for predicate_text, table_set in predicates:
        classifier.add_predicate(predicate_text, table_set)
    
    # Classify predicates
    classifier.classify_predicates()
    
    # Detect constant-equality joins (e.g., t1.col='X' AND t2.col='X' => t1.col=t2.col)
    constant_joins = _detect_constant_equality_joins(classifier, tables)
    
    # Add constant-equality joins to join graph
    for const_join in constant_joins:
        join_graph.add_join(
            const_join.t1,
            const_join.t2,
            const_join.column,
            const_join.column,
            is_original=False  # These are transitive
        )
    
    # Compute transitive closure
    join_graph.compute_transitive_closure()
    
    # Build equivalence classes
    join_graph.build_equivalence_classes()
    
    return ParsedSQL(
        tables=tables,
        aliases=aliases,
        join_graph=join_graph,
        classifier=classifier
    )


def _extract_tables(ast: exp.Expression) -> Tuple[List[str], Dict[str, str]]:
    """
    Extract tables and aliases from AST
    
    Args:
        ast: SQLglot AST
    
    Returns:
        Tuple of (table_list, alias_map)
        - table_list: List of table aliases (or base names if no alias)
        - alias_map: Dict mapping alias to base table name
    """
    tables = []
    aliases = {}
    
    # Find all Table nodes in FROM and JOIN clauses
    for table_node in ast.find_all(exp.Table):
        # Get base table name
        base_name = table_node.name
        
        # Get alias (if any)
        if table_node.alias:
            alias = table_node.alias
        else:
            alias = base_name
        
        # Add to results
        if alias not in tables:
            tables.append(alias)
            aliases[alias] = base_name
    
    return tables, aliases


def _extract_joins_from_ast(ast: exp.Expression) -> List[JoinCondition]:
    """
    Extract join conditions from JOIN clauses and WHERE clause
    
    Handles both:
    - Modern syntax: JOIN ... ON t1.x = t2.y
    - Legacy syntax: FROM t1, t2 WHERE t1.x = t2.y
    
    Args:
        ast: SQLglot AST
    
    Returns:
        List of JoinCondition objects
    """
    join_conditions = []
    
    # Extract from explicit JOIN...ON clauses
    for join_node in ast.find_all(exp.Join):
        if join_node.on:
            conditions = _extract_join_conditions_from_expression(join_node.on)
            join_conditions.extend(conditions)
    
    # Extract from WHERE clause (handles legacy comma-separated syntax)
    where_node = ast.find(exp.Where)
    if where_node:
        conditions = _extract_join_conditions_from_expression(where_node.this)
        join_conditions.extend(conditions)
    
    return join_conditions


def _extract_join_conditions_from_expression(expr: exp.Expression) -> List[JoinCondition]:
    """
    Extract join conditions from a WHERE or ON expression
    
    Handles AND chains and equality comparisons between table columns
    
    Args:
        expr: SQLglot expression (WHERE or ON clause)
    
    Returns:
        List of JoinCondition objects
    """
    join_conditions = []
    
    # Handle AND chains
    if isinstance(expr, exp.And):
        # Recursively process left and right
        join_conditions.extend(_extract_join_conditions_from_expression(expr.left))
        join_conditions.extend(_extract_join_conditions_from_expression(expr.right))
    
    # Handle EQ (equality) expressions
    elif isinstance(expr, exp.EQ):
        join_cond = _extract_join_condition_from_eq(expr)
        if join_cond:
            join_conditions.append(join_cond)
    
    return join_conditions


def _extract_join_condition_from_eq(eq_expr: exp.EQ) -> Optional[JoinCondition]:
    """
    Extract join condition from an equality expression
    
    Only extracts if both sides are column references (table.column)
    
    Args:
        eq_expr: SQLglot EQ expression
    
    Returns:
        JoinCondition if valid join, None otherwise
    """
    left = eq_expr.left
    right = eq_expr.right
    
    # Both sides must be Column references
    if isinstance(left, exp.Column) and isinstance(right, exp.Column):
        # Extract table and column names
        left_table = left.table if left.table else None
        left_col = left.name
        
        right_table = right.table if right.table else None
        right_col = right.name
        
        # Both must have table qualifiers
        if left_table and right_table and left_table != right_table:
            return JoinCondition(
                left_table=left_table,
                left_column=left_col,
                right_table=right_table,
                right_column=right_col
            )
    
    return None


def _extract_predicates_from_where(ast: exp.Expression) -> List[Tuple[str, Set[str]]]:
    """
    Extract all predicates from WHERE clause
    
    Returns predicates as strings along with the set of tables they reference
    
    Args:
        ast: SQLglot AST
    
    Returns:
        List of (predicate_string, table_set) tuples
    """
    predicates = []
    
    where_node = ast.find(exp.Where)
    if not where_node:
        return predicates
    
    # Break WHERE into individual conditions (split by AND)
    conditions = _split_where_conditions(where_node.this)
    
    for condition in conditions:
        # Convert condition to SQL string
        predicate_text = condition.sql()
        
        # Extract tables referenced in this condition
        tables = set()
        for col in condition.find_all(exp.Column):
            if col.table:
                tables.add(col.table)
        
        predicates.append((predicate_text, tables))
    
    return predicates


def _split_where_conditions(expr: exp.Expression) -> List[exp.Expression]:
    """
    Split WHERE expression into individual conditions
    
    Recursively splits AND chains into individual conditions
    
    Args:
        expr: WHERE expression
    
    Returns:
        List of individual condition expressions
    """
    if isinstance(expr, exp.And):
        # Recursively split left and right
        left_conditions = _split_where_conditions(expr.left)
        right_conditions = _split_where_conditions(expr.right)
        return left_conditions + right_conditions
    else:
        # Base case: single condition
        return [expr]


def _detect_constant_equality_joins(
    classifier: 'PredicateClassifier',
    tables: List[str]
) -> List[ConstantEqualityJoin]:
    """
    Detect joins implied by constant equality
    
    Example: t1.col = 'X' AND t2.col = 'X' => t1.col = t2.col
    
    IMPORTANT: Only creates joins for SINGLE value constraints.
    - Valid: t1.col = 'X' AND t2.col = 'X'
    - Valid: t1.col IN ('X') AND t2.col IN ('X')
    - Invalid: t1.col IN ('X', 'Y') AND t2.col IN ('X', 'Y')
    
    Args:
        classifier: PredicateClassifier with selection predicates
        tables: List of table aliases
    
    Returns:
        List of ConstantEqualityJoin objects
    """
    # Map: "column:value" -> [ConstantValue, ...]
    constant_groups: Dict[str, List[ConstantValue]] = {}
    
    # Scan all tables for single-value constant predicates
    for table in tables:
        predicates = classifier.get_predicates_for_subset([table])
        
        for pred in predicates.selections:
            const_val = _extract_single_constant_value(pred)
            if const_val:
                key = f"{const_val.column}:{const_val.value}"
                
                if key not in constant_groups:
                    constant_groups[key] = []
                constant_groups[key].append(const_val)
    
    # Generate joins for groups with 2+ tables
    constant_joins = []
    
    for key, group in constant_groups.items():
        if len(group) >= 2:
            # Create join between each pair
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    constant_joins.append(ConstantEqualityJoin(
                        t1=group[i].table,
                        t2=group[j].table,
                        column=group[i].column
                    ))
    
    return constant_joins


def _extract_single_constant_value(predicate: str) -> Optional[ConstantValue]:
    """
    Extract single constant value from a predicate
    
    Returns ConstantValue only if predicate constrains column to ONE value
    
    Valid patterns:
    - table.column = 'value'
    - table.column = 123
    - table.column IN ('value')  -- single value only
    
    Invalid patterns:
    - table.column IN ('a', 'b')  -- multiple values
    - table.column > 10  -- not equality
    
    Args:
        predicate: Predicate string
    
    Returns:
        ConstantValue if single-value constraint, None otherwise
    """
    # Pattern 1: table.column = constant
    eq_match = re.search(r'(\w+)\.(\w+)\s*=\s*(.+?)(?:\s+(?:AND|OR)|$)', predicate, re.IGNORECASE)
    if eq_match:
        table = eq_match.group(1)
        column = eq_match.group(2)
        raw_value = eq_match.group(3)
        value = _normalize_value(raw_value)
        return ConstantValue(table=table, column=column, value=value)
    
    # Pattern 2: table.column IN (...)
    in_match = re.search(r'(\w+)\.(\w+)\s+IN\s*\(([^)]+)\)', predicate, re.IGNORECASE)
    if in_match:
        table = in_match.group(1)
        column = in_match.group(2)
        value_list = in_match.group(3)
        
        # Split by comma and count values
        values = [v.strip() for v in value_list.split(',')]
        
        # Only valid if exactly ONE value
        if len(values) == 1:
            normalized = _normalize_value(values[0])
            return ConstantValue(table=table, column=column, value=normalized)
    
    return None


def _normalize_value(raw_value: str) -> str:
    """
    Normalize a constant value
    
    Removes:
    - Quotes (single and double)
    - Type casts (::type)
    - Whitespace
    
    Args:
        raw_value: Raw value string from SQL
    
    Returns:
        Normalized value
    """
    normalized = raw_value.strip()
    
    # Remove quotes
    normalized = re.sub(r"^['\"]|['\"]$", '', normalized)
    
    # Remove type casts
    normalized = re.sub(r'::\w+$', '', normalized)
    
    return normalized.strip()