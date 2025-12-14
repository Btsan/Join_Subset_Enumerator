"""
Data structures for PostgreSQL join enumeration

This module defines all dataclasses and type definitions used throughout
the join enumerator implementation.
"""

from dataclasses import dataclass
from typing import Set, List, Optional, Dict


@dataclass
class JoinDetail:
    """
    Join between two tables with column information
    
    Attributes:
        t1: First table name (alias)
        t1_col: Column from first table
        t2: Second table name (alias)
        t2_col: Column from second table
        is_original: True if from original query, False if transitive
    """
    t1: str
    t1_col: str
    t2: str
    t2_col: str
    is_original: bool


@dataclass
class TransitiveJoin:
    """
    Transitive join candidate formed from two existing joins
    
    Example: A.x = B.y AND B.y = C.z => A.x = C.z (transitive)
    """
    t1: str
    t1_col: str
    t2: str
    t2_col: str


@dataclass
class ConstantValue:
    """
    Single constant value extracted from a predicate
    
    Example: "t1.status = 'active'" => ConstantValue('t1', 'status', 'active')
    """
    table: str
    column: str
    value: str


@dataclass
class ConstantEqualityJoin:
    """
    Join inferred from constant equality
    
    Example: t1.col = 'X' AND t2.col = 'X' => t1.col = t2.col
    """
    t1: str
    t2: str
    column: str


@dataclass
class JoinCondition:
    """
    Parsed join condition from SQL
    
    Example: "A.x = B.y" => JoinCondition('A', 'x', 'B', 'y')
    """
    left_table: str
    left_column: str
    right_table: str
    right_column: str


@dataclass
class PredicateSet:
    """
    Classified predicates for a subset of tables
    
    Attributes:
        selections: Single-table predicates (e.g., "t.col > 10")
        joins: Two-table predicates (e.g., "t1.x = t2.y")
        complex: Multi-table predicates (e.g., "t1.x = t2.y + t3.z")
    """
    selections: List[str]
    joins: List[str]
    complex: List[str]


@dataclass
class Decomposition:
    """
    Valid decomposition of a subset into left ⋈ right
    
    Example: {A, B, C} => Decomposition({A, B}, {C})
    """
    left: Set[str]
    right: Set[str]


@dataclass
class JoinPredicate:
    """
    Join predicate with priority flag
    
    Attributes:
        predicate: String representation (e.g., "t1.x = t2.y")
        is_original: True if from original query, False if transitive
    """
    predicate: str
    is_original: bool


@dataclass
class NextTable:
    """
    Next table to add to JOIN tree with its join predicate
    
    Used during SQL generation to build JOIN tree by following edges
    """
    table: str
    join_pred: Optional[JoinPredicate]


@dataclass
class EnumerationPlan:
    """
    Single enumerated subset with metadata
    
    Attributes:
        subset: Set of table aliases
        left: Left subset in decomposition (None for base tables)
        right: Right subset in decomposition (None for base tables)
        sql: Generated SQL query for this subset
    """
    subset: Set[str]
    left: Optional[Set[str]]
    right: Optional[Set[str]]
    sql: str


@dataclass
class EnumerationResult:
    """
    Complete enumeration results for a query
    
    Attributes:
        all_plans: All enumerated subsets in order
        counts: Number of subsets at each level (level -> count)
    """
    all_plans: List[EnumerationPlan]
    counts: Dict[int, int]


@dataclass
class ParsedSQL:
    """
    Result of SQL parsing
    
    Contains all information extracted from a SQL query needed for enumeration
    
    Attributes:
        tables: List of table aliases in query
        aliases: Mapping from alias to base table name
        join_graph: JoinGraph with all joins (original + constant-equality)
        classifier: PredicateClassifier with all predicates
    """
    tables: List[str]
    aliases: Dict[str, str]
    join_graph: 'JoinGraph'  # Forward reference
    classifier: 'PredicateClassifier'  # Forward reference


# Type aliases for clarity
TableAlias = str
CanonicalKey = str  # Sorted, pipe-separated table names: "t1|||t2|||t3"
EdgeKey = str  # Sorted pair: "t1|||t2"
