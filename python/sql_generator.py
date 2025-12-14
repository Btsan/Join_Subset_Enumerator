"""
SQL Query Generator

Generates SQL queries for enumerated subsets with proper JOIN syntax
and complete WHERE clauses.
"""

from typing import Set, List, Optional, Dict
from constants import JoinPredicate, NextTable, PredicateSet
from join_graph import JoinGraph
from predicates import PredicateClassifier


class SubqueryGenerator:
    """
    Generates SQL queries for enumerated join subsets
    
    Prioritizes original joins over transitive joins when building JOIN trees.
    """
    
    def __init__(
        self, 
        aliases: Dict[str, str], 
        classifier: PredicateClassifier,
        join_graph: JoinGraph
    ):
        """
        Initialize SQL generator
        
        Args:
            aliases: Mapping from alias to base table name
            classifier: PredicateClassifier with all predicates
            join_graph: JoinGraph with all joins
        """
        self.aliases = aliases
        self.classifier = classifier
        self.join_graph = join_graph
    
    def generate_subquery(
        self, 
        subset: Set[str], 
        left: Optional[Set[str]], 
        right: Optional[Set[str]]
    ) -> str:
        """
        Generate SQL query for a subset
        
        Args:
            subset: Set of table aliases
            left: Left subset in decomposition (None for base tables)
            right: Right subset in decomposition (None for base tables)
        
        Returns:
            SQL query string
        """
        if len(subset) == 1:
            return self._generate_base_table_query(list(subset)[0])
        else:
            return self._generate_join_query(subset, left, right)
    
    def _generate_base_table_query(self, table: str) -> str:
        """
        Generate query for single table
        
        Args:
            table: Table alias
        
        Returns:
            SQL query: "SELECT COUNT(*) FROM base_table alias WHERE ..."
        """
        # Get predicates for this table
        preds = self.classifier.get_predicates_for_subset([table])
        
        # Build query
        table_clause = self._render_table(table)
        
        if preds.selections:
            where_clause = ' AND '.join(preds.selections)
            return f"SELECT COUNT(*) FROM {table_clause} WHERE {where_clause};"
        else:
            return f"SELECT COUNT(*) FROM {table_clause};"
    
    def _generate_join_query(
        self, 
        subset: Set[str], 
        left: Optional[Set[str]], 
        right: Optional[Set[str]]
    ) -> str:
        """
        Generate JOIN query for multiple tables
        
        Builds JOIN tree by following original edges when possible.
        
        Args:
            subset: Full subset of tables
            left: Left subset (for metadata only)
            right: Right subset (for metadata only)
        
        Returns:
            SQL query with JOIN syntax
        """
        subset_list = sorted(subset)
        
        # Build FROM clause with JOINs
        # Start with first table
        from_clause = self._render_table(subset_list[0])
        added_tables = {subset_list[0]}
        remaining_tables = set(subset_list[1:])
        used_join_predicates = set()
        
        # Add tables one by one, following original edges
        while remaining_tables:
            next_info = self._find_next_table_for_join_tree(added_tables, remaining_tables)
            
            if not next_info:
                # Should not happen if subset is connected
                break
            
            table = next_info.table
            join_pred = next_info.join_pred
            
            # Add JOIN clause
            from_clause += f"\nJOIN {self._render_table(table)}"
            
            if join_pred:
                from_clause += f" ON {join_pred.predicate}"
                used_join_predicates.add(join_pred.predicate)
            
            added_tables.add(table)
            remaining_tables.remove(table)
        
        # Build WHERE clause with remaining predicates
        where_clause = self._build_where_clause(subset_list, used_join_predicates)
        
        if where_clause:
            return f"SELECT * FROM {from_clause}\nWHERE {where_clause};"
        else:
            return f"SELECT * FROM {from_clause};"
    
    def _find_next_table_for_join_tree(
        self, 
        added_tables: Set[str], 
        remaining_tables: Set[str]
    ) -> Optional[NextTable]:
        """
        Find next table to add to JOIN tree
        
        Prioritizes tables with ORIGINAL joins to current tree.
        Falls back to transitive joins if no original join exists.
        
        Args:
            added_tables: Tables already in JOIN tree
            remaining_tables: Tables not yet added
        
        Returns:
            NextTable with table and join predicate, or None if none found
        """
        # Try to find table with original join first
        for table in sorted(remaining_tables):
            for added in added_tables:
                predicates = self._find_join_predicates([added], [table])
                
                # Look for original join
                for pred in predicates:
                    if pred.is_original:
                        return NextTable(table=table, join_pred=pred)
        
        # Fall back to any join (transitive)
        for table in sorted(remaining_tables):
            for added in added_tables:
                predicates = self._find_join_predicates([added], [table])
                
                if predicates:
                    return NextTable(table=table, join_pred=predicates[0])
        
        return None
    
    def _find_join_predicates(
        self, 
        left_tables: List[str], 
        right_tables: List[str]
    ) -> List[JoinPredicate]:
        """
        Find all join predicates between two sets of tables
        
        Returns predicates sorted with original joins first.
        
        Args:
            left_tables: Tables on left side
            right_tables: Tables on right side
        
        Returns:
            List of JoinPredicate objects, original first
        """
        predicates = []
        
        for l in left_tables:
            for r in right_tables:
                # Get edge key
                edge = '|||'.join(sorted([l, r]))
                
                if edge in self.join_graph.join_details:
                    for detail in self.join_graph.join_details[edge]:
                        # Format predicate
                        pred_str = f"{detail.t1}.{detail.t1_col} = {detail.t2}.{detail.t2_col}"
                        
                        predicates.append(JoinPredicate(
                            predicate=pred_str,
                            is_original=detail.is_original
                        ))
        
        # Sort: original first
        predicates.sort(key=lambda p: (not p.is_original, p.predicate))
        
        return predicates
    
    def _build_where_clause(
        self, 
        subset: List[str], 
        used_join_predicates: Set[str]
    ) -> str:
        """
        Build WHERE clause with all applicable predicates
        
        Includes:
        - Selection predicates (single-table)
        - Join predicates not used in JOIN clauses
        - Complex predicates (multi-table)
        
        Args:
            subset: List of tables in subset
            used_join_predicates: Join predicates already used in JOIN clauses
        
        Returns:
            WHERE clause (without "WHERE" keyword), or empty string
        """
        all_predicates = []
        
        # Get predicates for this subset
        preds = self.classifier.get_predicates_for_subset(subset)
        
        # Add selections
        all_predicates.extend(preds.selections)
        
        # Add unused joins
        for join_pred in preds.joins:
            if join_pred not in used_join_predicates:
                all_predicates.append(join_pred)
        
        # Add complex predicates
        all_predicates.extend(preds.complex)
        
        if all_predicates:
            return ' AND '.join(all_predicates)
        else:
            return ""
    
    def _render_table(self, alias: str) -> str:
        """
        Render table with alias
        
        Args:
            alias: Table alias
        
        Returns:
            "base_table alias" or just "base_table" if no alias
        """
        base_name = self.aliases.get(alias, alias)
        
        if base_name != alias:
            return f"{base_name} {alias}"
        else:
            return base_name
