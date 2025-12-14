"""
Join Graph with Equivalence Classes and Transitive Closure

This module implements the join graph data structure with:
- Equivalence class construction using Union-Find
- Column-aware transitive closure computation
- BFS-based connectivity checking
"""

from typing import Dict, Set, List, Optional
from constants import JoinDetail, TransitiveJoin


class JoinGraph:
    """
    Graph representation of join relationships between tables
    
    Uses equivalence classes to track which columns must be equal.
    Implements PostgreSQL-style join enumeration logic.
    """
    
    def __init__(self):
        self.edges: Set[str] = set()  # "t1|||t2" canonical keys
        self.join_details: Dict[str, List[JoinDetail]] = {}  # edge -> join details
        self.table_aliases: Dict[str, str] = {}  # alias -> base_table
        self.equivalence_classes: List[Set[str]] = []  # [{t1.col, t2.col}, ...]
    
    def add_join(self, t1: str, t2: str, t1_col: str, t2_col: str, is_original: bool) -> None:
        """
        Add a join to the graph
        
        Args:
            t1: First table alias
            t2: Second table alias
            t1_col: Column from first table
            t2_col: Column from second table
            is_original: True if from original query, False if transitive
        """
        # Create canonical edge key (sorted)
        edge = '|||'.join(sorted([t1, t2]))
        self.edges.add(edge)
        
        # Track column details for transitive closure
        if t1_col and t2_col:
            if edge not in self.join_details:
                self.join_details[edge] = []
            
            # Normalize: always store in sorted table order
            table1, table2 = sorted([t1, t2])
            detail = JoinDetail(
                t1=table1,
                t1_col=t1_col if table1 == t1 else t2_col,
                t2=table2,
                t2_col=t2_col if table2 == t2 else t1_col,
                is_original=is_original
            )
            
            self.join_details[edge].append(detail)
    
    def build_equivalence_classes(self) -> int:
        """
        Build equivalence classes using Union-Find algorithm
        
        Each equivalence class is a set of table.column strings that must be equal.
        
        Returns:
            Number of equivalence classes created
        """
        # Union-Find data structures
        parent: Dict[str, str] = {}
        rank: Dict[str, int] = {}
        
        def find(x: str) -> str:
            """Find root with path compression"""
            if x not in parent:
                parent[x] = x
                rank[x] = 0
            if parent[x] != x:
                parent[x] = find(parent[x])  # Path compression
            return parent[x]
        
        def union(x: str, y: str) -> None:
            """Union by rank"""
            root_x = find(x)
            root_y = find(y)
            
            if root_x == root_y:
                return
            
            # Union by rank
            if rank[root_x] < rank[root_y]:
                parent[root_x] = root_y
            elif rank[root_x] > rank[root_y]:
                parent[root_y] = root_x
            else:
                parent[root_y] = root_x
                rank[root_x] += 1
        
        # Process all join details
        for edge, details in self.join_details.items():
            for detail in details:
                # Create table.column identifiers
                tc1 = f"{detail.t1}.{detail.t1_col}"
                tc2 = f"{detail.t2}.{detail.t2_col}"
                
                # Union the two columns
                union(tc1, tc2)
        
        # Group by root to form equivalence classes
        ec_groups: Dict[str, Set[str]] = {}
        for tc in parent.keys():
            root = find(tc)
            if root not in ec_groups:
                ec_groups[root] = set()
            ec_groups[root].add(tc)
        
        # Convert to list of sets
        self.equivalence_classes = list(ec_groups.values())
        
        return len(self.equivalence_classes)
    
    def compute_transitive_closure(self) -> int:
        """
        Compute transitive closure with column-aware checking
        
        Only adds transitive joins when columns match on the shared table.
        Example: A.x=B.y AND B.y=C.z => A.x=C.z (columns match on B)
        
        Returns:
            Number of transitive joins added
        """
        added_count = 0
        max_iterations = 10
        
        for iteration in range(max_iterations):
            found_new = False
            current_edges = list(self.join_details.items())
            
            # Try to find new transitive joins
            for edge1_key, details1 in current_edges:
                for edge2_key, details2 in current_edges:
                    if edge1_key == edge2_key:
                        continue
                    
                    for d1 in details1:
                        for d2 in details2:
                            transitive = self._try_form_transitive_join(d1, d2)
                            
                            if transitive and transitive.t1 != transitive.t2:
                                if not self._join_exists(transitive):
                                    self.add_join(
                                        transitive.t1,
                                        transitive.t2,
                                        transitive.t1_col,
                                        transitive.t2_col,
                                        is_original=False
                                    )
                                    added_count += 1
                                    found_new = True
            
            if not found_new:
                break
        
        return added_count
    
    def _try_form_transitive_join(self, d1: JoinDetail, d2: JoinDetail) -> Optional[TransitiveJoin]:
        """
        Try to form a transitive join from two join details
        
        Checks 4 cases for how two joins might share a table with matching columns.
        
        Args:
            d1: First join detail
            d2: Second join detail
        
        Returns:
            TransitiveJoin if valid, None otherwise
        """
        # Case 1: d1.t2 = d2.t1 with matching columns
        if d1.t2 == d2.t1 and d1.t2_col == d2.t1_col:
            return TransitiveJoin(
                t1=d1.t1,
                t1_col=d1.t1_col,
                t2=d2.t2,
                t2_col=d2.t2_col
            )
        
        # Case 2: d1.t2 = d2.t2 with matching columns
        if d1.t2 == d2.t2 and d1.t2_col == d2.t2_col:
            return TransitiveJoin(
                t1=d1.t1,
                t1_col=d1.t1_col,
                t2=d2.t1,
                t2_col=d2.t1_col
            )
        
        # Case 3: d1.t1 = d2.t1 with matching columns
        if d1.t1 == d2.t1 and d1.t1_col == d2.t1_col:
            return TransitiveJoin(
                t1=d1.t2,
                t1_col=d1.t2_col,
                t2=d2.t2,
                t2_col=d2.t2_col
            )
        
        # Case 4: d1.t1 = d2.t2 with matching columns
        if d1.t1 == d2.t2 and d1.t1_col == d2.t2_col:
            return TransitiveJoin(
                t1=d1.t2,
                t1_col=d1.t2_col,
                t2=d2.t1,
                t2_col=d2.t1_col
            )
        
        return None
    
    def _join_exists(self, transitive: TransitiveJoin) -> bool:
        """
        Check if a join already exists in join_details
        
        Args:
            transitive: TransitiveJoin to check
        
        Returns:
            True if join exists, False otherwise
        """
        edge = '|||'.join(sorted([transitive.t1, transitive.t2]))
        existing = self.join_details.get(edge)
        
        if not existing:
            return False
        
        # Check if this exact join already exists
        for detail in existing:
            # Check both directions
            if ((detail.t1 == transitive.t1 and detail.t1_col == transitive.t1_col and
                 detail.t2 == transitive.t2 and detail.t2_col == transitive.t2_col) or
                (detail.t1 == transitive.t2 and detail.t1_col == transitive.t2_col and
                 detail.t2 == transitive.t1 and detail.t2_col == transitive.t1_col)):
                return True
        
        return False
    
    def is_connected(self, subset: Set[str]) -> bool:
        """
        Check if a subset of tables is connected via equivalence classes
        
        Uses BFS to traverse the join graph via EC membership.
        
        Args:
            subset: Set of table aliases
        
        Returns:
            True if all tables in subset are reachable from first table
        """
        if len(subset) <= 1:
            return True
        
        subset_list = list(subset)
        start = subset_list[0]
        
        visited = {start}
        queue = [start]
        
        while queue:
            current = queue.pop(0)
            
            # Find all tables that share an EC with current
            for other in subset:
                if other not in visited and self.are_in_same_ec(current, other):
                    visited.add(other)
                    queue.append(other)
        
        return len(visited) == len(subset)
    
    def are_in_same_ec(self, t1: str, t2: str) -> bool:
        """
        Check if two tables share any equivalence class
        
        Args:
            t1: First table alias
            t2: Second table alias
        
        Returns:
            True if tables share an EC, False otherwise
        """
        for ec in self.equivalence_classes:
            # Check if this EC contains columns from both tables
            has_t1 = any(tc.startswith(f"{t1}.") for tc in ec)
            has_t2 = any(tc.startswith(f"{t2}.") for tc in ec)
            
            if has_t1 and has_t2:
                return True
        
        return False
    
    def can_join(self, left: Set[str], right: Set[str]) -> bool:
        """
        Check if two subsets can be joined
        
        Returns True if any table from left shares an EC with any table from right.
        
        Args:
            left: Left subset of tables
            right: Right subset of tables
        
        Returns:
            True if subsets can join, False otherwise
        """
        for l in left:
            for r in right:
                if self.are_in_same_ec(l, r):
                    return True
        
        return False

