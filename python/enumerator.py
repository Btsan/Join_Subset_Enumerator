"""
PostgreSQL Join Enumerator

Dynamic Programming-based join enumeration that matches PostgreSQL's behavior.
Enumerates all connected subsets in level-by-level order.
"""

from typing import Set, List, Optional, Dict
from itertools import combinations

from constants import EnumerationResult, EnumerationPlan, Decomposition
from join_graph import JoinGraph


class PostgreSQLJoinEnumerator:
    """
    PostgreSQL-style join enumerator using dynamic programming
    
    Enumerates subsets level-by-level (1, 2, 3, ...) and maintains
    exact ordering to match PostgreSQL optimizer behavior.
    """
    
    def __init__(self, join_graph: JoinGraph):
        """
        Initialize enumerator
        
        Args:
            join_graph: JoinGraph with all joins and equivalence classes
        """
        self.join_graph = join_graph
        self.dp_table: Set[str] = set()  # Canonical subset keys
        self.all_plans: List[EnumerationPlan] = []  # Ordered results
        self.counts: Dict[int, int] = {}  # level -> count
    
    def enumerate_subsets(self, tables: List[str], max_level: int = 20) -> EnumerationResult:
        """
        Enumerate all valid join subsets using dynamic programming
        
        Args:
            tables: List of table aliases to enumerate
            max_level: Maximum enumeration level (default 20)
        
        Returns:
            EnumerationResult with all plans and counts
        """
        # Sort tables for consistent ordering
        tables = sorted(tables)
        num_tables = len(tables)
        
        # Limit enumeration depth
        max_level = min(max_level, num_tables)
        
        # Reset state
        self.dp_table = set()
        self.all_plans = []
        self.counts = {}
        
        # Enumerate level by level
        for level in range(1, max_level + 1):
            checked, added, skipped = self._enumerate_level(level, tables)
            self.counts[level] = added
        
        return EnumerationResult(
            all_plans=self.all_plans,
            counts=self.counts
        )
    
    def _enumerate_level(self, level: int, tables: List[str]) -> tuple:
        """
        Enumerate all subsets at a given level
        
        Args:
            level: Size of subsets to enumerate
            tables: Full list of tables
        
        Returns:
            Tuple of (checked, added, skipped) counts
        """
        checked = 0
        added = 0
        skipped = 0
        
        # Generate all k-subsets in sorted order
        for subset_tuple in combinations(tables, level):
            subset = set(subset_tuple)
            checked += 1
            
            # Check connectivity via equivalence classes
            if not self.join_graph.is_connected(subset):
                skipped += 1
                continue
            
            # For level 1, add directly
            if level == 1:
                self._add_subset(subset, None, None)
                added += 1
            else:
                # Find valid decomposition
                decomp = self._find_valid_decomposition(subset)
                if decomp:
                    self._add_subset(subset, decomp.left, decomp.right)
                    added += 1
                else:
                    skipped += 1
        
        return (checked, added, skipped)
    
    def _find_valid_decomposition(self, subset: Set[str]) -> Optional[Decomposition]:
        """
        Find a valid decomposition of subset into left ⋈ right
        
        A decomposition is valid if:
        1. Both left and right are in dp_table (already enumerated)
        2. left and right can join (share an equivalence class)
        3. left ∪ right = subset
        
        Args:
            subset: Subset to decompose
        
        Returns:
            Decomposition if found, None otherwise
        """
        subset_size = len(subset)
        
        # Try all possible split sizes
        for left_size in range(1, subset_size):
            right_size = subset_size - left_size
            
            # Generate all left subsets of this size
            for left_tuple in combinations(sorted(subset), left_size):
                left = set(left_tuple)
                right = subset - left
                
                # Check if both are already enumerated
                left_key = self._canonical_key(left)
                right_key = self._canonical_key(right)
                
                if left_key not in self.dp_table or right_key not in self.dp_table:
                    continue
                
                # Check if they can join
                if self.join_graph.can_join(left, right):
                    return Decomposition(left=left, right=right)
        
        return None
    
    def _add_subset(self, subset: Set[str], left: Optional[Set[str]], right: Optional[Set[str]]) -> None:
        """
        Add a subset to results and dp_table
        
        Args:
            subset: Subset being added
            left: Left part of decomposition (None for base tables)
            right: Right part of decomposition (None for base tables)
        """
        # Add to dp_table
        key = self._canonical_key(subset)
        self.dp_table.add(key)
        
        # Create plan (SQL will be generated later)
        plan = EnumerationPlan(
            subset=subset,
            left=left,
            right=right,
            sql=""  # Will be filled by SQL generator
        )
        
        self.all_plans.append(plan)
    
    def _canonical_key(self, subset: Set[str]) -> str:
        """
        Generate canonical key for subset
        
        Always uses sorted order to ensure consistency.
        
        Args:
            subset: Set of table aliases
        
        Returns:
            Canonical key string
        """
        return '|||'.join(sorted(subset))
