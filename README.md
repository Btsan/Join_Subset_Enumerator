Join Enumerator

This tool implements PostgreSQL-style join enumeration for cardinality estimation research.

Algorithm Overview:
1. Parse query to extract tables and join predicates
2. Build Equivalence Classes from join predicates
3. Include transitive joins
4. Enumerate subsets level-by-level:
   - Check connectivity via ECs
   - Generate SQL with complete join conditions

Only works for inner joins.