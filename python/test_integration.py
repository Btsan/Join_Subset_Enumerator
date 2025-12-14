"""
Integration tests for complete enumeration pipeline

Tests end-to-end flow from SQL parsing to enumeration to SQL generation
"""

from parser import parse_sql
from enumerator import PostgreSQLJoinEnumerator
from sql_generator import SubqueryGenerator
from utils import format_subset


def test_simple_join_pipeline():
    """Test complete pipeline with simple join"""
    sql = "SELECT * FROM A, B WHERE A.x = B.y AND A.z > 10;"
    
    # Parse
    parsed = parse_sql(sql)
    assert len(parsed.tables) == 2
    assert set(parsed.tables) == {'A', 'B'}
    
    # Enumerate
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    result = enumerator.enumerate_subsets(parsed.tables)
    
    # Should have 3 subsets: {A}, {B}, {A,B}
    assert len(result.all_plans) == 3
    assert result.counts[1] == 2
    assert result.counts[2] == 1
    
    # Generate SQL
    generator = SubqueryGenerator(parsed.aliases, parsed.classifier, parsed.join_graph)
    
    for plan in result.all_plans:
        sql_query = generator.generate_subquery(plan.subset, plan.left, plan.right)
        assert sql_query.strip().startswith("SELECT")
        assert sql_query.strip().endswith(";")
    
    print("✓ simple join pipeline works")


def test_chain_join_pipeline():
    """Test pipeline with chain: A-B-C"""
    sql = """
    SELECT * FROM A, B, C 
    WHERE A.x = B.y 
      AND B.y = C.z
      AND A.w > 5;
    """
    
    # Parse
    parsed = parse_sql(sql)
    assert len(parsed.tables) == 3
    
    # Enumerate
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    result = enumerator.enumerate_subsets(parsed.tables)
    
    # Level 1: {A}, {B}, {C}
    # Level 2: {A,B}, {B,C}, {A,C} (transitive)
    # Level 3: {A,B,C}
    assert result.counts[1] == 3
    assert result.counts[2] == 3
    assert result.counts[3] == 1
    assert len(result.all_plans) == 7
    
    # Generate SQL for all
    generator = SubqueryGenerator(parsed.aliases, parsed.classifier, parsed.join_graph)
    
    for plan in result.all_plans:
        sql_query = generator.generate_subquery(plan.subset, plan.left, plan.right)
        assert "SELECT" in sql_query
        
        # Check that selection predicate appears in single-table query
        if plan.subset == {'A'}:
            assert "A.w > 5" in sql_query or "w > 5" in sql_query
    
    print("✓ chain join pipeline works")


def test_constant_equality_pipeline():
    """Test pipeline with constant-equality join"""
    sql = """
    SELECT * FROM info_type it1, info_type it2
    WHERE it1.info = 'rating'
      AND it2.info = 'rating';
    """
    
    # Parse
    parsed = parse_sql(sql)
    assert len(parsed.tables) == 2
    
    # Should detect constant-equality join
    # it1.info = 'rating' AND it2.info = 'rating' => it1.info = it2.info
    
    # Enumerate
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    result = enumerator.enumerate_subsets(parsed.tables)
    
    # Should enumerate {it1, it2} even though only constant predicates
    assert result.counts[2] == 1
    
    # Generate SQL
    generator = SubqueryGenerator(parsed.aliases, parsed.classifier, parsed.join_graph)
    
    level2_plan = [p for p in result.all_plans if len(p.subset) == 2][0]
    sql_query = generator.generate_subquery(level2_plan.subset, level2_plan.left, level2_plan.right)
    
    # Should have JOIN with it1.info = it2.info
    assert "JOIN" in sql_query
    assert "it1.info = it2.info" in sql_query
    
    print("✓ constant equality pipeline works")


def test_modern_join_syntax():
    """Test pipeline with modern JOIN syntax"""
    sql = """
    SELECT * FROM A 
    JOIN B ON A.x = B.y
    JOIN C ON B.y = C.z
    WHERE A.w > 10;
    """
    
    # Parse
    parsed = parse_sql(sql)
    assert len(parsed.tables) == 3
    
    # Enumerate
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    result = enumerator.enumerate_subsets(parsed.tables)
    
    # Should enumerate all subsets
    assert len(result.all_plans) == 7
    
    print("✓ modern JOIN syntax pipeline works")


def test_self_join_pipeline():
    """Test pipeline with self-join"""
    sql = """
    SELECT * FROM users u1, users u2
    WHERE u1.manager_id = u2.id
      AND u1.department = 'Engineering';
    """
    
    # Parse
    parsed = parse_sql(sql)
    assert len(parsed.tables) == 2
    assert 'u1' in parsed.tables
    assert 'u2' in parsed.tables
    
    # Enumerate
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    result = enumerator.enumerate_subsets(parsed.tables)
    
    # Should enumerate {u1, u2}
    assert result.counts[2] == 1
    
    # Generate SQL
    generator = SubqueryGenerator(parsed.aliases, parsed.classifier, parsed.join_graph)
    
    level2_plan = [p for p in result.all_plans if len(p.subset) == 2][0]
    sql_query = generator.generate_subquery(level2_plan.subset, level2_plan.left, level2_plan.right)
    
    # Should have proper aliasing
    assert "users u1" in sql_query
    assert "users u2" in sql_query
    
    print("✓ self-join pipeline works")


def test_disconnected_tables():
    """Test that disconnected tables are not joined"""
    sql = """
    SELECT * FROM A, B
    WHERE A.x > 10 AND B.y < 20;
    """
    
    # Parse
    parsed = parse_sql(sql)
    assert len(parsed.tables) == 2
    
    # Enumerate
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    result = enumerator.enumerate_subsets(parsed.tables)
    
    # Should only enumerate base tables (no level-2)
    assert result.counts[1] == 2
    assert result.counts.get(2, 0) == 0
    assert len(result.all_plans) == 2
    
    print("✓ disconnected tables pipeline works")


def test_in_operator_constant_equality():
    """Test IN operator with single value creates constant-equality join"""
    sql = """
    SELECT * FROM kind_type kt1, kind_type kt2
    WHERE kt1.kind IN ('tv series')
      AND kt2.kind IN ('tv series');
    """
    
    # Parse
    parsed = parse_sql(sql)
    
    # Enumerate
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    result = enumerator.enumerate_subsets(parsed.tables)
    
    # Should enumerate {kt1, kt2}
    assert result.counts[2] == 1
    
    print("✓ IN operator constant equality works")


def test_in_operator_multiple_values_no_join():
    """Test IN operator with multiple values does NOT create join"""
    sql = """
    SELECT * FROM A, B
    WHERE A.type IN ('x', 'y')
      AND B.type IN ('x', 'y');
    """
    
    # Parse
    parsed = parse_sql(sql)
    
    # Enumerate
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    result = enumerator.enumerate_subsets(parsed.tables)
    
    # Should NOT enumerate {A, B} (multiple values in IN)
    assert result.counts.get(2, 0) == 0
    
    print("✓ IN operator multiple values correctly rejected")


def test_subset_formatting():
    """Test subset formatting"""
    subset = {'c', 'a', 'b'}
    formatted = format_subset(subset)
    
    # Should be sorted
    assert formatted == '{a, b, c}'
    
    print("✓ subset formatting works")


def test_max_level_limit():
    """Test that enumeration respects max_level"""
    sql = """
    SELECT * FROM A, B, C, D
    WHERE A.x = B.y 
      AND B.y = C.z 
      AND C.z = D.w;
    """
    
    # Parse
    parsed = parse_sql(sql)
    
    # Enumerate with max_level=2
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    result = enumerator.enumerate_subsets(parsed.tables, max_level=2)
    
    # Should only enumerate up to level 2
    assert 1 in result.counts
    assert 2 in result.counts
    assert 3 not in result.counts
    assert 4 not in result.counts
    
    print("✓ max_level limit works")


if __name__ == '__main__':
    print("\nRunning integration tests...\n")
    
    test_simple_join_pipeline()
    test_chain_join_pipeline()
    test_constant_equality_pipeline()
    test_modern_join_syntax()
    test_self_join_pipeline()
    test_disconnected_tables()
    test_in_operator_constant_equality()
    test_in_operator_multiple_values_no_join()
    test_subset_formatting()
    test_max_level_limit()
    
    print("\n✅ All integration tests passed!\n")
