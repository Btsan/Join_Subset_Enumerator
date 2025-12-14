"""
Test PostgreSQLJoinEnumerator implementation

Tests dynamic programming enumeration and ordering
"""

from join_graph import JoinGraph
from enumerator import PostgreSQLJoinEnumerator


def test_enumerate_single_table():
    """Test enumeration with single table"""
    jg = JoinGraph()
    jg.build_equivalence_classes()
    
    enum = PostgreSQLJoinEnumerator(jg)
    result = enum.enumerate_subsets(['a'])
    
    # Should have 1 subset at level 1
    assert len(result.all_plans) == 1
    assert result.counts[1] == 1
    assert result.all_plans[0].subset == {'a'}
    
    print("✓ enumerate single table works")


def test_enumerate_disconnected_tables():
    """Test enumeration with disconnected tables"""
    jg = JoinGraph()
    jg.build_equivalence_classes()
    
    enum = PostgreSQLJoinEnumerator(jg)
    result = enum.enumerate_subsets(['a', 'b'])
    
    # Should only have 2 base tables (no level-2)
    assert len(result.all_plans) == 2
    assert result.counts[1] == 2
    assert result.counts.get(2, 0) == 0  # No level-2 subsets
    
    print("✓ enumerate disconnected tables works")


def test_enumerate_simple_join():
    """Test enumeration with simple join"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.build_equivalence_classes()
    
    enum = PostgreSQLJoinEnumerator(jg)
    result = enum.enumerate_subsets(['a', 'b'])
    
    # Should have 2 at level 1, 1 at level 2
    assert result.counts[1] == 2  # {a}, {b}
    assert result.counts[2] == 1  # {a, b}
    assert len(result.all_plans) == 3
    
    # Check level-2 subset
    level2 = [p for p in result.all_plans if len(p.subset) == 2][0]
    assert level2.subset == {'a', 'b'}
    assert level2.left == {'a'}
    assert level2.right == {'b'}
    
    print("✓ enumerate simple join works")


def test_enumerate_chain():
    """Test enumeration with chain: A-B-C"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'y', 'z', is_original=True)
    jg.compute_transitive_closure()  # Adds a-c
    jg.build_equivalence_classes()
    
    enum = PostgreSQLJoinEnumerator(jg)
    result = enum.enumerate_subsets(['a', 'b', 'c'])
    
    # Level 1: {a}, {b}, {c}
    assert result.counts[1] == 3
    
    # Level 2: {a,b}, {b,c}, {a,c}
    assert result.counts[2] == 3
    
    # Level 3: {a,b,c}
    assert result.counts[3] == 1
    
    # Total: 3 + 3 + 1 = 7
    assert len(result.all_plans) == 7
    
    print("✓ enumerate chain works")


def test_enumerate_star():
    """Test enumeration with star: center connected to s1, s2, s3"""
    jg = JoinGraph()
    jg.add_join('center', 's1', 'id', 'cid', is_original=True)
    jg.add_join('center', 's2', 'id', 'cid', is_original=True)
    jg.add_join('center', 's3', 'id', 'cid', is_original=True)
    jg.compute_transitive_closure()  # Adds s1-s2, s1-s3, s2-s3
    jg.build_equivalence_classes()
    
    enum = PostgreSQLJoinEnumerator(jg)
    result = enum.enumerate_subsets(['center', 's1', 's2', 's3'])
    
    # Level 1: 4 tables
    assert result.counts[1] == 4
    
    # Level 2: All pairs should be connected (via transitive closure)
    # {center,s1}, {center,s2}, {center,s3}, {s1,s2}, {s1,s3}, {s2,s3}
    assert result.counts[2] == 6
    
    print("✓ enumerate star works")


def test_enumerate_ordering():
    """Test that enumeration maintains correct ordering"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'y', 'z', is_original=True)
    jg.compute_transitive_closure()
    jg.build_equivalence_classes()
    
    enum = PostgreSQLJoinEnumerator(jg)
    result = enum.enumerate_subsets(['a', 'b', 'c'])
    
    # Check order: level 1, then level 2, then level 3
    # Within each level, alphabetically sorted
    
    # Level 1 subsets
    level1 = result.all_plans[0:3]
    assert level1[0].subset == {'a'}
    assert level1[1].subset == {'b'}
    assert level1[2].subset == {'c'}
    
    # Level 2 subsets (sorted)
    level2 = result.all_plans[3:6]
    subsets_l2 = [p.subset for p in level2]
    assert {'a', 'b'} in subsets_l2
    assert {'a', 'c'} in subsets_l2
    assert {'b', 'c'} in subsets_l2
    
    # Level 3 subset
    level3 = result.all_plans[6]
    assert level3.subset == {'a', 'b', 'c'}
    
    print("✓ enumerate ordering works")


def test_enumerate_max_level():
    """Test that max_level limits enumeration"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'y', 'z', is_original=True)
    jg.add_join('c', 'd', 'z', 'w', is_original=True)
    jg.compute_transitive_closure()
    jg.build_equivalence_classes()
    
    enum = PostgreSQLJoinEnumerator(jg)
    result = enum.enumerate_subsets(['a', 'b', 'c', 'd'], max_level=2)
    
    # Should only enumerate up to level 2
    assert 3 not in result.counts
    assert 4 not in result.counts
    
    # Should have level 1 and 2
    assert 1 in result.counts
    assert 2 in result.counts
    
    print("✓ enumerate max_level works")


def test_find_decomposition():
    """Test that decompositions are found correctly"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.build_equivalence_classes()
    
    enum = PostgreSQLJoinEnumerator(jg)
    result = enum.enumerate_subsets(['a', 'b'])
    
    # Find the level-2 plan
    level2_plan = next(p for p in result.all_plans if len(p.subset) == 2)
    
    # Should have decomposition
    assert level2_plan.left is not None
    assert level2_plan.right is not None
    assert level2_plan.left | level2_plan.right == {'a', 'b'}
    
    print("✓ find_decomposition works")


def test_enumerate_constant_equality():
    """Test enumeration with constant-equality join"""
    jg = JoinGraph()
    
    # Simulate constant-equality join: t1.col = t2.col (via same constant)
    jg.add_join('t1', 't2', 'status', 'status', is_original=False)
    jg.build_equivalence_classes()
    
    enum = PostgreSQLJoinEnumerator(jg)
    result = enum.enumerate_subsets(['t1', 't2'])
    
    # Should enumerate {t1, t2} even though it's a transitive join
    assert result.counts[2] == 1
    
    level2 = [p for p in result.all_plans if len(p.subset) == 2][0]
    assert level2.subset == {'t1', 't2'}
    
    print("✓ enumerate constant_equality works")


if __name__ == '__main__':
    print("\nTesting PostgreSQLJoinEnumerator...\n")
    
    test_enumerate_single_table()
    test_enumerate_disconnected_tables()
    test_enumerate_simple_join()
    test_enumerate_chain()
    test_enumerate_star()
    test_enumerate_ordering()
    test_enumerate_max_level()
    test_find_decomposition()
    test_enumerate_constant_equality()
    
    print("\n✅ All enumerator tests passed!\n")
