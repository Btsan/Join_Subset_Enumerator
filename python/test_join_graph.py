"""
Test JoinGraph implementation

Tests equivalence class construction, transitive closure, and connectivity
"""

from join_graph import JoinGraph


def test_add_join_basic():
    """Test adding a basic join"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    
    # Check edge was added (normalized)
    assert 'a|||b' in jg.edges
    
    # Check details were stored
    assert 'a|||b' in jg.join_details
    assert len(jg.join_details['a|||b']) == 1
    
    detail = jg.join_details['a|||b'][0]
    assert detail.is_original is True
    
    print("✓ add_join basic works")


def test_add_join_normalization():
    """Test that joins are normalized (sorted)"""
    jg = JoinGraph()
    
    # Add in different orders
    jg.add_join('b', 'a', 'y', 'x', is_original=True)
    
    # Should normalize to a|||b
    assert 'a|||b' in jg.edges
    assert 'b|||a' not in jg.edges  # Reverse should not exist
    
    print("✓ add_join normalization works")


def test_build_equivalence_classes_simple():
    """Test EC construction with simple chain"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'y', 'z', is_original=True)
    
    num_ecs = jg.build_equivalence_classes()
    
    # Should have 1 EC: {a.x, b.y, c.z}
    assert num_ecs == 1
    assert len(jg.equivalence_classes) == 1
    
    ec = jg.equivalence_classes[0]
    assert 'a.x' in ec
    assert 'b.y' in ec
    assert 'c.z' in ec
    
    print("✓ build_equivalence_classes simple works")


def test_build_equivalence_classes_multiple():
    """Test EC construction with multiple independent chains"""
    jg = JoinGraph()
    
    # Chain 1: a.x = b.y
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    
    # Chain 2: c.z = d.w
    jg.add_join('c', 'd', 'z', 'w', is_original=True)
    
    num_ecs = jg.build_equivalence_classes()
    
    # Should have 2 ECs
    assert num_ecs == 2
    assert len(jg.equivalence_classes) == 2
    
    # Find the two ECs
    ec1 = next(ec for ec in jg.equivalence_classes if 'a.x' in ec)
    ec2 = next(ec for ec in jg.equivalence_classes if 'c.z' in ec)
    
    assert 'a.x' in ec1 and 'b.y' in ec1
    assert 'c.z' in ec2 and 'd.w' in ec2
    
    print("✓ build_equivalence_classes multiple works")


def test_are_in_same_ec():
    """Test checking if tables share an EC"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'y', 'z', is_original=True)
    jg.build_equivalence_classes()
    
    # a and b share EC
    assert jg.are_in_same_ec('a', 'b') is True
    
    # a and c share EC (transitively)
    assert jg.are_in_same_ec('a', 'c') is True
    
    # b and c share EC
    assert jg.are_in_same_ec('b', 'c') is True
    
    print("✓ are_in_same_ec works")


def test_are_in_same_ec_negative():
    """Test that unconnected tables don't share EC"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.build_equivalence_classes()
    
    # c is not connected
    assert jg.are_in_same_ec('a', 'c') is False
    assert jg.are_in_same_ec('b', 'c') is False
    
    print("✓ are_in_same_ec negative works")


def test_is_connected_simple():
    """Test connectivity check for simple chain"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'y', 'z', is_original=True)
    jg.build_equivalence_classes()
    
    # All connected
    assert jg.is_connected({'a', 'b', 'c'}) is True
    assert jg.is_connected({'a', 'b'}) is True
    assert jg.is_connected({'b', 'c'}) is True
    assert jg.is_connected({'a', 'c'}) is True
    
    print("✓ is_connected simple works")


def test_is_connected_disconnected():
    """Test that disconnected subsets are detected"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.build_equivalence_classes()
    
    # c is not connected to a-b chain
    assert jg.is_connected({'a', 'b', 'c'}) is False
    assert jg.is_connected({'a', 'c'}) is False
    
    print("✓ is_connected disconnected works")


def test_can_join():
    """Test checking if two subsets can join"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'y', 'z', is_original=True)
    jg.build_equivalence_classes()
    
    # {a} can join with {b}
    assert jg.can_join({'a'}, {'b'}) is True
    
    # {a, b} can join with {c}
    assert jg.can_join({'a', 'b'}, {'c'}) is True
    
    # {a} can join with {c} (transitively)
    assert jg.can_join({'a'}, {'c'}) is True
    
    print("✓ can_join works")


def test_compute_transitive_closure():
    """Test transitive closure computation"""
    jg = JoinGraph()
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'y', 'z', is_original=True)
    
    # Compute transitive closure
    added = jg.compute_transitive_closure()
    
    # Should add a.x = c.z
    assert added >= 1
    
    # Check that a-c edge exists
    edge_ac = 'a|||c'
    assert edge_ac in jg.join_details
    
    # Check it's marked as transitive
    detail = jg.join_details[edge_ac][0]
    assert detail.is_original is False
    
    print("✓ compute_transitive_closure works")


def test_transitive_closure_column_aware():
    """Test that transitive closure is column-aware"""
    jg = JoinGraph()
    
    # A.x = B.y AND B.y = C.z => A.x = C.z (valid, columns match on B)
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'y', 'z', is_original=True)
    
    added = jg.compute_transitive_closure()
    
    # Should add transitive join
    assert added >= 1
    assert 'a|||c' in jg.join_details
    
    print("✓ transitive_closure column-aware works")


def test_transitive_closure_no_match():
    """Test that transitive closure doesn't add when columns don't match"""
    jg = JoinGraph()
    
    # A.x = B.y AND B.z = C.w => NO transitive (different columns on B)
    jg.add_join('a', 'b', 'x', 'y', is_original=True)
    jg.add_join('b', 'c', 'z', 'w', is_original=True)
    
    added = jg.compute_transitive_closure()
    
    # Should NOT add a-c join (columns don't match on B)
    assert added == 0
    assert 'a|||c' not in jg.join_details
    
    print("✓ transitive_closure rejects non-matching columns")


def test_self_join_not_connected():
    """Test that self-joins without condition are not connected"""
    jg = JoinGraph()
    
    # No join condition between t1 and t2 (same base table)
    jg.table_aliases['t1'] = 'table'
    jg.table_aliases['t2'] = 'table'
    
    jg.build_equivalence_classes()
    
    # Should NOT be connected (no join condition)
    assert jg.is_connected({'t1', 't2'}) is False
    
    print("✓ self_join without condition not connected")


def test_self_join_with_condition():
    """Test that self-joins WITH condition are connected"""
    jg = JoinGraph()
    
    # t1.x = t2.x (explicit join condition)
    jg.table_aliases['t1'] = 'table'
    jg.table_aliases['t2'] = 'table'
    jg.add_join('t1', 't2', 'x', 'x', is_original=True)
    
    jg.build_equivalence_classes()
    
    # Should be connected (explicit join)
    assert jg.is_connected({'t1', 't2'}) is True
    
    print("✓ self_join with condition is connected")


if __name__ == '__main__':
    print("\nTesting JoinGraph...\n")
    
    test_add_join_basic()
    test_add_join_normalization()
    test_build_equivalence_classes_simple()
    test_build_equivalence_classes_multiple()
    test_are_in_same_ec()
    test_are_in_same_ec_negative()
    test_is_connected_simple()
    test_is_connected_disconnected()
    test_can_join()
    test_compute_transitive_closure()
    test_transitive_closure_column_aware()
    test_transitive_closure_no_match()
    test_self_join_not_connected()
    test_self_join_with_condition()
    
    print("\n✅ All JoinGraph tests passed!\n")
