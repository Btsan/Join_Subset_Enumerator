"""
Test basic parser data structures

This tests that the data structures are properly defined and can be instantiated
"""

from constants import (
    JoinDetail,
    TransitiveJoin,
    ConstantValue,
    ConstantEqualityJoin,
    JoinCondition,
    PredicateSet,
    Decomposition,
    JoinPredicate,
    NextTable,
    EnumerationPlan,
    EnumerationResult
)


def test_join_detail():
    """Test JoinDetail dataclass"""
    jd = JoinDetail(
        t1='a',
        t1_col='x',
        t2='b',
        t2_col='y',
        is_original=True
    )
    assert jd.t1 == 'a'
    assert jd.t2 == 'b'
    assert jd.is_original is True
    print("✓ JoinDetail works")


def test_transitive_join():
    """Test TransitiveJoin dataclass"""
    tj = TransitiveJoin(
        t1='a',
        t1_col='x',
        t2='c',
        t2_col='z'
    )
    assert tj.t1 == 'a'
    assert tj.t2 == 'c'
    print("✓ TransitiveJoin works")


def test_constant_value():
    """Test ConstantValue dataclass"""
    cv = ConstantValue(
        table='t1',
        column='status',
        value='active'
    )
    assert cv.table == 't1'
    assert cv.column == 'status'
    assert cv.value == 'active'
    print("✓ ConstantValue works")


def test_constant_equality_join():
    """Test ConstantEqualityJoin dataclass"""
    cej = ConstantEqualityJoin(
        t1='t1',
        t2='t2',
        column='status'
    )
    assert cej.t1 == 't1'
    assert cej.t2 == 't2'
    assert cej.column == 'status'
    print("✓ ConstantEqualityJoin works")


def test_join_condition():
    """Test JoinCondition dataclass"""
    jc = JoinCondition(
        left_table='a',
        left_column='x',
        right_table='b',
        right_column='y'
    )
    assert jc.left_table == 'a'
    assert jc.right_column == 'y'
    print("✓ JoinCondition works")


def test_predicate_set():
    """Test PredicateSet dataclass"""
    ps = PredicateSet(
        selections=['a.x > 10'],
        joins=['a.x = b.y'],
        complex=['a.x + b.y = c.z']
    )
    assert len(ps.selections) == 1
    assert len(ps.joins) == 1
    assert len(ps.complex) == 1
    print("✓ PredicateSet works")


def test_decomposition():
    """Test Decomposition dataclass"""
    decomp = Decomposition(
        left={'a', 'b'},
        right={'c'}
    )
    assert len(decomp.left) == 2
    assert len(decomp.right) == 1
    assert 'a' in decomp.left
    print("✓ Decomposition works")


def test_join_predicate():
    """Test JoinPredicate dataclass"""
    jp = JoinPredicate(
        predicate='a.x = b.y',
        is_original=True
    )
    assert jp.predicate == 'a.x = b.y'
    assert jp.is_original is True
    print("✓ JoinPredicate works")


def test_next_table():
    """Test NextTable dataclass"""
    nt = NextTable(
        table='c',
        join_pred=JoinPredicate('b.y = c.z', True)
    )
    assert nt.table == 'c'
    assert nt.join_pred.predicate == 'b.y = c.z'
    print("✓ NextTable works")


def test_enumeration_plan():
    """Test EnumerationPlan dataclass"""
    plan = EnumerationPlan(
        subset={'a', 'b'},
        left={'a'},
        right={'b'},
        sql='SELECT * FROM a JOIN b ON a.x = b.y;'
    )
    assert len(plan.subset) == 2
    assert 'a' in plan.subset
    assert plan.left == {'a'}
    print("✓ EnumerationPlan works")


def test_enumeration_result():
    """Test EnumerationResult dataclass"""
    plan1 = EnumerationPlan({'a'}, None, None, 'SELECT * FROM a;')
    plan2 = EnumerationPlan({'b'}, None, None, 'SELECT * FROM b;')
    
    result = EnumerationResult(
        all_plans=[plan1, plan2],
        counts={1: 2}
    )
    assert len(result.all_plans) == 2
    assert result.counts[1] == 2
    print("✓ EnumerationResult works")


if __name__ == '__main__':
    print("\nTesting data structures...\n")
    
    test_join_detail()
    test_transitive_join()
    test_constant_value()
    test_constant_equality_join()
    test_join_condition()
    test_predicate_set()
    test_decomposition()
    test_join_predicate()
    test_next_table()
    test_enumeration_plan()
    test_enumeration_result()
    
    print("\n✅ All data structure tests passed!\n")
