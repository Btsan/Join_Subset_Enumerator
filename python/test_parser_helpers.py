"""
Test parser helper functions

Tests the query extraction and constant-value detection functions
"""

import re
from parser import (
    _normalize_value,
    _extract_single_constant_value
)
from predicates import PredicateClassifier


def test_normalize_value():
    """Test value normalization"""
    # Single quotes
    assert _normalize_value("'active'") == 'active'
    
    # Double quotes
    assert _normalize_value('"active"') == 'active'
    
    # With type cast
    assert _normalize_value("'2024-01-01'::timestamp") == '2024-01-01'
    
    # Number
    assert _normalize_value('123') == '123'
    
    # With whitespace
    assert _normalize_value("  'value'  ") == 'value'
    
    print("✓ _normalize_value works")


def test_extract_single_constant_value_equality():
    """Test extracting constant value from = predicates"""
    # Simple equality
    result = _extract_single_constant_value("t1.status = 'active'")
    assert result is not None
    assert result.table == 't1'
    assert result.column == 'status'
    assert result.value == 'active'
    
    # With type cast
    result = _extract_single_constant_value("t2.date = '2024-01-01'::timestamp")
    assert result is not None
    assert result.table == 't2'
    assert result.value == '2024-01-01'
    
    # Number
    result = _extract_single_constant_value("t3.id = 123")
    assert result is not None
    assert result.value == '123'
    
    print("✓ Extract single constant from = works")


def test_extract_single_constant_value_in_single():
    """Test extracting constant value from IN with single value"""
    # Single value in IN
    result = _extract_single_constant_value("t1.kind IN ('tv series')")
    assert result is not None
    assert result.table == 't1'
    assert result.column == 'kind'
    assert result.value == 'tv series'
    
    # Single value with spaces
    result = _extract_single_constant_value("t2.status IN ( 'active' )")
    assert result is not None
    assert result.value == 'active'
    
    print("✓ Extract single constant from IN (single value) works")


def test_extract_single_constant_value_in_multiple():
    """Test that IN with multiple values returns None"""
    # Multiple values - should return None
    result = _extract_single_constant_value("t1.status IN ('active', 'pending')")
    assert result is None
    
    result = _extract_single_constant_value("t2.type IN ('a', 'b', 'c')")
    assert result is None
    
    print("✓ Extract rejects IN with multiple values")


def test_extract_single_constant_value_invalid():
    """Test that non-equality predicates return None"""
    # Greater than
    result = _extract_single_constant_value("t1.value > 10")
    assert result is None
    
    # Less than
    result = _extract_single_constant_value("t2.value <= 100")
    assert result is None
    
    # LIKE
    result = _extract_single_constant_value("t3.name LIKE 'abc%'")
    assert result is None
    
    # IS NULL
    result = _extract_single_constant_value("t4.col IS NULL")
    assert result is None
    
    print("✓ Extract rejects non-equality predicates")


def test_constant_equality_detection_scenario():
    """
    Test constant equality detection with realistic scenario
    
    Simulates: t1.status = 'active' AND t2.status = 'active'
    Should infer: t1.status = t2.status
    """
    from parser import _detect_constant_equality_joins
    
    # Create classifier with predicates
    classifier = PredicateClassifier()
    classifier.add_predicate("t1.status = 'active'", {'t1'})
    classifier.add_predicate("t2.status = 'active'", {'t2'})
    classifier.add_predicate("t1.value > 10", {'t1'})  # Non-constant
    classifier.classify_predicates()
    
    # Detect constant joins
    tables = ['t1', 't2']
    const_joins = _detect_constant_equality_joins(classifier, tables)
    
    # Should find one join: t1.status = t2.status
    assert len(const_joins) == 1
    assert const_joins[0].column == 'status'
    assert {const_joins[0].t1, const_joins[0].t2} == {'t1', 't2'}
    
    print("✓ Constant equality detection works")


def test_constant_equality_no_match():
    """
    Test that different constants don't create joins
    
    t1.status = 'active' AND t2.status = 'pending' -> NO join
    """
    from parser import _detect_constant_equality_joins
    
    classifier = PredicateClassifier()
    classifier.add_predicate("t1.status = 'active'", {'t1'})
    classifier.add_predicate("t2.status = 'pending'", {'t2'})
    classifier.classify_predicates()
    
    tables = ['t1', 't2']
    const_joins = _detect_constant_equality_joins(classifier, tables)
    
    # Should find no joins (different constants)
    assert len(const_joins) == 0
    
    print("✓ Different constants correctly rejected")


def test_constant_equality_three_tables():
    """
    Test constant equality with three tables
    
    t1.col = 'X' AND t2.col = 'X' AND t3.col = 'X'
    Should create 3 joins: t1-t2, t1-t3, t2-t3
    """
    from parser import _detect_constant_equality_joins
    
    classifier = PredicateClassifier()
    classifier.add_predicate("t1.col = 'X'", {'t1'})
    classifier.add_predicate("t2.col = 'X'", {'t2'})
    classifier.add_predicate("t3.col = 'X'", {'t3'})
    classifier.classify_predicates()
    
    tables = ['t1', 't2', 't3']
    const_joins = _detect_constant_equality_joins(classifier, tables)
    
    # Should find 3 joins (all pairs)
    assert len(const_joins) == 3
    
    # Check all pairs exist
    pairs = {(j.t1, j.t2) for j in const_joins}
    pairs |= {(j.t2, j.t1) for j in const_joins}  # Add reverse pairs
    
    assert ('t1', 't2') in pairs or ('t2', 't1') in pairs
    assert ('t1', 't3') in pairs or ('t3', 't1') in pairs
    assert ('t2', 't3') in pairs or ('t3', 't2') in pairs
    
    print("✓ Three-table constant equality works")


if __name__ == '__main__':
    print("\nTesting parser helper functions...\n")
    
    test_normalize_value()
    test_extract_single_constant_value_equality()
    test_extract_single_constant_value_in_single()
    test_extract_single_constant_value_in_multiple()
    test_extract_single_constant_value_invalid()
    test_constant_equality_detection_scenario()
    test_constant_equality_no_match()
    test_constant_equality_three_tables()
    
    print("\n✅ All parser tests passed!\n")
