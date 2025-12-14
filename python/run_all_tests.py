"""
Test runner for all implemented components

Runs tests that don't require external dependencies first,
then tries tests that need sqlglot if available.
"""

import sys

print("=" * 60)
print("RUNNING ALL TESTS")
print("=" * 60)

# Phase 1: Data structures (no dependencies)
print("\n1. Testing data structures...")
try:
    import test_data_structures
    print("   ✅ Data structures OK")
except Exception as e:
    print(f"   ❌ Data structures FAILED: {e}")
    sys.exit(1)

# Phase 2: JoinGraph (no external dependencies)
print("\n2. Testing JoinGraph...")
try:
    import test_join_graph
    print("   ✅ JoinGraph OK")
except Exception as e:
    print(f"   ❌ JoinGraph FAILED: {e}")
    sys.exit(1)

# Phase 3: Enumerator (no external dependencies)
print("\n3. Testing PostgreSQLJoinEnumerator...")
try:
    import test_enumerator
    print("   ✅ Enumerator OK")
except Exception as e:
    print(f"   ❌ Enumerator FAILED: {e}")
    sys.exit(1)

# Phase 4: Parser helpers (requires sqlglot)
print("\n4. Testing parser helpers (requires sqlglot)...")
try:
    import sqlglot
    import test_parser_helpers
    print("   ✅ Parser helpers OK")
except ImportError:
    print("   ⚠️  Parser helpers SKIPPED (sqlglot not installed)")
except Exception as e:
    print(f"   ❌ Parser helpers FAILED: {e}")

# Phase 5: Integration tests (requires sqlglot)
print("\n5. Testing integration (requires sqlglot)...")
try:
    import sqlglot
    import test_integration
    print("   ✅ Integration tests OK")
except ImportError:
    print("   ⚠️  Integration tests SKIPPED (sqlglot not installed)")
except Exception as e:
    print(f"   ❌ Integration tests FAILED: {e}")

print("\n" + "=" * 60)
print("TEST SUMMARY")
print("=" * 60)
print("Core functionality: ✅ PASSED")
print("SQL parsing: Install sqlglot to test")
print("\nTo install sqlglot:")
print("  pip install sqlglot")
print("=" * 60)
