import React, { useState } from 'react';

/**
 * PostgreSQL Join Enumerator with Equivalence Class Support
 * 
 * This tool implements PostgreSQL-style join enumeration for cardinality estimation research.
 * 
 * Key Features:
 * - Equivalence Classes (ECs): Groups columns that must be equal (PostgreSQL's approach)
 * - Column-Aware Transitive Closure: Only infers transitivity when columns match
 * - Dynamic Programming: Enumerates subsets level-by-level using previously enumerated subsets
 * - Complete Join Conditions: Generated SQL includes ALL join predicates, not just spanning tree
 * 
 * Algorithm Overview:
 * 1. Parse query to extract tables and join predicates
 * 2. Build Equivalence Classes from join predicates (Union-Find)
 * 3. Compute transitive closure (column-aware)
 * 4. Enumerate subsets level-by-level:
 *    - Check connectivity via ECs
 *    - Find decomposition using DP table
 *    - Generate SQL with complete join conditions
 * 
 * See RESEARCH_DOCUMENTATION.md for detailed explanation of algorithms and design decisions.
 */

// Join Graph Class with Equivalence Class Support
class JoinGraph {
  constructor() {
    this.edges = new Set(); // Still keep for quick connectivity checks
    this.joinDetails = new Map(); // table1|||table2 -> [{t1col, t2col}, ...]
    this.tableAliases = new Map(); // alias -> base table name
    this.equivalenceClasses = []; // Array of Sets, each Set contains "table.column" strings
  }

  setTableAlias(alias, baseTableName) {
    this.tableAliases.set(alias, baseTableName);
  }

  // Build equivalence classes from join predicates
  buildEquivalenceClasses() {
    console.log('\n🔍 Building Equivalence Classes...');

    // Process each join predicate using Union-Find approach
    for (let [edge, details] of this.joinDetails.entries()) {
      for (let detail of details) {
        const left = `${detail.t1}.${detail.t1col}`;
        const right = `${detail.t2}.${detail.t2col}`;

        // Find ECs containing these columns
        let leftEC = this.equivalenceClasses.find(ec => ec.has(left));
        let rightEC = this.equivalenceClasses.find(ec => ec.has(right));

        if (leftEC && rightEC && leftEC !== rightEC) {
          // Merge two different ECs
          for (let member of rightEC) leftEC.add(member);
          this.equivalenceClasses = this.equivalenceClasses.filter(ec => ec !== rightEC);
        } else if (leftEC) {
          leftEC.add(right);
        } else if (rightEC) {
          rightEC.add(left);
        } else {
          // Create new EC
          this.equivalenceClasses.push(new Set([left, right]));
        }
      }
    }

    // Display final ECs
    console.log(`  Built ${this.equivalenceClasses.length} Equivalence Class(es):`);
    for (let i = 0; i < this.equivalenceClasses.length; i++) {
      const members = Array.from(this.equivalenceClasses[i]);
      const tables = [...new Set(members.map(m => m.split('.')[0]))];
      console.log(`    EC${i + 1}: {${members.join(', ')}} [${tables.length} tables]`);
    }
  }

  // Check if two tables are in the same equivalence class
  areInSameEC(t1, t2) {
    for (let ec of this.equivalenceClasses) {
      let hasT1 = false;
      let hasT2 = false;

      for (let member of ec) {
        const [table, col] = member.split('.');
        if (table === t1) hasT1 = true;
        if (table === t2) hasT2 = true;
      }

      if (hasT1 && hasT2) {
        return true;
      }
    }
    return false;
  }

  addJoin(t1, t2, t1col = null, t2col = null, isOriginal = true) {
    const edge = [t1, t2].sort().join('|||');
    this.edges.add(edge);

    // Track column details with original vs transitive flag
    if (t1col && t2col) {
      if (!this.joinDetails.has(edge)) {
        this.joinDetails.set(edge, []);
      }
      // Normalize: store in sorted order
      const [table1, table2] = [t1, t2].sort();
      const detail = table1 === t1
        ? { t1: table1, t1col, t2: table2, t2col, isOriginal }
        : { t1: table1, t1col: t2col, t2: table2, t2col: t1col, isOriginal };

      this.joinDetails.get(edge).push(detail);
    }
  }

  // Check if two subsets can be joined
  // Returns true if any table from left shares an EC with any table from right
  canJoin(left, right) {
    for (let l of left) {
      for (let r of right) {
        if (this.areInSameEC(l, r)) {
          return true;
        }
      }
    }
    return false;
  }

  isConnected(subset) {
    if (subset.size <= 1) return true;

    const subsetArray = Array.from(subset);

    // Check EC connectivity with BFS
    const visited = new Set([subsetArray[0]]);
    const queue = [subsetArray[0]];

    while (queue.length > 0) {
      const current = queue.shift();
      for (let other of subsetArray) {
        if (!visited.has(other)) {
          // Check if connected via equivalence class
          if (this.areInSameEC(current, other)) {
            visited.add(other);
            queue.push(other);
            continue;
          }

          // Check explicit edge (fallback for non-EC connections)
          const edge = [current, other].sort().join('|||');
          if (this.edges.has(edge)) {
            visited.add(other);
            queue.push(other);
          }
        }
      }
    }

    return visited.size === subset.size;
  }

  // Helper: Check if two join details can form a transitive join
  // Returns the transitive join if columns match on shared table, null otherwise
  tryFormTransitiveJoin(d1, d2) {
    // Check 4 cases for how two joins might share a table with matching columns
    if (d1.t2 === d2.t1 && d1.t2col === d2.t1col) {
      return { t1: d1.t1, t1col: d1.t1col, t2: d2.t2, t2col: d2.t2col };
    } else if (d1.t2 === d2.t2 && d1.t2col === d2.t2col) {
      return { t1: d1.t1, t1col: d1.t1col, t2: d2.t1, t2col: d2.t1col };
    } else if (d1.t1 === d2.t1 && d1.t1col === d2.t1col) {
      return { t1: d1.t2, t1col: d1.t2col, t2: d2.t2, t2col: d2.t2col };
    } else if (d1.t1 === d2.t2 && d1.t1col === d2.t2col) {
      return { t1: d1.t2, t1col: d1.t2col, t2: d2.t1, t2col: d2.t1col };
    }
    return null;
  }

  // Helper: Check if a join already exists in joinDetails
  joinExists(transitiveJoin) {
    const edge = [transitiveJoin.t1, transitiveJoin.t2].sort().join('|||');
    const existing = this.joinDetails.get(edge);

    if (!existing) return false;

    return existing.some(e =>
      (e.t1 === transitiveJoin.t1 && e.t1col === transitiveJoin.t1col &&
        e.t2 === transitiveJoin.t2 && e.t2col === transitiveJoin.t2col) ||
      (e.t1 === transitiveJoin.t2 && e.t1col === transitiveJoin.t2col &&
        e.t2 === transitiveJoin.t1 && e.t2col === transitiveJoin.t1col)
    );
  }

  // Compute transitive closure with column-aware checking
  // Only adds transitive joins when columns match on the shared table
  computeTransitiveClosure() {
    let addedCount = 0;
    const maxIterations = 10;

    for (let iteration = 0; iteration < maxIterations; iteration++) {
      let foundNew = false;
      const currentEdges = Array.from(this.joinDetails.entries());

      // Try to find new transitive joins
      for (let [edge1Key, details1] of currentEdges) {
        for (let [edge2Key, details2] of currentEdges) {
          if (edge1Key === edge2Key) continue;

          for (let d1 of details1) {
            for (let d2 of details2) {
              const transitiveJoin = this.tryFormTransitiveJoin(d1, d2);

              if (transitiveJoin &&
                transitiveJoin.t1 !== transitiveJoin.t2 &&
                !this.joinExists(transitiveJoin)) {
                this.addJoin(transitiveJoin.t1, transitiveJoin.t2,
                  transitiveJoin.t1col, transitiveJoin.t2col, false);
                addedCount++;
                foundNew = true;
              }
            }
          }
        }
      }

      if (!foundNew) break;
    }

    if (addedCount > 0) {
      console.log(`  Added ${addedCount} transitive join(s)`);
    }

    return addedCount;
  }
}

// PostgreSQL Join Enumerator Class
class PostgreSQLJoinEnumerator {
  constructor(tables, joinGraph) {
    this.tables = tables.sort();
    this.joinGraph = joinGraph;
    this.n = tables.length;
  }

  enumerateAllValid() {
    const allSubplans = [];
    const dpTable = new Set(); // DP table: tracks which subsets are enumerable

    // Level 1: Base tables
    for (let table of this.tables) {
      const subset = new Set([table]);
      const subsetKey = this.getSubsetKey(subset);

      if (!dpTable.has(subsetKey)) {
        allSubplans.push({
          level: 1,
          subset: subset,
          left: null,
          right: null
        });
        dpTable.add(subsetKey);
      }
    }

    // Levels 2 through n
    for (let level = 2; level <= this.n; level++) {
      const levelPlans = this.enumerateLevel(level, dpTable);
      allSubplans.push(...levelPlans);
    }

    return allSubplans;
  }

  enumerateLevel(level, dpTable) {
    const plans = [];
    const subsets = this.combinations(this.tables, level);

    console.log(`\nLevel ${level}: Checking ${subsets.length} possible subsets`);

    let skippedSeen = 0;
    let skippedDisconnected = 0;
    let skippedNoDecomp = 0;

    for (let subsetArray of subsets) {
      const subset = new Set(subsetArray);
      const subsetKey = this.getSubsetKey(subset);

      // Skip if already enumerated
      if (dpTable.has(subsetKey)) {
        skippedSeen++;
        continue;
      }

      // Skip if not connected via equivalence classes
      if (!this.joinGraph.isConnected(subset)) {
        skippedDisconnected++;
        continue;
      }

      // Find valid decomposition using previously enumerated subsets
      const decomposition = this.findValidDecomposition(subset, dpTable);

      if (decomposition) {
        plans.push({
          level: level,
          subset: subset,
          left: decomposition.left,
          right: decomposition.right
        });
        dpTable.add(subsetKey);
      } else {
        skippedNoDecomp++;
      }
    }

    console.log(`  Added: ${plans.length}, Skipped: seen=${skippedSeen}, disconnected=${skippedDisconnected}, no_decomp=${skippedNoDecomp}`);
    return plans;
  }

  findValidDecomposition(subset, dpTable) {
    // Find a valid way to split subset into left and right
    // Both left and right must already be in the DP table
    const subsetArray = Array.from(subset);
    const level = subsetArray.length;

    for (let leftSize = 1; leftSize < level; leftSize++) {
      const rightSize = level - leftSize;

      // Optimization: skip symmetric decompositions
      if (leftSize > rightSize) continue;

      for (let leftArray of this.combinations(subsetArray, leftSize)) {
        const left = new Set(leftArray);
        const right = new Set(subsetArray.filter(t => !left.has(t)));

        const leftKey = this.getSubsetKey(left);
        const rightKey = this.getSubsetKey(right);

        // Both sides must be previously enumerated
        if (!dpTable.has(leftKey) || !dpTable.has(rightKey)) {
          continue;
        }

        // Check if left and right can be joined via shared EC
        if (this.joinGraph.canJoin(left, right)) {
          return { left, right };
        }
      }
    }

    return null;
  }

  getSubsetKey(subset) {
    // Create a canonical string representation of the subset
    return Array.from(subset).sort().join(',');
  }

  combinations(array, size) {
    const result = [];

    const combine = (start, combo) => {
      if (combo.length === size) {
        result.push([...combo]);
        return;
      }

      for (let i = start; i < array.length; i++) {
        combo.push(array[i]);
        combine(i + 1, combo);
        combo.pop();
      }
    };

    combine(0, []);
    return result;
  }

  countByLevel() {
    const allPlans = this.enumerateAllValid();
    const counts = {};

    for (let plan of allPlans) {
      counts[plan.level] = (counts[plan.level] || 0) + 1;
    }

    return counts;
  }
}

// Predicate Classifier
class PredicateClassifier {
  constructor(sql) {
    this.sql = sql;
    this.selectionPredicates = {}; // table -> [predicates]
    this.joinPredicates = []; // [{tables: Set, predicate: string, leftTable, leftCol, rightTable, rightCol}]
    this.complexPredicates = []; // [{tables: Set, predicate: string}]
    this.tableAliases = new Map(); // alias -> full table name
    this.multiTableOrPredicates = []; // Track multi-table OR predicates for expansion
  }

  extractTableFromColumn(column) {
    // Extract table alias from "table.column" or "table.column::type"
    const match = column.match(/^(\w+)\./);
    return match ? match[1] : null;
  }

  extractTablesFromPredicate(predicate) {
    // Find all table references in predicate
    const tables = new Set();
    const regex = /(\w+)\.\w+/g;
    let match;
    while ((match = regex.exec(predicate)) !== null) {
      tables.add(match[1]);
    }
    return tables;
  }

  isEquality(operator) {
    return operator === '=' || operator === '==';
  }

  // Extract individual OR terms from a parenthesized OR predicate
  extractOrTerms(orPredicate) {
    // Remove outer parens if present
    let pred = orPredicate.trim();
    if (pred.startsWith('(') && pred.endsWith(')')) {
      pred = pred.slice(1, -1).trim();
    }

    // Split by OR at top level (respecting nested parens)
    const terms = [];
    let current = '';
    let parenDepth = 0;
    let inString = false;
    let stringChar = null;

    for (let i = 0; i < pred.length; i++) {
      const char = pred[i];
      const remaining = pred.substring(i).toUpperCase();

      if (!inString) {
        if (char === "'" || char === '"') {
          inString = true;
          stringChar = char;
          current += char;
        } else if (char === '(') {
          parenDepth++;
          current += char;
        } else if (char === ')') {
          parenDepth--;
          current += char;
        } else if (parenDepth === 0 && remaining.startsWith('OR ')) {
          const prevChar = i > 0 ? pred[i - 1] : ' ';
          if (/\s/.test(prevChar) || prevChar === ')') {
            terms.push(current.trim());
            current = '';
            i += 2; // Skip 'OR'
            continue;
          } else {
            current += char;
          }
        } else {
          current += char;
        }
      } else {
        if (char === stringChar && (i === 0 || pred[i - 1] !== '\\')) {
          inString = false;
          stringChar = null;
        }
        current += char;
      }
    }

    if (current.trim()) {
      terms.push(current.trim());
    }

    return terms;
  }

  // Expand a multi-table OR to DNF terms
  expandMultiTableOr(orPredicate) {
    const terms = this.extractOrTerms(orPredicate);

    // Each OR term becomes a separate conjunction
    const expansions = [];
    for (let term of terms) {
      // Check if term itself has nested structure
      if (term.includes(' AND ') && !term.includes('(')) {
        // Simple case: "A AND B" 
        expansions.push(term);
      } else if (term.includes('(') && term.includes(' AND ')) {
        // Nested case: "(A AND B)" - recursively handle
        // For now, keep as-is (full DNF expansion is complex)
        expansions.push(term);
      } else {
        // Simple term: just a predicate
        expansions.push(term);
      }
    }

    return expansions;
  }

  classifyPredicates() {
    // Extract WHERE clause
    const whereMatch = this.sql.match(/WHERE\s+(.*?)(?:GROUP BY|ORDER BY|LIMIT|$)/is);
    if (!whereMatch) return;

    const whereClause = whereMatch[1].trim();

    // Split by AND (respects parentheses and quotes)
    const predicates = this.splitByAnd(whereClause);

    for (let pred of predicates) {
      pred = pred.trim();
      if (!pred) continue;

      // Check for top-level OR (no surrounding parens) - warn and skip
      if (this.hasTopLevelOr(pred)) {
        console.warn('Top-level OR predicate detected (not fully supported):', pred);
        // Treat as complex predicate for now
        const tables = this.extractTablesFromPredicate(pred);
        if (tables.size > 0) {
          this.complexPredicates.push({
            tables: tables,
            predicate: pred
          });
        }
        continue;
      }

      const tables = this.extractTablesFromPredicate(pred);

      // Check if it's a join predicate (equality between two tables)
      const joinMatch = pred.match(/(\w+)\.(\w+)\s*(=|==)\s*(\w+)\.(\w+)/);

      if (joinMatch && tables.size === 2) {
        // Join predicate
        const leftTable = joinMatch[1];
        const leftCol = joinMatch[2];
        const rightTable = joinMatch[4];
        const rightCol = joinMatch[5];

        this.joinPredicates.push({
          tables: new Set([leftTable, rightTable]),
          predicate: pred,
          leftTable,
          leftCol,
          rightTable,
          rightCol
        });
      } else if (tables.size === 1) {
        // Selection predicate - check various patterns
        const table = Array.from(tables)[0];

        // Patterns we recognize as selection predicates:
        // 1. Column = value, Column > value, etc.
        // 2. Column LIKE 'pattern'
        // 3. Column NOT LIKE 'pattern'
        // 4. Column IS NULL
        // 5. Column IS NOT NULL
        // 6. Column IN (values)
        // 7. Column NOT IN (values)
        // 8. Parenthesized OR of same table

        if (!this.selectionPredicates[table]) {
          this.selectionPredicates[table] = [];
        }
        this.selectionPredicates[table].push(pred);

      } else if (tables.size > 1) {
        // Multi-table predicate
        // Check if it's a parenthesized OR
        if (pred.trim().startsWith('(') && pred.includes(' OR ')) {
          // Multi-table OR - mark for potential expansion
          this.multiTableOrPredicates.push({
            tables: tables,
            predicate: pred
          });
          console.log('Multi-table OR detected:', pred, '(tables:', Array.from(tables).join(', ') + ')');
        }

        this.complexPredicates.push({
          tables: tables,
          predicate: pred
        });
      }
    }
  }

  hasTopLevelOr(pred) {
    // Check if predicate has OR at top level (not in parens)
    // Remove parens groups first
    let depth = 0;
    let inString = false;
    let stringChar = null;
    let cleaned = '';

    for (let i = 0; i < pred.length; i++) {
      const char = pred[i];

      if (!inString) {
        if (char === "'" || char === '"') {
          inString = true;
          stringChar = char;
        } else if (char === '(') {
          depth++;
        } else if (char === ')') {
          depth--;
        } else if (depth === 0) {
          cleaned += char;
        }
      } else if (char === stringChar && (i === 0 || pred[i - 1] !== '\\')) {
        inString = false;
        stringChar = null;
      }
    }

    // Check if cleaned string contains ' OR '
    return /\s+OR\s+/i.test(cleaned);
  }

  splitByAnd(clause) {
    // Split by AND while respecting:
    // 1. Parentheses (don't split inside parens)
    // 2. String literals (don't split inside quotes)
    // 3. Keywords in strings ('AND Corporation', 'OR gate')
    // 4. BETWEEN...AND (don't split the AND that's part of BETWEEN)

    const predicates = [];
    let current = '';
    let parenDepth = 0;
    let inString = false;
    let stringChar = null;
    let inBetween = false;  // Track if we're inside a BETWEEN clause

    for (let i = 0; i < clause.length; i++) {
      const char = clause[i];
      const remaining = clause.substring(i).toUpperCase();

      if (!inString) {
        if (char === "'" || char === '"') {
          inString = true;
          stringChar = char;
          current += char;
        } else if (char === '(') {
          parenDepth++;
          current += char;
        } else if (char === ')') {
          parenDepth--;
          current += char;
        } else if (parenDepth === 0 && remaining.startsWith('BETWEEN ')) {
          // Entering a BETWEEN clause
          inBetween = true;
          current += char;
        } else if (parenDepth === 0 && remaining.startsWith('AND ')) {
          // Check if this is actually " AND " (not part of a longer word)
          const prevChar = i > 0 ? clause[i - 1] : ' ';
          if (/\s/.test(prevChar) || prevChar === ')') {
            if (inBetween) {
              // This is the AND in "BETWEEN x AND y" - don't split
              inBetween = false;  // Reset flag after consuming the AND
              current += char;
            } else {
              // This is a top-level AND separator - split here
              predicates.push(current.trim());
              current = '';
              i += 3; // Skip 'AND'
              continue;
            }
          } else {
            // Part of a word like "ISLAND" or "LAND"
            current += char;
          }
        } else {
          current += char;
        }
      } else {
        // Inside string
        if (char === stringChar && (i === 0 || clause[i - 1] !== '\\')) {
          inString = false;
          stringChar = null;
        }
        current += char;
      }
    }

    if (current.trim()) {
      predicates.push(current.trim());
    }

    return predicates;
  }

  getPredicatesForSubset(subset) {
    const subsetSet = new Set(subset);
    const applicable = {
      selections: [],
      joins: [],
      complex: []
    };

    // Add selection predicates
    for (let table of subset) {
      if (this.selectionPredicates[table]) {
        applicable.selections.push(...this.selectionPredicates[table]);
      }
    }

    // Add join predicates (both tables in subset)
    for (let jp of this.joinPredicates) {
      if (Array.from(jp.tables).every(t => subsetSet.has(t))) {
        applicable.joins.push(jp.predicate);
      }
    }

    // Add complex predicates (all tables in subset)
    for (let cp of this.complexPredicates) {
      if (Array.from(cp.tables).every(t => subsetSet.has(t))) {
        applicable.complex.push(cp.predicate);
      }
    }

    return applicable;
  }

  getJoinPredicatesBetween(left, right) {
    const leftSet = new Set(left);
    const rightSet = new Set(right);
    const connecting = [];

    for (let jp of this.joinPredicates) {
      const tables = Array.from(jp.tables);
      if (tables.length === 2) {
        const hasLeft = tables.some(t => leftSet.has(t));
        const hasRight = tables.some(t => rightSet.has(t));
        if (hasLeft && hasRight) {
          connecting.push(jp);
        }
      }
    }

    return connecting;
  }
}

// SQL Parser - supports both modern JOIN and old-style comma syntax
// Add transitive joins for tables with same column = same constant
// ONLY when both predicates constrain to the SAME SINGLE VALUE
// Valid: t1.col = 'x' AND t2.col = 'x' => t1.col = t2.col
// Valid: t1.col IN ('x') AND t2.col IN ('x') => t1.col = t2.col
// Valid: t1.col = 'x' AND t2.col IN ('x') => t1.col = t2.col
// Invalid: t1.col IN ('a', 'b') AND t2.col IN ('a', 'b') => NO (could be different)
function addConstantEqualityJoins(joinGraph, classifier, tables) {
  console.log('\n🔍 Detecting constant-equality joins...');

  // Map: "column:value" -> [{table, column}, ...]
  // Only includes predicates that constrain to a SINGLE value
  const constantGroups = new Map();

  // Scan all tables for selection predicates
  for (let table of tables) {
    const preds = classifier.getPredicatesForSubset([table]);

    for (let pred of preds.selections) {
      const result = extractSingleConstantValue(pred);
      if (result) {
        const { table: tbl, column: col, value: val } = result;
        const key = `${col}:${val}`;

        if (!constantGroups.has(key)) {
          constantGroups.set(key, []);
        }
        constantGroups.get(key).push({ table: tbl, column: col });
      }
    }
  }

  // Add joins for groups with 2+ tables
  let addedCount = 0;
  for (let [key, group] of constantGroups) {
    if (group.length >= 2) {
      console.log(`  Found ${group.length} tables with ${key}:`);
      console.log(`    Tables: ${group.map(g => g.table).join(', ')}`);

      // Add join between each pair
      for (let i = 0; i < group.length; i++) {
        for (let j = i + 1; j < group.length; j++) {
          joinGraph.addJoin(
            group[i].table,
            group[j].table,
            group[i].column,
            group[j].column,
            false  // isOriginal=false (transitive via constant)
          );
          addedCount++;
        }
      }
    }
  }

  if (addedCount > 0) {
    console.log(`  Added ${addedCount} constant-equality join(s)`);
  } else {
    console.log(`  No constant-equality joins found`);
  }
}

// Extract single constant value from a predicate
// Returns {table, column, value} if predicate constrains to ONE value, null otherwise
function extractSingleConstantValue(pred) {
  // Pattern 1: table.column = constant
  // IMPORTANT: (?:\s|$) accepts EITHER space OR end-of-string
  // This is correct - do not change to (?:\s+$) which would require space before end!
  const eqMatch = pred.match(/(\w+)\.(\w+)\s*=\s*(.+?)(?:\s|$)/);
  if (eqMatch) {
    const [_, table, column, rawValue] = eqMatch;
    const value = normalizeValue(rawValue);
    return { table, column, value };
  }

  // Pattern 2: table.column IN (...)
  const inMatch = pred.match(/(\w+)\.(\w+)\s+IN\s*\(([^)]+)\)/i);
  if (inMatch) {
    const [_, table, column, valueList] = inMatch;

    // Extract individual values
    const values = valueList
      .split(',')
      .map(v => normalizeValue(v.trim()));

    // Only valid if exactly ONE value in the IN list
    if (values.length === 1) {
      return { table, column, value: values[0] };
    }
  }

  return null;
}

// Normalize a constant value: remove quotes, type casts, whitespace
function normalizeValue(rawValue) {
  return rawValue
    .replace(/['"]/g, '')      // Remove quotes
    .replace(/::.+$/, '')       // Remove type casts like ::timestamp
    .trim();
}

// Workload Analyzer - tracks table and column usage across queries
class WorkloadAnalyzer {
  constructor() {
    this.tables = new Map(); // base_table -> { query_count, columns: Map }
    this.query_count = 0;
  }

  analyzeQuery(tables, aliases, classifier, joinGraph) {
    this.query_count++;

    // Get base tables (resolve aliases)
    const baseTables = new Set();
    tables.forEach(alias => {
      const baseTable = aliases.get(alias) || alias;
      baseTables.add(baseTable);
    });

    // Track each base table (ignore self-joins - count once per query)
    baseTables.forEach(baseTable => {
      if (!this.tables.has(baseTable)) {
        this.tables.set(baseTable, {
          query_count: 0,
          columns: new Map()
        });
      }

      const tableData = this.tables.get(baseTable);
      tableData.query_count++;
    });

    // Analyze columns from predicates
    this.analyzeColumns(tables, aliases, classifier, joinGraph);
  }

  analyzeColumns(tables, aliases, classifier, joinGraph) {
    // Get all table.column references from all predicates
    const tableColumnRefs = this.extractAllTableColumnRefs(tables, classifier);

    // Classify each table.column usage
    tableColumnRefs.forEach(({ alias, column, predicate, predicateType }) => {
      const baseTable = aliases.get(alias) || alias;

      if (!this.tables.has(baseTable)) return;

      const tableData = this.tables.get(baseTable);

      if (!tableData.columns.has(column)) {
        tableData.columns.set(column, {
          query_count: 0,
          join_count: 0,
          equality_count: 0,
          range_count: 0,
          like_count: 0,
          range_min: 'NA',
          range_max: 'NA',
          queries_seen: new Set()
        });
      }

      const columnData = tableData.columns.get(column);

      // Track which queries use this column (for query_count)
      if (!columnData.queries_seen.has(this.query_count)) {
        columnData.queries_seen.add(this.query_count);
        columnData.query_count++;
      }

      // Classify usage
      if (predicateType === 'join') {
        columnData.join_count++;
      } else if (predicateType === 'selection') {
        // Further classify selection predicates
        if (this.isRangePredicate(predicate)) {
          columnData.range_count++;

          // Extract and update range bounds
          const bounds = this.extractRangeBounds(predicate, column);
          this.updateRangeBounds(columnData, bounds.min, bounds.max);
        } else if (this.isLikePredicate(predicate)) {
          columnData.like_count++;
        } else {
          // Equality predicates (=, !=, IN, etc.)
          columnData.equality_count++;
        }
      }
    });
  }

  extractAllTableColumnRefs(tables, classifier) {
    const refs = [];

    // Get predicates for full query
    const allPreds = classifier.getPredicatesForSubset(tables);

    // Extract from join predicates
    allPreds.joins.forEach(pred => {
      const columns = this.extractColumnsFromExpression(pred);
      columns.forEach(({ alias, column }) => {
        refs.push({ alias, column, predicate: pred, predicateType: 'join' });
      });
    });

    // Extract from selection predicates
    allPreds.selections.forEach(pred => {
      const columns = this.extractColumnsFromExpression(pred);
      columns.forEach(({ alias, column }) => {
        refs.push({ alias, column, predicate: pred, predicateType: 'selection' });
      });
    });

    // Extract from complex predicates
    allPreds.complex.forEach(pred => {
      const columns = this.extractColumnsFromExpression(pred);
      columns.forEach(({ alias, column }) => {
        refs.push({ alias, column, predicate: pred, predicateType: 'complex' });
      });
    });

    return refs;
  }

  extractColumnsFromExpression(expression) {
    // Extract all table.column references using regex
    // Handles: t.col, t1.col2, table_name.column_name
    const pattern = /\b(\w+)\.(\w+)\b/g;
    const columns = [];
    let match;

    while ((match = pattern.exec(expression)) !== null) {
      columns.push({
        alias: match[1],
        column: match[2]
      });
    }

    return columns;
  }

  isRangePredicate(predicate) {
    // Check for range operators: <, >, <=, >=, BETWEEN
    return /[<>]=?/.test(predicate) || /\bBETWEEN\b/i.test(predicate);
  }

  isLikePredicate(predicate) {
    // Check for LIKE operators: LIKE, NOT LIKE, ILIKE, NOT ILIKE
    return /\b(NOT\s+)?(I)?LIKE\b/i.test(predicate);
  }

  extractRangeBounds(predicate, column) {
    const bounds = { min: 'NA', max: 'NA' };

    // Escape column name for regex
    const colPattern = column.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

    // Allow optional table prefix (e.g., n.column or just column)
    const optionalPrefix = '(?:\\w+\\.)?';

    // Pattern 1: col > value or col >= value
    const greaterPattern = new RegExp(`${optionalPrefix}${colPattern}\\s*(>=?)\\s*([^\\s,)]+)`, 'i');
    const greaterMatch = predicate.match(greaterPattern);
    if (greaterMatch) {
      bounds.min = this.normalizeValue(greaterMatch[2]);
    }

    // Pattern 2: col < value or col <= value
    const lessPattern = new RegExp(`${optionalPrefix}${colPattern}\\s*(<=?)\\s*([^\\s,)]+)`, 'i');
    const lessMatch = predicate.match(lessPattern);
    if (lessMatch) {
      bounds.max = this.normalizeValue(lessMatch[2]);
    }

    // Pattern 3: col BETWEEN x AND y
    const betweenPattern = new RegExp(
      `${optionalPrefix}${colPattern}\\s+BETWEEN\\s+([^\\s]+)\\s+AND\\s+([^\\s,)]+)`,
      'i'
    );
    const betweenMatch = predicate.match(betweenPattern);
    if (betweenMatch) {
      bounds.min = this.normalizeValue(betweenMatch[1]);
      bounds.max = this.normalizeValue(betweenMatch[2]);
    }

    return bounds;
  }

  normalizeValue(val) {
    // Remove quotes and type casts
    return val.trim()
      .replace(/^['"]|['"]$/g, '')
      .replace(/::\w+$/g, '')
      .trim();
  }

  updateRangeBounds(columnData, newMin, newMax) {
    // Update min (take the smaller value)
    if (newMin !== 'NA') {
      if (columnData.range_min === 'NA') {
        columnData.range_min = newMin;
      } else {
        // String comparison (works for numbers, dates, strings)
        columnData.range_min = newMin < columnData.range_min ? newMin : columnData.range_min;
      }
    }

    // Update max (take the larger value)
    if (newMax !== 'NA') {
      if (columnData.range_max === 'NA') {
        columnData.range_max = newMax;
      } else {
        // String comparison (works for numbers, dates, strings)
        columnData.range_max = newMax > columnData.range_max ? newMax : columnData.range_max;
      }
    }
  }

  getSummary() {
    // Convert Maps to plain objects for easier rendering
    const tablesObj = {};

    this.tables.forEach((tableData, tableName) => {
      const columnsObj = {};

      tableData.columns.forEach((columnData, columnName) => {
        columnsObj[columnName] = {
          query_count: columnData.query_count,
          join_count: columnData.join_count,
          equality_count: columnData.equality_count,
          range_count: columnData.range_count,
          like_count: columnData.like_count,
          range_min: columnData.range_min,
          range_max: columnData.range_max
        };
      });

      tablesObj[tableName] = {
        query_count: tableData.query_count,
        columns: columnsObj
      };
    });

    return {
      summary: {
        total_queries: this.query_count,
        distinct_tables: this.tables.size,
        distinct_columns: Array.from(this.tables.values())
          .reduce((sum, t) => sum + t.columns.size, 0)
      },
      tables: tablesObj
    };
  }
}

function parseSQL(sql) {
  sql = sql.replace(/\s+/g, ' ').trim();

  const tables = new Set();
  const aliases = new Map();
  const joinGraph = new JoinGraph();
  const classifier = new PredicateClassifier(sql);

  // Detect which JOIN style is used
  const hasExplicitJoin = /\sJOIN\s/i.test(sql);

  if (hasExplicitJoin) {
    // Modern JOIN syntax: FROM table1 JOIN table2 ON ...
    parseModernJoinSyntax(sql, tables, aliases, joinGraph, classifier);
  } else {
    // Old-style comma syntax: FROM table1, table2, table3 WHERE table1.x = table2.y
    parseOldStyleJoinSyntax(sql, tables, aliases, joinGraph, classifier);
  }

  console.log(`\n📊 Query Analysis:`);
  console.log(`  Tables: ${tables.size}`);
  console.log(`  Explicit joins: ${joinGraph.edges.size}`);
  console.log(`  Join details tracked: ${joinGraph.joinDetails.size}`);

  // Show all joins
  if (joinGraph.joinDetails.size > 0) {
    console.log(`\n  Explicit join predicates:`);
    for (let [edge, details] of joinGraph.joinDetails.entries()) {
      for (let d of details) {
        console.log(`    ${d.t1}.${d.t1col} = ${d.t2}.${d.t2col}`);
      }
    }
  }

  // Classify predicates (works for both styles)
  classifier.classifyPredicates();

  // Add constant-equality joins (e.g., t1.col='X' AND t2.col='X' => t1.col=t2.col)
  // These are transitive joins through constant equality
  addConstantEqualityJoins(joinGraph, classifier, tables);

  // Compute transitive closure with CORRECT column checking
  // Now only adds transitive joins when columns match:
  // A.x=B.y AND B.y=C.z => A.x=C.z ✓
  // A.x=B.y AND B.z=C.w => NO transitive join ✗ (different columns on B)
  const transitiveJoins = joinGraph.computeTransitiveClosure();
  if (transitiveJoins > 0) {
    console.log(`🔗 Transitive joins enabled: ${transitiveJoins} implicit join(s) added`);
  }

  console.log(`  Total joins (explicit + transitive): ${joinGraph.edges.size}`);

  // Build Equivalence Classes (PostgreSQL-style)
  joinGraph.buildEquivalenceClasses();

  return {
    tables: Array.from(tables),
    joinGraph: joinGraph,
    aliases: aliases,
    classifier: classifier
  };
}

function parseModernJoinSyntax(sql, tables, aliases, joinGraph, classifier) {
  // Parse initial FROM table
  const fromMatch = sql.match(/FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?/i);
  if (fromMatch) {
    const tableName = fromMatch[1];
    const alias = fromMatch[2] || tableName;
    tables.add(alias);
    aliases.set(alias, tableName);
    classifier.tableAliases.set(alias, tableName);
    joinGraph.setTableAlias(alias, tableName);
  }

  // Parse JOIN clauses
  const joinRegex = /JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/gi;
  let joinMatch;

  while ((joinMatch = joinRegex.exec(sql)) !== null) {
    const tableName = joinMatch[1];
    const alias = joinMatch[2] || tableName;
    const leftTable = joinMatch[3];
    const leftCol = joinMatch[4];
    const rightTable = joinMatch[5];
    const rightCol = joinMatch[6];

    tables.add(alias);
    aliases.set(alias, tableName);
    classifier.tableAliases.set(alias, tableName);
    joinGraph.setTableAlias(alias, tableName);
    joinGraph.addJoin(leftTable, rightTable, leftCol, rightCol);
  }

  // Also check WHERE clause for additional join predicates
  const whereRegex = /WHERE.*?(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/g;
  let whereMatch;
  let whereJoinCount = 0;

  while ((whereMatch = whereRegex.exec(sql)) !== null) {
    const leftTable = whereMatch[1];
    const leftCol = whereMatch[2];
    const rightTable = whereMatch[3];
    const rightCol = whereMatch[4];

    if (tables.has(leftTable) && tables.has(rightTable)) {
      joinGraph.addJoin(leftTable, rightTable, leftCol, rightCol);
      whereJoinCount++;
    }
  }

  if (whereJoinCount > 0) {
    console.log(`  Found ${whereJoinCount} additional join(s) in WHERE clause`);
  }
}

function parseOldStyleJoinSyntax(sql, tables, aliases, joinGraph, classifier) {
  // Extract FROM clause (everything between FROM and WHERE/GROUP/ORDER/LIMIT)
  const fromMatch = sql.match(/FROM\s+(.*?)\s+(?:WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|$)/is);
  if (!fromMatch) return;

  const fromClause = fromMatch[1].trim();

  // Split by comma to get individual table declarations
  // Need to handle: "table1 AS alias1, table2 AS alias2, table3 alias3"
  const tableParts = splitByComma(fromClause);

  for (let part of tableParts) {
    part = part.trim();
    if (!part) continue;

    // Match: table_name [AS] alias
    // Handles: "title AS t", "title t", "title"
    const match = part.match(/^(\w+)(?:\s+(?:AS\s+)?(\w+))?$/i);
    if (match) {
      const tableName = match[1];
      const alias = match[2] || tableName;
      tables.add(alias);
      aliases.set(alias, tableName);
      classifier.tableAliases.set(alias, tableName);
      joinGraph.setTableAlias(alias, tableName);
    }
  }

  // Extract all join predicates from WHERE clause
  const whereMatch = sql.match(/WHERE\s+(.*?)(?:GROUP\s+BY|ORDER\s+BY|LIMIT|$)/is);
  if (whereMatch) {
    const whereClause = whereMatch[1];

    // Find all equality predicates between tables
    const joinRegex = /(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/g;
    let match;
    let joinCount = 0;

    while ((match = joinRegex.exec(whereClause)) !== null) {
      const leftTable = match[1];
      const leftCol = match[2];
      const rightTable = match[3];
      const rightCol = match[4];

      // Only add as join if both tables exist and are different
      if (tables.has(leftTable) && tables.has(rightTable) && leftTable !== rightTable) {
        joinGraph.addJoin(leftTable, rightTable, leftCol, rightCol);
        joinCount++;
      }
    }

    if (joinCount > 0) {
      console.log(`  Found ${joinCount} join predicate(s) in WHERE clause`);
    }
  }
}

function splitByComma(text) {
  // Split by comma, but respect parentheses and quotes
  const parts = [];
  let current = '';
  let parenDepth = 0;
  let inString = false;
  let stringChar = null;

  for (let i = 0; i < text.length; i++) {
    const char = text[i];

    if (!inString) {
      if (char === "'" || char === '"') {
        inString = true;
        stringChar = char;
      } else if (char === '(') {
        parenDepth++;
      } else if (char === ')') {
        parenDepth--;
      } else if (char === ',' && parenDepth === 0) {
        parts.push(current.trim());
        current = '';
        continue;
      }
    } else if (char === stringChar && (i === 0 || text[i - 1] !== '\\')) {
      inString = false;
      stringChar = null;
    }

    current += char;
  }

  if (current.trim()) {
    parts.push(current.trim());
  }

  return parts;
}

// Subquery Generator
class SubqueryGenerator {
  constructor(aliases, classifier, joinGraph) {
    this.aliases = aliases;
    this.classifier = classifier;
    this.joinGraph = joinGraph;
  }

  generateSubquery(subset, left = null, right = null) {
    const subsetArray = Array.from(subset).sort();

    if (subsetArray.length === 1) {
      return this.generateBaseTableQuery(subsetArray[0]);
    } else {
      return this.generateJoinQuery(subset, left, right);
    }
  }

  // Helper: Render table with alias if needed
  renderTable(alias) {
    const tableName = this.aliases.get(alias) || alias;
    return alias !== tableName ? `${tableName} ${alias}` : tableName;
  }

  generateBaseTableQuery(table) {
    const predicates = this.classifier.getPredicatesForSubset([table]);

    let sql = `SELECT * FROM ${this.renderTable(table)}`;

    const allPredicates = [...predicates.selections, ...predicates.complex];
    if (allPredicates.length > 0) {
      sql += '\nWHERE ' + allPredicates.join('\n  AND ');
    }

    return sql + ';';
  }

  // Helper: Find best next table to add to JOIN tree (prioritizes original edges)
  findNextTableForJoinTree(addedTables, remainingTables) {
    let bestTable = null;
    let bestJoinPred = null;

    // Try to find a table that has an original join to the current tree
    for (let table of remainingTables) {
      const joinPreds = this.findJoinPredicates(Array.from(addedTables), [table]);

      // Look for original join first
      const originalJoin = joinPreds.find(p => p.isOriginal);
      if (originalJoin) {
        return { table, joinPred: originalJoin }; // Found original, use immediately
      }

      // Track first available join (transitive) as fallback
      if (!bestJoinPred && joinPreds.length > 0) {
        bestTable = table;
        bestJoinPred = joinPreds[0];
      }
    }

    return bestTable ? { table: bestTable, joinPred: bestJoinPred } : null;
  }

  // Helper: Build WHERE clause with selection predicates and unused join predicates
  buildWhereClause(subsetArray, usedJoinPredicates) {
    // Find ALL join predicates between tables in this subset
    const allJoinPredicates = [];
    for (let i = 0; i < subsetArray.length; i++) {
      for (let j = i + 1; j < subsetArray.length; j++) {
        const preds = this.findJoinPredicates([subsetArray[i]], [subsetArray[j]]);
        for (let pred of preds) {
          allJoinPredicates.push(pred.predicate);
        }
      }
    }

    // Collect all predicates: selections + unused joins
    const predicates = this.classifier.getPredicatesForSubset(subsetArray);
    const allPredicates = [...predicates.selections, ...predicates.complex];

    // Add unused join predicates (additional constraints)
    for (let joinPred of allJoinPredicates) {
      if (!usedJoinPredicates.has(joinPred)) {
        allPredicates.push(joinPred);
      }
    }

    return allPredicates.length > 0
      ? '\nWHERE ' + allPredicates.join('\n  AND ')
      : '';
  }

  generateJoinQuery(subset, left, right) {
    const subsetArray = Array.from(subset).sort();

    // Build FROM clause with JOINs
    // Strategy: Add tables by following original edges (not alphabetical order)
    const firstTable = subsetArray[0];
    let sql = `SELECT * FROM ${this.renderTable(firstTable)}`;

    // Track used predicates and table membership
    const usedJoinPredicates = new Set();
    const addedTables = new Set([firstTable]);
    const remainingTables = new Set(subsetArray.slice(1));

    // Build JOIN tree by following original edges when possible
    while (remainingTables.size > 0) {
      const next = this.findNextTableForJoinTree(addedTables, remainingTables);

      if (!next) break; // No more joinable tables

      sql += `\nJOIN ${this.renderTable(next.table)}`;

      if (next.joinPred) {
        sql += ' ON ' + next.joinPred.predicate;
        usedJoinPredicates.add(next.joinPred.predicate);
      }

      addedTables.add(next.table);
      remainingTables.delete(next.table);
    }

    // Add WHERE clause with selections and unused join predicates
    sql += this.buildWhereClause(subsetArray, usedJoinPredicates);

    return sql + ';';
  }

  findJoinPredicates(leftTables, rightTables) {
    // Generate join predicates from joinGraph.joinDetails
    // Prioritize original joins over transitive joins
    const predicates = [];

    for (let leftTable of leftTables) {
      for (let rightTable of rightTables) {
        const edge = [leftTable, rightTable].sort().join('|||');
        const details = this.joinGraph.joinDetails.get(edge);

        if (details) {
          for (let detail of details) {
            // Generate predicate string from column information
            const predicate = `${detail.t1}.${detail.t1col} = ${detail.t2}.${detail.t2col}`;
            predicates.push({
              predicate,
              isOriginal: detail.isOriginal
            });
          }
        }
      }
    }

    // Sort: original joins first, then transitive joins
    predicates.sort((a, b) => {
      if (a.isOriginal === b.isOriginal) return 0;
      return a.isOriginal ? -1 : 1;
    });

    return predicates;
  }
}

// Format subset for display
function formatSubset(subset) {
  return '{' + Array.from(subset).sort().join(', ') + '}';
}

// Example queries
const examples = {
  job_light_69: `SELECT COUNT(*)
FROM title t,
     movie_info mi,
     movie_companies mc,
     cast_info ci,
     movie_keyword mk
WHERE t.id=mi.movie_id
  AND t.id=mc.movie_id
  AND t.id=ci.movie_id
  AND t.id=mk.movie_id
  AND ci.role_id=2
  AND mi.info_type_id=16
  AND t.production_year>2000
  AND t.production_year<2005
  AND mk.keyword_id=7084;`,

  stats_ceb_67: `SELECT COUNT(*) 
FROM comments as c,
     posts as p,
     postLinks as pl,
     postHistory as ph,
     votes as v,
     users as u 
WHERE p.Id = pl.PostId 
  AND p.Id = ph.PostId 
  AND p.Id = c.PostId 
  AND u.Id = c.UserId 
  AND u.Id = v.UserId 
  AND c.Score=0 
  AND c.CreationDate>='2010-08-02 20:27:48'::timestamp 
  AND c.CreationDate<='2014-09-10 16:09:23'::timestamp 
  AND p.PostTypeId=1 
  AND p.Score=4 
  AND p.ViewCount<=4937 
  AND pl.CreationDate>='2011-11-03 05:09:35'::timestamp 
  AND ph.PostHistoryTypeId=1 
  AND u.Reputation<=270 
  AND u.Views>=0 
  AND u.Views<=51 
  AND u.DownVotes>=0;`,

  job_33a: `SELECT MIN(cn1.name) AS first_company,
       MIN(cn2.name) AS second_company,
       MIN(mi_idx1.info) AS first_rating,
       MIN(mi_idx2.info) AS second_rating,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM company_name AS cn1,
     company_name AS cn2,
     info_type AS it1,
     info_type AS it2,
     kind_type AS kt1,
     kind_type AS kt2,
     link_type AS lt,
     movie_companies AS mc1,
     movie_companies AS mc2,
     movie_info_idx AS mi_idx1,
     movie_info_idx AS mi_idx2,
     movie_link AS ml,
     title AS t1,
     title AS t2
WHERE cn1.country_code = '[us]'
  AND it1.info = 'rating'
  AND it2.info = 'rating'
  AND kt1.kind IN ('tv series')
  AND kt2.kind IN ('tv series')
  AND lt.link IN ('sequel',
                  'follows',
                  'followed by')
  AND mi_idx2.info < '3.0'
  AND t2.production_year BETWEEN 2005 AND 2008
  AND lt.id = ml.link_type_id
  AND t1.id = ml.movie_id
  AND t2.id = ml.linked_movie_id
  AND it1.id = mi_idx1.info_type_id
  AND t1.id = mi_idx1.movie_id
  AND kt1.id = t1.kind_id
  AND cn1.id = mc1.company_id
  AND t1.id = mc1.movie_id
  AND ml.movie_id = mi_idx1.movie_id
  AND ml.movie_id = mc1.movie_id
  AND mi_idx1.movie_id = mc1.movie_id
  AND it2.id = mi_idx2.info_type_id
  AND t2.id = mi_idx2.movie_id
  AND kt2.id = t2.kind_id
  AND cn2.id = mc2.company_id
  AND t2.id = mc2.movie_id
  AND ml.linked_movie_id = mi_idx2.movie_id
  AND ml.linked_movie_id = mc2.movie_id
  AND mi_idx2.movie_id = mc2.movie_id;`,
};

// Main App Component
export default function JoinEnumeratorApp() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState(null);
  const [error, setError] = useState(null);
  const [selectedPlan, setSelectedPlan] = useState(null);
  const [batchMode, setBatchMode] = useState(false);
  const [batchResults, setBatchResults] = useState(null);
  const [processingBatch, setProcessingBatch] = useState(false);
  const [workloadAnalysis, setWorkloadAnalysis] = useState(null);
  const [showExamples, setShowExamples] = useState(false); // Hidden by default

  const enumerateJoins = () => {
    if (!query.trim()) {
      setError('Please enter a SQL query.');
      setResults(null);
      return;
    }

    try {
      // Strip trailing semicolon before processing
      const cleanedQuery = query.trim().replace(/;+\s*$/, '');
      const { tables, joinGraph, aliases, classifier } = parseSQL(cleanedQuery);

      if (tables.length === 0) {
        setError('No tables found in query. Please check your SQL syntax.');
        setResults(null);
        return;
      }

      // Check for multi-table OR predicates
      const multiTableOrs = classifier.multiTableOrPredicates;
      let expansionInfo = null;

      if (multiTableOrs.length > 0) {
        console.log(`Found ${multiTableOrs.length} multi-table OR predicate(s)`);

        // Calculate potential branches
        let totalBranches = 1;
        const branchCounts = [];

        for (let mtOr of multiTableOrs) {
          const terms = classifier.extractOrTerms(mtOr.predicate);
          branchCounts.push(terms.length);
          totalBranches *= terms.length;
        }

        expansionInfo = {
          count: multiTableOrs.length,
          predicates: multiTableOrs.map(m => m.predicate),
          branchCounts: branchCounts,
          totalBranches: totalBranches,
          expanded: false,
          branches: []
        };

        console.log(`Would expand to ${totalBranches} UNION branches`);

        if (totalBranches > 200) {
          console.warn(`Branch limit exceeded: ${totalBranches} > 200. Skipping expansion.`);
          expansionInfo.error = `Branch limit exceeded: ${totalBranches} branches (limit: 200)`;
        } else if (totalBranches > 1) {
          console.log('Expanding multi-table OR to UNION branches...');
          expansionInfo.expanded = true;

          // Generate UNION branches
          const branches = expandMultiTableOr(multiTableOrs, query, tables, joinGraph, aliases, classifier);
          expansionInfo.branches = branches;

          console.log(`Generated ${branches.length} UNION branches`);
        }
      }

      // Enumerate joins for the main query (or first branch if expanded)
      const enumerator = new PostgreSQLJoinEnumerator(tables, joinGraph);
      const allPlans = enumerator.enumerateAllValid();
      const counts = enumerator.countByLevel();

      console.log(`\n📈 Enumeration Summary:`);
      console.log(`  Total subsets: ${allPlans.length}`);
      console.log(`  Join subsets: ${allPlans.length - tables.length}`);
      console.log(`  By level:`, counts);

      // Sanity check: for n tables with complete graph, max is 2^n - 1
      const maxPossible = Math.pow(2, tables.length) - 1;
      console.log(`  Theoretical max (complete graph): ${maxPossible}`);
      if (allPlans.length > maxPossible) {
        console.warn(`  ⚠️  WARNING: More subsets than possible! Possible duplicate bug.`);
      }

      // Detect syntax style
      const syntaxStyle = /\sJOIN\s/i.test(query) ? 'Modern (JOIN ... ON)' : 'Old-Style (Comma-separated)';

      // Generate SQL for each plan
      const sqlGenerator = new SubqueryGenerator(aliases, classifier, joinGraph);
      const plansWithSQL = allPlans.map(plan => ({
        ...plan,
        sql: sqlGenerator.generateSubquery(plan.subset, plan.left, plan.right),
        predicates: classifier.getPredicatesForSubset(Array.from(plan.subset))
      }));

      setResults({
        tables,
        joinGraph,
        allPlans: plansWithSQL,
        counts,
        classifier,
        syntaxStyle,
        expansionInfo
      });
      setError(null);
      setSelectedPlan(null);
    } catch (err) {
      setError(`Error parsing query: ${err.message}`);
      setResults(null);
      console.error(err);
    }
  };

  // Expand multi-table OR predicates to UNION branches
  function expandMultiTableOr(multiTableOrs, originalQuery, tables, joinGraph, aliases, classifier) {
    // Generate all combinations (Cartesian product of OR terms)
    const allTerms = multiTableOrs.map(mtOr => classifier.extractOrTerms(mtOr.predicate));

    // Generate Cartesian product
    function cartesianProduct(arrays) {
      if (arrays.length === 0) return [[]];
      if (arrays.length === 1) return arrays[0].map(x => [x]);

      const [first, ...rest] = arrays;
      const restProduct = cartesianProduct(rest);

      const result = [];
      for (let item of first) {
        for (let combo of restProduct) {
          result.push([item, ...combo]);
        }
      }
      return result;
    }

    const combinations = cartesianProduct(allTerms);

    // Create a branch for each combination
    const branches = combinations.map((combo, idx) => {
      // Build description of this branch
      const description = combo.map((term, i) => {
        const tables = Array.from(multiTableOrs[i].tables).join(', ');
        return `OR#${i + 1}[${tables}]: ${term.substring(0, 50)}${term.length > 50 ? '...' : ''}`;
      }).join(' + ');

      return {
        branchId: idx + 1,
        predicates: combo,
        description: description,
        originalOrPredicates: multiTableOrs.map(m => m.predicate)
      };
    });

    return branches;
  }

  // Handle batch query file upload
  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    setProcessingBatch(true);
    setBatchResults(null);
    setWorkloadAnalysis(null);
    setError(null);

    try {
      const text = await file.text();
      const queries = text.split('\n').filter(q => q.trim().length > 0);

      console.log(`Processing ${queries.length} queries...`);

      const allResults = [];
      let successCount = 0;
      let errorCount = 0;
      const analyzer = new WorkloadAnalyzer();

      for (let i = 0; i < queries.length; i++) {
        const queryText = queries[i].trim().replace(/;+\s*$/, '');
        const queryId = i + 1; // Line number (1-indexed)
        console.log(`Processing query ${queryId}/${queries.length}`);

        try {
          const { tables, joinGraph, aliases, classifier } = parseSQL(queryText);

          if (tables.length === 0) {
            console.warn(`Query ${queryId}: No tables found`);
            errorCount++;
            continue;
          }

          // Analyze workload
          analyzer.analyzeQuery(tables, aliases, classifier, joinGraph);

          const enumerator = new PostgreSQLJoinEnumerator(tables, joinGraph);
          const allPlans = enumerator.enumerateAllValid();

          const sqlGenerator = new SubqueryGenerator(aliases, classifier, joinGraph);

          for (let plan of allPlans) {
            const subsetStr = Array.from(plan.subset).sort().join(',');
            const sql = sqlGenerator.generateSubquery(plan.subset, plan.left, plan.right);

            allResults.push({
              queryId: queryId,
              subset: subsetStr,
              sql: sql.replace(/\n/g, ' ').replace(/\s+/g, ' ').trim()
            });
          }

          successCount++;
        } catch (err) {
          console.error(`Error processing query ${queryId}:`, err);
          errorCount++;
        }
      }

      console.log(`Batch processing complete: ${successCount} success, ${errorCount} errors`);

      setBatchResults({
        queries: queries.length,
        successCount,
        errorCount,
        results: allResults
      });

      // Set workload analysis
      setWorkloadAnalysis(analyzer.getSummary());

    } catch (err) {
      setError(`Error reading file: ${err.message}`);
    } finally {
      setProcessingBatch(false);
    }
  };

  // Download CSV
  const downloadCSV = () => {
    if (!batchResults || !batchResults.results) return;

    // Create CSV content with query_id as first column
    const header = 'query_id,subset,query\n';
    const rows = batchResults.results.map(row => {
      // Escape quotes in SQL
      const escapedSQL = row.sql.replace(/"/g, '""');
      return `${row.queryId},"${row.subset}","${escapedSQL}"`;
    }).join('\n');

    const csv = header + rows;

    // Create download
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'join_subsets.csv';
    link.click();
    URL.revokeObjectURL(url);
  };

  // Download CSV with joins only (exclude single-table subsets)
  const downloadJoinsOnlyCSV = () => {
    if (!batchResults || !batchResults.results) return;

    // Filter out single-table subsets (those without commas in subset)
    const joinsOnly = batchResults.results.filter(row => row.subset.includes(','));

    // Create CSV content
    const header = 'query_id,subset,query\n';
    const rows = joinsOnly.map(row => {
      // Escape quotes in SQL
      const escapedSQL = row.sql.replace(/"/g, '""');
      return `${row.queryId},"${row.subset}","${escapedSQL}"`;
    }).join('\n');

    const csv = header + rows;

    // Create download
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'join_subsets_joins_only.csv';
    link.click();
    URL.revokeObjectURL(url);
  };

  const loadExample = (type) => {
    setQuery(examples[type]);
    setResults(null);
    setError(null);
  };

  const clearAll = () => {
    setQuery('');
    setResults(null);
    setError(null);
    setSelectedPlan(null);
    setBatchResults(null);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-600 to-purple-900 p-4">
      <div className="max-w-7xl mx-auto bg-white rounded-xl shadow-2xl overflow-hidden">
        {/* Header */}
        <div className="bg-gradient-to-r from-purple-600 to-purple-800 text-white p-8 text-center">
          <h1 className="text-4xl font-bold mb-2">PostgreSQL Join Enumerator</h1>
          <p className="text-lg opacity-90">Enumerate inner-join subsets</p>
        </div>

        {/* Content */}
        <div className="grid md:grid-cols-2 gap-6 p-8">
          {/* Input Panel */}
          <div className="space-y-4">
            <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
              {/* Mode Toggle */}
              <div className="flex gap-2 mb-4 border-b pb-4">
                <button
                  onClick={() => { setBatchMode(false); setBatchResults(null); }}
                  className={`flex-1 py-2 px-4 rounded-lg font-semibold transition-colors ${!batchMode ? 'bg-purple-600 text-white' : 'bg-gray-200 text-gray-700 hover:bg-gray-300'}`}
                >
                  Single Query
                </button>
                <button
                  onClick={() => { setBatchMode(true); setResults(null); }}
                  className={`flex-1 py-2 px-4 rounded-lg font-semibold transition-colors ${batchMode ? 'bg-purple-600 text-white' : 'bg-gray-200 text-gray-700 hover:bg-gray-300'}`}
                >
                  Batch Mode
                </button>
              </div>

              {!batchMode ? (
                <>
                  <h2 className="text-2xl font-bold text-gray-800 mb-4 pb-2 border-b-2 border-purple-600">
                    📝 Input Query
                  </h2>

                  <textarea
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Paste your SQL query here...&#10;&#10;Example:&#10;SELECT * FROM title t&#10;JOIN cast_info ci ON t.id = ci.movie_id&#10;JOIN movie_info mi ON t.id = mi.movie_id&#10;WHERE t.production_year > 2000"
                    className="w-full h-64 p-3 border-2 border-gray-300 rounded-lg font-mono text-sm resize-none focus:outline-none focus:border-purple-600 transition-colors"
                  />

                  <div className="flex gap-3 mt-4">
                    <button
                      onClick={enumerateJoins}
                      className="flex-1 bg-gradient-to-r from-purple-600 to-purple-800 text-white px-6 py-3 rounded-lg font-semibold hover:shadow-lg transform hover:-translate-y-0.5 transition-all"
                    >
                      Enumerate Joins
                    </button>
                    <button
                      onClick={clearAll}
                      className="bg-gray-600 text-white px-6 py-3 rounded-lg font-semibold hover:bg-gray-700 transition-colors"
                    >
                      Clear
                    </button>
                  </div>
                </>
              ) : (
                <>
                  <h2 className="text-2xl font-bold text-gray-800 mb-4 pb-2 border-b-2 border-purple-600">
                    📁 Batch Processing
                  </h2>

                  <div className="bg-blue-50 border-l-4 border-blue-500 p-4 rounded mb-4 text-sm">
                    <div className="font-semibold text-blue-900 mb-2">File Format:</div>
                    <ul className="text-blue-800 space-y-1 text-xs">
                      <li>• One SQL query per line</li>
                      <li>• Text file (.txt or .sql)</li>
                      <li>• Empty lines ignored</li>
                      <li>• Output: CSV with query_id, subset, and query columns</li>
                      <li>• query_id = line number from input file (1-indexed)</li>
                    </ul>
                  </div>

                  <div className="border-2 border-dashed border-purple-400 rounded-lg p-8 text-center hover:border-purple-600 transition-colors bg-purple-50">
                    <input
                      type="file"
                      id="fileInput"
                      accept=".txt,.sql"
                      onChange={handleFileUpload}
                      className="hidden"
                    />
                    <label
                      htmlFor="fileInput"
                      className="cursor-pointer block"
                    >
                      <div className="text-4xl mb-2">📤</div>
                      <div className="text-lg font-semibold text-purple-800 mb-1">
                        {processingBatch ? 'Processing...' : 'Upload Query File'}
                      </div>
                      <div className="text-sm text-purple-600">
                        Click to select a file with queries (one per line)
                      </div>
                    </label>
                  </div>

                  {processingBatch && (
                    <div className="mt-4 text-center">
                      <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-purple-600"></div>
                      <div className="text-sm text-gray-600 mt-2">Processing queries...</div>
                    </div>
                  )}

                  {batchResults && (
                    <div className="mt-4 space-y-3">
                      <div className="bg-green-50 border-l-4 border-green-500 p-4 rounded">
                        <div className="font-semibold text-green-900 mb-2">Processing Complete!</div>
                        <div className="text-sm text-green-800">
                          <div>Total queries: {batchResults.queries}</div>
                          <div>✓ Success: {batchResults.successCount}</div>
                          {batchResults.errorCount > 0 && (
                            <div className="text-red-700">✗ Errors: {batchResults.errorCount}</div>
                          )}
                          <div className="mt-2 pt-2 border-t border-green-300">
                            <div className="font-semibold">Generated subsets:</div>
                            <div className="ml-2">
                              <div>• All subsets: {batchResults.results.length}</div>
                              <div>• Join subsets (2+ tables): {batchResults.results.filter(r => r.subset.includes(',')).length}</div>
                              <div>• Single tables: {batchResults.results.filter(r => !r.subset.includes(',')).length}</div>
                            </div>
                          </div>
                        </div>
                      </div>

                      <div className="grid grid-cols-2 gap-3">
                        <button
                          onClick={downloadCSV}
                          className="bg-gradient-to-r from-green-600 to-green-700 text-white px-4 py-3 rounded-lg font-semibold hover:shadow-lg transform hover:-translate-y-0.5 transition-all text-sm"
                        >
                          📥 Download All Subsets
                        </button>
                        <button
                          onClick={downloadJoinsOnlyCSV}
                          className="bg-gradient-to-r from-blue-600 to-blue-700 text-white px-4 py-3 rounded-lg font-semibold hover:shadow-lg transform hover:-translate-y-0.5 transition-all text-sm"
                        >
                          🔗 Download Joins Only
                        </button>
                      </div>

                      <div className="bg-blue-50 border-l-4 border-blue-500 p-3 rounded text-xs">
                        <div className="font-semibold text-blue-900 mb-1">💡 Which download to use?</div>
                        <div className="text-blue-800 space-y-1">
                          <div><strong>All Subsets:</strong> Includes single tables + joins (for base table cardinality)</div>
                          <div><strong>Joins Only:</strong> Only multi-table joins (for join cardinality estimation)</div>
                        </div>
                      </div>

                      {/* Workload Analysis Section */}
                      {workloadAnalysis && (
                        <div className="mt-4 border-2 border-indigo-300 rounded-lg overflow-hidden">
                          <div className="bg-indigo-600 text-white px-4 py-3 font-bold text-sm">
                            📊 Workload Analysis
                          </div>

                          {/* Summary Statistics */}
                          <div className="bg-indigo-50 p-4 grid grid-cols-3 gap-3 text-center border-b-2 border-indigo-200">
                            <div>
                              <div className="text-2xl font-bold text-indigo-900">
                                {workloadAnalysis.summary.total_queries}
                              </div>
                              <div className="text-xs text-indigo-700">Total Queries</div>
                            </div>
                            <div>
                              <div className="text-2xl font-bold text-indigo-900">
                                {workloadAnalysis.summary.distinct_tables}
                              </div>
                              <div className="text-xs text-indigo-700">Distinct Tables</div>
                            </div>
                            <div>
                              <div className="text-2xl font-bold text-indigo-900">
                                {workloadAnalysis.summary.distinct_columns}
                              </div>
                              <div className="text-xs text-indigo-700">Distinct Columns</div>
                            </div>
                          </div>

                          {/* Tables Section */}
                          <div className="p-4 space-y-3 max-h-[800px] overflow-y-auto">
                            {Object.entries(workloadAnalysis.tables)
                              .sort((a, b) => b[1].query_count - a[1].query_count)
                              .map(([tableName, tableData]) => (
                                <details key={tableName} className="border-2 border-indigo-200 rounded-lg">
                                  <summary className="cursor-pointer bg-indigo-100 px-3 py-2 font-semibold text-indigo-900 hover:bg-indigo-200 transition-colors text-sm">
                                    <span className="inline-block w-48 truncate align-middle">{tableName}</span>
                                    <span className="text-xs text-indigo-700 ml-2">
                                      (in {tableData.query_count} queries, {Object.keys(tableData.columns).length} cols)
                                    </span>
                                  </summary>

                                  <div className="p-3 bg-white">
                                    <div className="overflow-x-auto">
                                      <table className="w-full text-xs border-collapse">
                                        <thead>
                                          <tr className="bg-indigo-50">
                                            <th className="px-2 py-1 text-left border border-indigo-200 font-semibold">Column</th>
                                            <th className="px-2 py-1 text-center border border-indigo-200 font-semibold">Queries</th>
                                            <th className="px-2 py-1 text-center border border-indigo-200 font-semibold">Joins</th>
                                            <th className="px-2 py-1 text-center border border-indigo-200 font-semibold">Equality</th>
                                            <th className="px-2 py-1 text-center border border-indigo-200 font-semibold">Ranges</th>
                                            <th className="px-2 py-1 text-center border border-indigo-200 font-semibold">LIKE</th>
                                          </tr>
                                        </thead>
                                        <tbody>
                                          {Object.entries(tableData.columns)
                                            .sort((a, b) => b[1].query_count - a[1].query_count)
                                            .map(([columnName, columnData]) => (
                                              <tr key={columnName} className="hover:bg-indigo-50">
                                                <td className="px-2 py-1 border border-indigo-200 font-mono">{columnName}</td>
                                                <td className="px-2 py-1 text-center border border-indigo-200">{columnData.query_count}</td>
                                                <td className={`px-2 py-1 text-center border border-indigo-200 ${columnData.join_count > 0 ? 'bg-green-100 font-semibold' : ''}`}>
                                                  {columnData.join_count > 0 ? columnData.join_count : '-'}
                                                </td>
                                                <td className={`px-2 py-1 text-center border border-indigo-200 ${columnData.equality_count > 0 ? 'bg-blue-100 font-semibold' : ''}`}>
                                                  {columnData.equality_count > 0 ? columnData.equality_count : '-'}
                                                </td>
                                                <td className={`px-2 py-1 text-center border border-indigo-200 font-mono text-xs ${columnData.range_count > 0 ? 'bg-purple-100 font-semibold' : ''}`}>
                                                  {columnData.range_count > 0 ? `${columnData.range_count} [${columnData.range_min}–${columnData.range_max}]` : '-'}
                                                </td>
                                                <td className={`px-2 py-1 text-center border border-indigo-200 ${columnData.like_count > 0 ? 'bg-yellow-100 font-semibold' : ''}`}>
                                                  {columnData.like_count > 0 ? columnData.like_count : '-'}
                                                </td>
                                              </tr>
                                            ))}
                                        </tbody>
                                      </table>
                                    </div>

                                    <div className="mt-2 pt-2 border-t border-indigo-200 text-xs text-indigo-700">
                                      <div className="flex gap-4">
                                        <div><span className="inline-block w-3 h-3 bg-green-100 border border-green-300 mr-1"></span>Join key</div>
                                        <div><span className="inline-block w-3 h-3 bg-blue-100 border border-blue-300 mr-1"></span>Equality (=, !=, IN)</div>
                                        <div><span className="inline-block w-3 h-3 bg-purple-100 border border-purple-300 mr-1"></span>Range (&lt;, &gt;, BETWEEN)</div>
                                        <div><span className="inline-block w-3 h-3 bg-yellow-100 border border-yellow-300 mr-1"></span>LIKE pattern</div>
                                      </div>
                                    </div>
                                  </div>
                                </details>
                              ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </>
              )}

              {/* Examples */}
              <div className="mt-6">
                <div className="flex justify-between items-center mb-3">
                  <h3 className="font-semibold text-gray-700">Example Queries:</h3>
                  <button
                    onClick={() => setShowExamples(!showExamples)}
                    className="text-xs px-2 py-1 bg-gray-200 hover:bg-gray-300 rounded transition-colors font-medium"
                  >
                    {showExamples ? '🙈 Hide' : '👁️ Show'}
                  </button>
                </div>

                {showExamples && (
                  <div className="space-y-2">
                    {[
                      { key: 'job_light_69', label: 'JOB-light 69' },
                      { key: 'stats_ceb_67', label: 'Stats-CEB 67' },
                      { key: 'job_33a', label: 'JOB 33a' }
                    ].map(ex => (
                      <button
                        key={ex.key}
                        onClick={() => loadExample(ex.key)}
                        className="w-full text-left px-4 py-2 border-2 border-teal-600 text-teal-600 rounded-lg hover:bg-teal-600 hover:text-white transition-colors font-medium text-sm"
                      >
                        {ex.label}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Results Panel */}
          <div className="bg-gray-50 rounded-lg p-6 border border-gray-200">
            <h2 className="text-2xl font-bold text-gray-800 mb-4 pb-2 border-b-2 border-purple-600">
              📊 Results
            </h2>

            <div className="bg-white rounded-lg p-4 max-h-[600px] overflow-y-auto">
              {error && (
                <div className="bg-red-100 border-l-4 border-red-500 text-red-700 p-4 rounded">
                  {error}
                </div>
              )}

              {!batchMode && !results && !error && (
                <div className="bg-blue-100 border-l-4 border-blue-500 text-blue-700 p-4 rounded">
                  Enter a SQL query and click "Enumerate Joins" to see all valid join combinations with their SQL subqueries.
                </div>
              )}

              {batchMode && !batchResults && !processingBatch && !error && (
                <div className="bg-blue-100 border-l-4 border-blue-500 text-blue-700 p-4 rounded">
                  <div className="font-semibold mb-2">Batch Mode Active</div>
                  <div className="text-sm">Upload a file with SQL queries (one per line) to generate a CSV with all join subsets.</div>
                  <div className="text-xs mt-2 space-y-1">
                    <div>• Each unique subset will be listed once</div>
                    <div>• Output format: query_id, subset, query</div>
                    <div>• query_id = line number from your input file</div>
                    <div>• Perfect for batch cardinality estimation</div>
                  </div>
                </div>
              )}

              {batchMode && batchResults && (
                <div className="space-y-3">
                  <div className="text-sm text-gray-700">
                    <div className="font-semibold mb-2">Preview (first 10 rows):</div>
                    <div className="bg-gray-900 text-green-400 p-3 rounded font-mono text-xs overflow-x-auto">
                      <div className="border-b border-gray-700 pb-1 mb-2 font-semibold">query_id,subset,query</div>
                      {batchResults.results.slice(0, 10).map((row, idx) => (
                        <div key={idx} className={`py-0.5 truncate ${!row.subset.includes(',') ? 'text-gray-500' : ''}`}>
                          {row.queryId},"{row.subset}","{row.sql.substring(0, 70)}..."
                          {!row.subset.includes(',') && <span className="text-yellow-500 ml-2">← single table</span>}
                        </div>
                      ))}
                      {batchResults.results.length > 10 && (
                        <div className="text-yellow-400 mt-2">
                          ... and {batchResults.results.length - 10} more rows
                        </div>
                      )}
                    </div>
                  </div>

                  <div className="bg-purple-50 border-l-4 border-purple-500 p-3 rounded text-sm">
                    <div className="font-semibold text-purple-900">CSV Ready for Download</div>
                    <div className="text-purple-800 text-xs mt-1">
                      Two download options available: All subsets or Joins only (2+ tables).
                    </div>
                  </div>
                </div>
              )}

              {results && (
                <div className="space-y-4">
                  {/* Statistics */}
                  <div className="grid grid-cols-3 gap-3">
                    <div className="bg-white border-2 border-gray-200 rounded-lg p-3 text-center">
                      <div className="text-3xl font-bold text-purple-600">{results.tables.length}</div>
                      <div className="text-xs text-gray-600 mt-1">Tables</div>
                    </div>
                    <div className="bg-white border-2 border-gray-200 rounded-lg p-3 text-center">
                      <div className="text-3xl font-bold text-purple-600">{results.allPlans.length}</div>
                      <div className="text-xs text-gray-600 mt-1">Total Plans</div>
                    </div>
                    <div className="bg-white border-2 border-gray-200 rounded-lg p-3 text-center">
                      <div className="text-3xl font-bold text-purple-600">{results.joinGraph.edges.size}</div>
                      <div className="text-xs text-gray-600 mt-1">Join Edges</div>
                    </div>
                  </div>

                  {/* Syntax Detection Badge */}
                  <div className={`${results.syntaxStyle.includes('Old-Style') ? 'bg-green-100 border-green-500' : 'bg-purple-100 border-purple-500'} border-l-4 p-3 rounded text-sm`}>
                    <span className="font-semibold">Detected Syntax:</span> {results.syntaxStyle}
                  </div>

                  {/* OR Expansion Info */}
                  {results.expansionInfo && (
                    <div className={`${results.expansionInfo.expanded ? 'bg-blue-100 border-blue-500' : results.expansionInfo.error ? 'bg-red-100 border-red-500' : 'bg-yellow-100 border-yellow-500'} border-l-4 p-3 rounded`}>
                      <div className="font-semibold text-sm mb-2">
                        🔄 Multi-Table OR Detected: {results.expansionInfo.count} predicate(s)
                      </div>

                      {results.expansionInfo.error ? (
                        <div className="text-sm text-red-700">
                          ⚠️ {results.expansionInfo.error}
                          <div className="text-xs mt-1">Query not expanded. Keeping original multi-table OR predicates.</div>
                        </div>
                      ) : results.expansionInfo.expanded ? (
                        <div className="text-sm">
                          <div className="text-blue-800">
                            ✅ Expanded to {results.expansionInfo.totalBranches} UNION branches
                          </div>
                          <div className="text-xs mt-2 space-y-1">
                            {results.expansionInfo.branches.map((branch, idx) => (
                              <div key={idx} className="font-mono text-xs bg-white bg-opacity-50 p-1 rounded">
                                Branch {branch.branchId}: {branch.description}
                              </div>
                            ))}
                          </div>
                          <div className="text-xs mt-2 text-blue-700">
                            💡 Each branch can be estimated separately with conjunctive (AND-only) predicates
                          </div>
                        </div>
                      ) : (
                        <div className="text-sm text-yellow-800">
                          ℹ️ Would expand to {results.expansionInfo.totalBranches} branches
                          <div className="text-xs mt-1">Multi-table OR predicates detected but not expanded (total branches = 1)</div>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Enumeration by level */}
                  {Object.keys(results.counts).sort((a, b) => a - b).map(level => {
                    const levelPlans = results.allPlans.filter(p => p.level === parseInt(level));
                    return (
                      <div key={level} className="space-y-2">
                        <div className="bg-purple-600 text-white px-3 py-2 rounded font-semibold">
                          Level {level}: Subsets of size {level}
                        </div>
                        {levelPlans.map((plan, idx) => {
                          const planIndex = results.allPlans.indexOf(plan);
                          const subsetStr = formatSubset(plan.subset);
                          const isSelected = selectedPlan === planIndex;

                          if (plan.left === null) {
                            return (
                              <div key={idx} className="space-y-2">
                                <div
                                  onClick={() => setSelectedPlan(isSelected ? null : planIndex)}
                                  className={`${isSelected ? 'bg-green-100 border-green-600' : 'bg-gray-50 border-green-500'} border-l-4 px-3 py-2 rounded font-mono text-sm hover:bg-green-50 transition-colors cursor-pointer`}
                                >
                                  <div className="font-semibold">{planIndex}. {subsetStr}</div>
                                </div>
                                {isSelected && (
                                  <div className="ml-4 bg-gray-900 text-green-400 p-3 rounded font-mono text-xs overflow-x-auto">
                                    <pre className="whitespace-pre-wrap">{plan.sql}</pre>
                                    {(plan.predicates.selections.length > 0 || plan.predicates.complex.length > 0) && (
                                      <div className="mt-2 pt-2 border-t border-gray-700 text-gray-300 text-xs">
                                        <div className="font-semibold text-yellow-400">Predicates Applied:</div>
                                        {plan.predicates.selections.length > 0 && (
                                          <div>• Selections: {plan.predicates.selections.length}</div>
                                        )}
                                        {plan.predicates.complex.length > 0 && (
                                          <div>• Complex: {plan.predicates.complex.length}</div>
                                        )}
                                      </div>
                                    )}
                                  </div>
                                )}
                              </div>
                            );
                          } else {
                            const leftStr = formatSubset(plan.left);
                            const rightStr = formatSubset(plan.right);
                            return (
                              <div key={idx} className="space-y-2">
                                <div
                                  onClick={() => setSelectedPlan(isSelected ? null : planIndex)}
                                  className={`${isSelected ? 'bg-purple-100 border-purple-600' : 'bg-gray-50 border-purple-500'} border-l-4 px-3 py-2 rounded font-mono text-sm hover:bg-purple-50 transition-colors cursor-pointer`}
                                >
                                  <div className="font-semibold">{planIndex}. {subsetStr} = {leftStr} ⋈ {rightStr}</div>
                                </div>
                                {isSelected && (
                                  <div className="ml-4 bg-gray-900 text-green-400 p-3 rounded font-mono text-xs overflow-x-auto">
                                    <pre className="whitespace-pre-wrap">{plan.sql}</pre>
                                    <div className="mt-2 pt-2 border-t border-gray-700 text-gray-300 text-xs">
                                      <div className="font-semibold text-yellow-400">Predicates Applied:</div>
                                      {plan.predicates.selections.length > 0 && (
                                        <div>• Selections: {plan.predicates.selections.length}</div>
                                      )}
                                      {plan.predicates.joins.length > 0 && (
                                        <div>• Joins: {plan.predicates.joins.length}</div>
                                      )}
                                      {plan.predicates.complex.length > 0 && (
                                        <div>• Complex: {plan.predicates.complex.length}</div>
                                      )}
                                    </div>
                                  </div>
                                )}
                              </div>
                            );
                          }
                        })}
                      </div>
                    );
                  })}

                  {/* Summary */}
                  <div className="bg-blue-50 border-l-4 border-blue-500 p-4 rounded">
                    <div className="font-semibold text-blue-900 mb-2">Summary:</div>
                    {Object.keys(results.counts).sort((a, b) => a - b).map(level => (
                      <div key={level} className="text-sm text-blue-800">
                        Level {level}: {results.counts[level]} unique subset(s)
                      </div>
                    ))}
                    <div className="text-sm font-semibold text-blue-900 mt-2">
                      Total: {results.allPlans.length} unique subsets
                    </div>
                    <div className="text-xs text-blue-700 mt-2 italic">
                      💡 Each subset shown once with one valid join decomposition. Click any subset to view its SQL subquery.
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
      <div className="text-white text-center">
        <p className="text-xs"><a href="https://github.com/Btsan/Join_Subset_Enumerator">Source on GitHub🔗</a></p>
      </div>
    </div>
  );
}