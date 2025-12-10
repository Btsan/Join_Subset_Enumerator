import React, { useState } from 'react';

// Join Graph Class
class JoinGraph {
  constructor() {
    this.edges = new Set();
  }

  addJoin(t1, t2) {
    const edge = [t1, t2].sort().join('|||');
    this.edges.add(edge);
  }

  canJoin(left, right) {
    for (let l of left) {
      for (let r of right) {
        const edge = [l, r].sort().join('|||');
        if (this.edges.has(edge)) {
          return true;
        }
      }
    }
    return false;
  }

  isConnected(subset) {
    if (subset.size <= 1) return true;

    const subsetArray = Array.from(subset);
    const visited = new Set([subsetArray[0]]);
    const queue = [subsetArray[0]];

    while (queue.length > 0) {
      const current = queue.shift();
      for (let other of subsetArray) {
        if (!visited.has(other)) {
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

  // Compute transitive closure of the join graph
  // If A.x=B.x AND B.x=C.x, then A.x=C.x is also a valid join
  computeTransitiveClosure() {
    // Extract all unique tables from edges
    const tables = new Set();
    for (let edge of this.edges) {
      const [t1, t2] = edge.split('|||');
      tables.add(t1);
      tables.add(t2);
    }
    
    const tableList = Array.from(tables).sort();
    const n = tableList.length;
    
    if (n === 0) return 0;
    
    // Build adjacency matrix
    const connected = Array(n).fill(null).map(() => Array(n).fill(false));
    
    // Initialize with direct edges
    for (let i = 0; i < n; i++) {
      connected[i][i] = true; // Self-loops
    }
    
    for (let edge of this.edges) {
      const [t1, t2] = edge.split('|||');
      const i = tableList.indexOf(t1);
      const j = tableList.indexOf(t2);
      connected[i][j] = true;
      connected[j][i] = true;
    }
    
    // Floyd-Warshall: find transitive closure
    for (let k = 0; k < n; k++) {
      for (let i = 0; i < n; i++) {
        for (let j = 0; j < n; j++) {
          if (connected[i][k] && connected[k][j]) {
            connected[i][j] = true;
          }
        }
      }
    }
    
    // Add all transitive edges to the graph
    let addedCount = 0;
    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        if (connected[i][j]) {
          const t1 = tableList[i];
          const t2 = tableList[j];
          const edge = [t1, t2].sort().join('|||');
          
          if (!this.edges.has(edge)) {
            this.edges.add(edge);
            addedCount++;
            console.log(`  Transitive join: ${t1} ⋈ ${t2}`);
          }
        }
      }
    }
    
    if (addedCount > 0) {
      console.log(`✓ Added ${addedCount} transitive join edge(s)`);
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
    const seenSubsets = new Set(); // Track which subsets we've already added

    // Level 1: Base tables
    for (let table of this.tables) {
      const subset = new Set([table]);
      const subsetKey = this.getSubsetKey(subset);
      
      if (!seenSubsets.has(subsetKey)) {
        allSubplans.push({
          level: 1,
          subset: subset,
          left: null,
          right: null
        });
        seenSubsets.add(subsetKey);
      }
    }

    // Levels 2 through n
    for (let level = 2; level <= this.n; level++) {
      const levelPlans = this.enumerateLevel(level, seenSubsets);
      allSubplans.push(...levelPlans);
    }

    return allSubplans;
  }

  enumerateLevel(level, seenSubsets) {
    const plans = [];
    const subsets = this.combinations(this.tables, level);

    for (let subsetArray of subsets) {
      const subset = new Set(subsetArray);
      const subsetKey = this.getSubsetKey(subset);

      // Skip if we've already seen this subset
      if (seenSubsets.has(subsetKey)) {
        continue;
      }

      // Skip if subset is not connected
      if (!this.joinGraph.isConnected(subset)) {
        continue;
      }

      // Find ONE valid decomposition for this subset
      // We only need to show that this subset is joinable, not enumerate all ways
      const decomposition = this.findValidDecomposition(subset);
      
      if (decomposition) {
        plans.push({
          level: level,
          subset: subset,
          left: decomposition.left,
          right: decomposition.right
        });
        seenSubsets.add(subsetKey);
      }
    }

    return plans;
  }

  findValidDecomposition(subset) {
    // Find ONE valid way to split this subset into left and right
    // Try binary partitions until we find a valid one
    const subsetArray = Array.from(subset);
    const level = subsetArray.length;

    for (let leftSize = 1; leftSize < level; leftSize++) {
      // Only try one partition size (leftSize <= rightSize to avoid duplicates)
      const rightSize = level - leftSize;
      if (leftSize > rightSize) continue;

      for (let leftArray of this.combinations(subsetArray, leftSize)) {
        const left = new Set(leftArray);
        const right = new Set(subsetArray.filter(t => !left.has(t)));

        // Check if this is a valid decomposition
        if (this.joinGraph.isConnected(left) &&
            this.joinGraph.isConnected(right) &&
            this.joinGraph.canJoin(left, right)) {
          
          return { left, right };
        }
      }
    }

    return null; // No valid decomposition found
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
          const prevChar = i > 0 ? pred[i-1] : ' ';
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
        if (char === stringChar && (i === 0 || pred[i-1] !== '\\')) {
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
      } else if (char === stringChar && (i === 0 || pred[i-1] !== '\\')) {
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
    
    const predicates = [];
    let current = '';
    let parenDepth = 0;
    let inString = false;
    let stringChar = null;

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
        } else if (parenDepth === 0 && remaining.startsWith('AND ')) {
          // Check if this is actually " AND " (not part of a longer word)
          const prevChar = i > 0 ? clause[i-1] : ' ';
          if (/\s/.test(prevChar) || prevChar === ')') {
            // This is a top-level AND separator
            predicates.push(current.trim());
            current = '';
            i += 3; // Skip 'AND'
            continue;
          } else {
            // Part of a word like "ISLAND" or "LAND"
            current += char;
          }
        } else {
          current += char;
        }
      } else {
        // Inside string
        if (char === stringChar && (i === 0 || clause[i-1] !== '\\')) {
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

  // Classify predicates (works for both styles)
  classifier.classifyPredicates();

  // Compute transitive closure of join graph
  // This handles cases like: A.x=B.x AND B.x=C.x implies A.x=C.x
  const transitiveJoins = joinGraph.computeTransitiveClosure();
  if (transitiveJoins > 0) {
    console.log(`🔗 Transitive joins enabled: ${transitiveJoins} implicit join(s) added`);
  }

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
  }

  // Parse JOIN clauses
  const joinRegex = /JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/gi;
  let joinMatch;
  
  while ((joinMatch = joinRegex.exec(sql)) !== null) {
    const tableName = joinMatch[1];
    const alias = joinMatch[2] || tableName;
    const leftTable = joinMatch[3];
    const rightTable = joinMatch[5];

    tables.add(alias);
    aliases.set(alias, tableName);
    classifier.tableAliases.set(alias, tableName);
    joinGraph.addJoin(leftTable, rightTable);
  }

  // Also check WHERE clause for additional join predicates
  const whereRegex = /WHERE.*?(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/g;
  let whereMatch;
  
  while ((whereMatch = whereRegex.exec(sql)) !== null) {
    const leftTable = whereMatch[1];
    const rightTable = whereMatch[3];
    if (tables.has(leftTable) && tables.has(rightTable)) {
      joinGraph.addJoin(leftTable, rightTable);
    }
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
    }
  }

  // Extract all join predicates from WHERE clause
  // These are equality conditions between two different tables
  const whereMatch = sql.match(/WHERE\s+(.*?)(?:GROUP\s+BY|ORDER\s+BY|LIMIT|$)/is);
  if (whereMatch) {
    const whereClause = whereMatch[1];
    
    // Find all equality predicates between tables (potential joins)
    const joinRegex = /(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/g;
    let match;
    
    while ((match = joinRegex.exec(whereClause)) !== null) {
      const leftTable = match[1];
      const rightTable = match[3];
      
      // Only add as join if both tables exist and are different
      if (tables.has(leftTable) && tables.has(rightTable) && leftTable !== rightTable) {
        joinGraph.addJoin(leftTable, rightTable);
      }
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
    } else if (char === stringChar && (i === 0 || text[i-1] !== '\\')) {
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

  generateBaseTableQuery(table) {
    const tableName = this.aliases.get(table) || table;
    const predicates = this.classifier.getPredicatesForSubset([table]);
    
    let sql = `SELECT * FROM ${tableName}`;
    if (table !== tableName) {
      sql += ` ${table}`;
    }

    const allPredicates = [...predicates.selections, ...predicates.complex];
    if (allPredicates.length > 0) {
      sql += '\nWHERE ' + allPredicates.join('\n  AND ');
    }

    return sql;
  }

  generateJoinQuery(subset, left, right) {
    const subsetArray = Array.from(subset).sort();
    
    // Build FROM clause with JOINs
    let sql = 'SELECT * FROM ';
    
    // Start with first table
    const firstTable = subsetArray[0];
    const firstName = this.aliases.get(firstTable) || firstTable;
    sql += firstName;
    if (firstTable !== firstName) {
      sql += ` ${firstTable}`;
    }

    // Add JOINs for other tables
    for (let i = 1; i < subsetArray.length; i++) {
      const table = subsetArray[i];
      const tableName = this.aliases.get(table) || table;
      
      // Find join predicate connecting this table to previous tables
      const joinPreds = this.findJoinPredicates(subsetArray.slice(0, i), [table]);
      
      sql += `\nJOIN ${tableName}`;
      if (table !== tableName) {
        sql += ` ${table}`;
      }
      
      if (joinPreds.length > 0) {
        sql += ' ON ' + joinPreds[0].predicate;
      }
    }

    // Add WHERE clause with all predicates
    const predicates = this.classifier.getPredicatesForSubset(subsetArray);
    const allPredicates = [...predicates.selections, ...predicates.complex];
    
    if (allPredicates.length > 0) {
      sql += '\nWHERE ' + allPredicates.join('\n  AND ');
    }

    return sql;
  }

  findJoinPredicates(leftTables, rightTables) {
    return this.classifier.getJoinPredicatesBetween(leftTables, rightTables);
  }
}

// Format subset for display
function formatSubset(subset) {
  return '{' + Array.from(subset).sort().join(', ') + '}';
}

// Example queries
const examples = {
  // Transitive join examples
  transitive_triangle: `SELECT * FROM A, B, C
WHERE A.id = B.id
  AND B.id = C.id`,

  transitive_star: `SELECT * FROM center, spoke1, spoke2, spoke3
WHERE center.id = spoke1.cid
  AND center.id = spoke2.cid
  AND center.id = spoke3.cid`,

  star: `SELECT * FROM title t
JOIN cast_info ci ON t.id = ci.movie_id
JOIN movie_info mi ON t.id = mi.movie_id
JOIN movie_companies mc ON t.id = mc.movie_id
WHERE t.production_year > 2000`,

  chain: `SELECT * FROM title t
JOIN movie_info mi ON t.id = mi.movie_id
JOIN info_type it ON mi.info_type_id = it.id
JOIN movie_companies mc ON t.id = mc.movie_id
WHERE t.kind_id = 1`,

  cycle: `SELECT * FROM title t
JOIN cast_info ci ON t.id = ci.movie_id
JOIN name n ON ci.person_id = n.id
JOIN aka_name an ON n.id = an.person_id
WHERE t.production_year > 1990`,

  complex: `SELECT * FROM title t
JOIN movie_info mi ON t.id = mi.movie_id
JOIN info_type it ON mi.info_type_id = it.id
JOIN cast_info ci ON t.id = ci.movie_id
JOIN role_type rt ON ci.role_id = rt.id
JOIN movie_companies mc ON t.id = mc.movie_id
WHERE t.production_year BETWEEN 2000 AND 2010`,

  job1a: `SELECT MIN(mc.note) AS production_note
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx
WHERE ct.kind = 'production companies'
  AND it.info = 'top 250 rank'
  AND mc.note LIKE '%(co-production)%'
  AND ct.id = mc.company_type_id
  AND it.id = mi_idx.info_type_id
  AND mc.movie_id = mi_idx.movie_id`,

  job1a_complex: `SELECT MIN(mc.note) AS production_note
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx
WHERE ct.kind = 'production companies'
  AND it.info = 'top 250 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%')
  AND ct.id = mc.company_type_id
  AND it.id = mi_idx.info_type_id
  AND mc.movie_id = mi_idx.movie_id`,

  job2a: `SELECT MIN(t.title) AS movie_title
FROM company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code = '[de]'
  AND k.keyword = 'character-name-in-title'
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND mc.movie_id = mk.movie_id`,

  job3a: `SELECT MIN(t.title) AS movie_title
FROM keyword AS k,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE k.keyword IN ('superhero', 'marvel-comics', 'based-on-comic')
  AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark')
  AND t.production_year > 2010
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id`,

  job13a: `SELECT MIN(mi.info) AS release_date,
       MIN(t.title) AS modern_internet_movie
FROM aka_title AS at,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code = '[us]'
  AND k.keyword = 'character-name-in-title'
  AND mi.note LIKE '%internet%'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'USA:% 199%' OR mi.info LIKE 'USA:% 200%')
  AND t.production_year > 1990
  AND t.id = at.movie_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mc.movie_id = at.movie_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mk.movie_id
  AND mi.movie_id = at.movie_id
  AND mi.movie_id = mk.movie_id
  AND mk.movie_id = at.movie_id
  AND cn.id = mc.company_id
  AND k.id = mk.keyword_id`,

  job15a: `SELECT MIN(t.title) AS movie_title
FROM company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code NOT IN ('[pl]', '[us]', '[ru]')
  AND k.keyword IN ('murder', 'murder-in-title', 'blood')
  AND t.production_year > 2010
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id`,

  job_null_test: `SELECT MIN(t.title) AS movie_title
FROM title AS t,
     movie_info AS mi,
     cast_info AS ci
WHERE t.title IS NOT NULL
  AND mi.info IS NOT NULL
  AND ci.note IS NULL
  AND t.id = mi.movie_id
  AND t.id = ci.movie_id
  AND t.production_year BETWEEN 2000 AND 2010`,

  // OR Handling Test Queries
  or_simple: `SELECT * FROM movie_companies AS mc, company_type AS ct
WHERE (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%')
  AND ct.id = mc.company_type_id`,

  or_nested: `SELECT * FROM name AS n, cast_info AS ci
WHERE (n.gender = 'm' OR (n.gender = 'f' AND n.name LIKE 'B%'))
  AND n.id = ci.person_id`,

  or_multi_table: `SELECT * FROM title AS t, movie_info AS mi
WHERE (t.production_year > 2000 OR mi.info LIKE '%internet%')
  AND t.id = mi.movie_id`,

  or_multiple_groups: `SELECT * FROM movie_companies AS mc, title AS t
WHERE (mc.note LIKE '%(USA)%' OR mc.note LIKE '%(worldwide)%')
  AND (t.title LIKE '%Freddy%' OR t.title LIKE '%Jason%')
  AND mc.movie_id = t.id`,

  or_with_null: `SELECT * FROM movie_info AS mi, title AS t
WHERE (mi.info IS NULL OR mi.info LIKE '%internet%')
  AND t.id = mi.movie_id`,

  or_three_way: `SELECT * FROM title AS t, movie_info AS mi, movie_keyword AS mk
WHERE (t.title LIKE '%Freddy%' OR t.title LIKE '%Jason%' OR t.title LIKE 'Saw%')
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id`,

  job7b: `SELECT MIN(n.name) AS of_person
FROM aka_name AS an,
     cast_info AS ci,
     info_type AS it,
     name AS n,
     person_info AS pi
WHERE an.name LIKE '%a%'
  AND it.info = 'mini biography'
  AND n.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (n.gender = 'm' OR (n.gender = 'f' AND n.name LIKE 'B%'))
  AND pi.note = 'Volker Schlöndorff'
  AND n.id = an.person_id
  AND n.id = pi.person_id
  AND ci.person_id = n.id
  AND it.id = pi.info_type_id
  AND pi.person_id = an.person_id
  AND pi.person_id = ci.person_id
  AND an.person_id = ci.person_id`
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
  const [showExamples, setShowExamples] = useState(false); // Hidden by default

  const enumerateJoins = () => {
    if (!query.trim()) {
      setError('Please enter a SQL query.');
      setResults(null);
      return;
    }

    try {
      const { tables, joinGraph, aliases, classifier } = parseSQL(query);

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
        return `OR#${i+1}[${tables}]: ${term.substring(0, 50)}${term.length > 50 ? '...' : ''}`;
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
    setError(null);

    try {
      const text = await file.text();
      const queries = text.split('\n').filter(q => q.trim().length > 0);
      
      console.log(`Processing ${queries.length} queries...`);
      
      const allResults = [];
      let successCount = 0;
      let errorCount = 0;

      for (let i = 0; i < queries.length; i++) {
        const queryText = queries[i].trim();
        const queryId = i + 1; // Line number (1-indexed)
        console.log(`Processing query ${queryId}/${queries.length}`);

        try {
          const { tables, joinGraph, aliases, classifier } = parseSQL(queryText);
          
          if (tables.length === 0) {
            console.warn(`Query ${queryId}: No tables found`);
            errorCount++;
            continue;
          }

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
          <h1 className="text-4xl font-bold mb-2">🔄 PostgreSQL Join Enumerator</h1>
          <p className="text-lg opacity-90">Enumerate unique join subsets for cardinality estimation</p>
          <p className="text-sm opacity-75 mt-1">Shows each subset once with one valid decomposition</p>
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
                  <div className="text-xs text-gray-600 font-semibold mb-1">Transitive Joins:</div>
                  {[
                    { key: 'transitive_triangle', label: '🔗 Triangle (3 tables)' },
                    { key: 'transitive_star', label: '🔗 Star (4 tables)' }
                  ].map(ex => (
                    <button
                      key={ex.key}
                      onClick={() => loadExample(ex.key)}
                      className="w-full text-left px-4 py-2 border-2 border-teal-600 text-teal-600 rounded-lg hover:bg-teal-600 hover:text-white transition-colors font-medium text-sm"
                    >
                      {ex.label}
                    </button>
                  ))}

                  <div className="text-xs text-gray-600 font-semibold mb-1 mt-3">Modern JOIN Syntax:</div>
                  {[
                    { key: 'star', label: 'Star Schema (4 tables)' },
                    { key: 'chain', label: 'Chain Schema (4 tables)' },
                    { key: 'complex', label: 'Complex Query (6 tables)' }
                  ].map(ex => (
                    <button
                      key={ex.key}
                      onClick={() => loadExample(ex.key)}
                      className="w-full text-left px-4 py-2 border-2 border-purple-600 text-purple-600 rounded-lg hover:bg-purple-600 hover:text-white transition-colors font-medium text-sm"
                    >
                      {ex.label}
                    </button>
                  ))}
                  
                  <div className="text-xs text-gray-600 font-semibold mb-1 mt-3">JOB Benchmark (Basic):</div>
                  {[
                    { key: 'job1a', label: 'JOB 1a - Simple (4 tables)' },
                    { key: 'job2a', label: 'JOB 2a - Star (5 tables)' }
                  ].map(ex => (
                    <button
                      key={ex.key}
                      onClick={() => loadExample(ex.key)}
                      className="w-full text-left px-4 py-2 border-2 border-green-600 text-green-600 rounded-lg hover:bg-green-600 hover:text-white transition-colors font-medium text-sm"
                    >
                      {ex.label}
                    </button>
                  ))}

                  <div className="text-xs text-gray-600 font-semibold mb-1 mt-3">Week 2: Advanced Operators:</div>
                  {[
                    { key: 'job1a_complex', label: '🔥 NOT LIKE + OR (4 tables)' },
                    { key: 'job3a', label: '🔥 IN lists (4 tables)' },
                    { key: 'job13a', label: '🔥 IS NULL + OR (7 tables)' },
                    { key: 'job15a', label: '🔥 NOT IN (5 tables)' },
                    { key: 'job_null_test', label: '🔥 NULL testing (3 tables)' }
                  ].map(ex => (
                    <button
                      key={ex.key}
                      onClick={() => loadExample(ex.key)}
                      className="w-full text-left px-4 py-2 border-2 border-orange-600 text-orange-600 rounded-lg hover:bg-orange-600 hover:text-white transition-colors font-medium text-sm"
                    >
                      {ex.label}
                    </button>
                  ))}

                  <div className="text-xs text-gray-600 font-semibold mb-1 mt-3">OR Handling Tests:</div>
                  {[
                    { key: 'or_simple', label: '🧪 Simple OR (2 tables)' },
                    { key: 'or_nested', label: '🧪 Nested OR+AND (2 tables)' },
                    { key: 'or_multi_table', label: '⚠️ Multi-table OR (2 tables)' },
                    { key: 'or_multiple_groups', label: '🧪 Multiple OR groups (2 tables)' },
                    { key: 'or_with_null', label: '🧪 OR with IS NULL (2 tables)' },
                    { key: 'or_three_way', label: '🧪 3-way OR (3 tables)' },
                    { key: 'job7b', label: '🔥 JOB 7b - Nested (5 tables)' }
                  ].map(ex => (
                    <button
                      key={ex.key}
                      onClick={() => loadExample(ex.key)}
                      className="w-full text-left px-4 py-2 border-2 border-blue-600 text-blue-600 rounded-lg hover:bg-blue-600 hover:text-white transition-colors font-medium text-sm"
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
    </div>
  );
}