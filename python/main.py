"""
Main entry point for PostgreSQL Join Enumerator

Command-line interface for enumerating join subsets and generating CSV output.
"""

import argparse
import csv
import sys
from typing import List, Dict

try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm not installed
    def tqdm(iterable, **kwargs):
        return iterable

from parser import parse_sql
from enumerator import PostgreSQLJoinEnumerator
from sql_generator import SubqueryGenerator
from utils import read_queries_from_file, format_subset


def main():
    """
    Main entry point for CLI
    
    Usage:
        python -m join_enumerator input.sql --output results.csv
    """
    parser = argparse.ArgumentParser(
        description='PostgreSQL Join Enumerator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage (one query per line)
  python main.py queries.sql

  # Semicolon-separated queries
  python main.py benchmark.sql --semicolon-separated

  # Stop on first error
  python main.py queries.sql --stop-on-error --verbose
        """
    )
    
    parser.add_argument('input_file', help='Input SQL file')
    parser.add_argument('--output', '-o', default='output.csv', 
                       help='Output CSV file (default: output.csv)')
    parser.add_argument('--semicolon-separated', action='store_true',
                       help='Input file has semicolon-separated queries (default: one per line)')
    parser.add_argument('--stop-on-error', action='store_true',
                       help='Stop processing on first error (default: continue)')
    parser.add_argument('--dialect', default='postgres', 
                       help='SQL dialect (default: postgres)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Verbose output')
    parser.add_argument('--max-level', type=int, default=20,
                       help='Maximum enumeration level (default: 20)')
    
    args = parser.parse_args()
    
    # Read queries
    try:
        queries = read_queries_from_file(args.input_file, args.semicolon_separated)
    except FileNotFoundError:
        print(f"ERROR: File not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to read input file: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not queries:
        print(f"WARNING: No queries found in {args.input_file}", file=sys.stderr)
        sys.exit(0)
    
    # Process queries with progress bar
    results = []
    errors = []
    
    try:
        with open(args.output, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['query_id', 'subset', 'query'])
            writer.writeheader()
            
            progress_bar = tqdm(queries, desc="Processing queries")
            
            for query_id, (line_num, query_text) in enumerate(progress_bar, 1):
                try:
                    rows = process_query(query_text, query_id, args)
                    
                    for row in rows:
                        writer.writerow(row)
                    
                    results.append((query_id, len(rows)))
                    
                    if args.verbose:
                        progress_bar.write(f"Query {query_id}: {len(rows)} subsets")
                
                except Exception as e:
                    error_msg = f"ERROR at line {line_num}: {str(e)}"
                    progress_bar.write(error_msg)
                    
                    if args.verbose:
                        progress_bar.write(f"  Query: {query_text[:100]}...")
                    
                    errors.append((line_num, str(e)))
                    
                    if args.stop_on_error:
                        progress_bar.write("\nStopping due to error (--stop-on-error enabled)")
                        sys.exit(1)
    
    except Exception as e:
        print(f"\nERROR: Failed to write output file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Print summary
    print(f"\nCompleted: {len(results)}/{len(queries)} queries")
    if errors:
        print(f"Errors: {len(errors)} queries failed")
    print(f"Output written to: {args.output}")


def process_query(sql: str, query_id: int, args) -> List[Dict]:
    """
    Process a single SQL query and return CSV rows
    
    Args:
        sql: SQL query string
        query_id: Query identifier
        args: Command-line arguments
    
    Returns:
        List of dicts with keys: query_id, subset, query
    """
    # Parse SQL
    parsed = parse_sql(sql, dialect=args.dialect)
    
    if not parsed.tables:
        raise ValueError("No tables found in query")
    
    # Enumerate subsets (max 20 levels)
    enumerator = PostgreSQLJoinEnumerator(parsed.join_graph)
    enum_result = enumerator.enumerate_subsets(
        parsed.tables, 
        max_level=min(args.max_level, 20)
    )
    
    if args.verbose:
        print(f"Query {query_id}: Enumerated {len(enum_result.all_plans)} subsets")
    
    # Generate SQL for each subset
    generator = SubqueryGenerator(parsed.aliases, parsed.classifier, parsed.join_graph)
    
    rows = []
    for plan in enum_result.all_plans:
        sql_query = generator.generate_subquery(plan.subset, plan.left, plan.right)
        if len(plan.subset) > 1:    
            rows.append({
                'query_id': query_id,
                'subset': format_subset(plan.subset),
                'query': str(sql_query).replace('\n', ' ')
            })
    
    return rows


if __name__ == '__main__':
    main()
