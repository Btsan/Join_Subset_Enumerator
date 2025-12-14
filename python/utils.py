"""
Utility functions for join enumeration

Includes query extraction, formatting helpers, and file I/O
"""

import re
from typing import List, Tuple, Set, Optional


def read_queries_from_file(filepath: str, semicolon_separated: bool = False) -> List[Tuple[int, str]]:
    """
    Read SQL queries from file
    
    Args:
        filepath: Path to input file
        semicolon_separated: If True, queries separated by semicolons (can be multi-line)
                            If False, one query per line
    
    Returns:
        List of (line_number, query_text) tuples
    """
    with open(filepath, 'r') as f:
        content = f.read()
    
    if semicolon_separated:
        return extract_queries_semicolon_separated(content)
    else:
        return extract_queries_line_by_line(content)


def extract_queries_line_by_line(content: str) -> List[Tuple[int, str]]:
    """
    Extract queries from file (one per line)
    Only extracts text between SELECT and semicolon/linebreak
    
    Args:
        content: File content
    
    Returns:
        List of (line_number, query_text) tuples
    """
    queries = []
    lines = content.split('\n')
    
    for line_num, line in enumerate(lines, 1):
        query = extract_select_query(line)
        if query:
            queries.append((line_num, query))
    
    return queries


def extract_queries_semicolon_separated(content: str) -> List[Tuple[int, str]]:
    """
    Extract queries from file (semicolon-separated, can be multi-line)
    Only extracts text between SELECT and semicolon
    
    Args:
        content: File content
    
    Returns:
        List of (line_number, query_text) tuples
    """
    queries = []
    
    # Find all SELECT...semicolon patterns
    pattern = r'(?i)(SELECT\s+.*?)(?:;)'
    matches = re.finditer(pattern, content, re.DOTALL)
    
    for match in matches:
        query = match.group(1).strip() + ';'
        
        # Calculate line number (count newlines before match)
        line_num = content[:match.start()].count('\n') + 1
        
        queries.append((line_num, query))
    
    return queries


def extract_select_query(text: str) -> Optional[str]:
    """
    Extract SELECT query from text
    Finds text between SELECT and semicolon/linebreak
    Ignores any text before SELECT
    
    Args:
        text: Raw text that may contain a query
    
    Returns:
        Extracted query or None if no SELECT found
    """
    # Find SELECT...semicolon or SELECT...end-of-string
    # Case-insensitive, captures SELECT to end
    pattern = r'(?i)(SELECT\s+.*?)(?:;|\Z)'
    match = re.search(pattern, text, re.DOTALL)
    
    if match:
        query = match.group(1).strip()
        # Add semicolon back if it was in original
        if match.end() < len(text) and text[match.end()-1] == ';':
            query += ';'
        elif not query.endswith(';'):
            query += ';'
        return query
    
    return None


def format_subset(subset: Set[str]) -> str:
    """
    Format subset as {t1, t2, t3}
    
    Args:
        subset: Set of table aliases
    
    Returns:
        Formatted string
    """
    return '{' + ', '.join(sorted(subset)) + '}'


def generate_canonical_key(subset: Set[str]) -> str:
    """
    Generate sorted, canonical key for subset
    
    Args:
        subset: Set of table aliases
    
    Returns:
        Canonical key: "t1|||t2|||t3"
    """
    return '|||'.join(sorted(subset))
