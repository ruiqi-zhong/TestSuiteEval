import numpy as np
from typing import Set, List
from sqlparse import tokens
from sqlparse.tokens import Whitespace, Wildcard, Punctuation, Name, Keyword
from sql_util.parse import tokenize, join_tokens, Token
from sql_util.dbinfo import extract_table_column_properties_path
from sql_util.run import can_execute_path
from sql_util.writedb import init_empty_db_from_orig_
import os
import re


AGG_OP = {'count', 'min', 'max', 'avg', 'sum'}
CMP_OP = {'>', '<', '>=', '<=', '=', '!='}
LIKE_OP = {'LIKE', 'NOT LIKE'}
IN_OP = {'IN', 'NOT IN'}
LOGICAL_OP = {'AND', 'OR'}
NUM_OP = {'/', '+', '-', '*'}
ORDER = {'DESC', 'ASC'}
empty_types = {}
NUM_ALTERNATIVES = 10
epsilon = 1e-1


def count_table_occurences(query_toks: List[str], table_names: List[str]) -> int:
    counter = 0
    for t in query_toks:
        if t.lower() in table_names:
            counter += 1
    return counter


def _other_toks_same_family(tok: Token, family: Set[str]) \
        -> List[Token]:
    t, v = tok.ttype, tok.value
    result = []
    if v.lower() in family or v.upper() in family:
        for s in family:
            if v.lower() != s and v.upper() != s:
                result.append(Token(t, s))
    return result


def _get_int_replacement(tok: Token) -> List[Token]:
    result = []
    if tok.ttype == tokens.Token.Literal.Number.Integer:
        v = int(tok.value)
        random_ints = np.random.randint(-np.abs(v) - 1, np.abs(v) + 1, NUM_ALTERNATIVES)
        for r in random_ints:
            if r != v:
                result.append(r)
        result.append(v + 1)
        result.append(v - 1)
    return [Token(tok.ttype, str(r)) for r in set(result)]


def _get_float_replacement(tok: Token) -> List[Token]:
    result = []
    if tok.ttype == tokens.Token.Literal.Number.Float:
        v = float(tok.value)
        random_vals = [np.random.random() * 2 * v - v for _ in range(NUM_ALTERNATIVES)]
        for r in random_vals:
            if r != v:
                result.append(r)
        result.append(v + epsilon)
        result.append(v - epsilon)
    return [Token(tok.ttype, str(r)) for r in set(result)]


def _get_string_replacement(tok: Token) -> List[Token]:
    result = []
    if tok.ttype == tokens.Token.Literal.String.Symbol or tok.ttype == tokens.Token.Literal.String.Single:
        v = tok.value
        result.append(v[0] + v[-1])
        start, end = 1, len(v) - 1
        for span_start in range(start, end):
            for span_end in range(span_start + 1, end):
                v_new = v[0] + v[span_start:span_end] + v[-1]
                result.append(v_new)
                v_new = v[:span_start] + v[span_end:]
                result.append(v_new)

        v = v.replace('%', '')
        for add_percent_last in ('%', ''):
            for add_percent_first in ('%', ''):
                result.append(v[0] + add_percent_first + v[1:-1] + add_percent_last + v[-1])
        result = [Token(tok.ttype, v_new) for v_new in set(result) if v_new != v]
    return result


def get_possible_replacement(tok: Token, column_names: Set[str]) \
        -> List[Token]:
    possible_replacement = []
    for family in [AGG_OP, CMP_OP, ORDER, NUM_OP, LOGICAL_OP, LIKE_OP, IN_OP, column_names]:
        # [VAR_NAMES, AGG_OP, CMP_OP, ORDER, NUM_OP, LOGICAL_OP, LIKE_OP, IN_OP, table_names, column_names]:
        # in many cases using table name is semantically equivalenet
        # so we do not replace those
        possible_replacement += _other_toks_same_family(tok, family)

    for get_family_function in [_get_int_replacement, _get_float_replacement, _get_string_replacement]:
        possible_replacement += get_family_function(tok)

    return possible_replacement


# does not drop
# 1. all blank spans
# 2. wild card spans
# 3. table name spans
def span_droppable(span: List[Token]) -> bool:
    # filter out white space
    span = [tok for tok in span if tok.ttype != Whitespace]
    if len(span) == 0:
        return False
    if len(span) == 1:
        if span[0].ttype == Keyword.Order and span[0].value.lower() == 'asc':
            return False
        if span[0].ttype == Wildcard:
            return False
        if span[0].ttype == Keyword and span[0].value.lower() == 'as':
            return False
        if span[0].ttype == Punctuation and span[0].value == ';':
            return False
    if len(span) == 2:
        if span[0].ttype == Name and span[1].ttype == Punctuation and span[1].value == '.':
            return False
        if span[0].value.lower() == 'as' and span[1].value.lower() == 'result':
            return False
    return True


def drop_any_span(toks: List[Token]) -> Set[str]:
    num_toks = len(toks)
    all_s = set()
    for span_start in range(num_toks):
        for span_end in range(span_start + 1, num_toks + 1):
            removed_tok = toks[span_start:span_end]
            if span_droppable(removed_tok):
                toks_left = toks[:span_start] + toks[span_end:]
                q = join_tokens(toks_left)
                all_s.add(q)
    return all_s


def rm_count_in_column(s):
    return re.sub('COUNT\s?\((.*?)\)', 'COUNT()', s, flags=re.IGNORECASE)


def equivalent_count(s1, s2):
    return rm_count_in_column(s1.strip()) == rm_count_in_column(s2.strip())


def generate_neighbor_queries_path(sqlite_path: str, query: str) -> List[str]:
    table2column2properties, _ = extract_table_column_properties_path(sqlite_path)
    column_names = set([column_name for table_name in table2column2properties
                        for column_name in table2column2properties[table_name]])
    sql_toks = tokenize(query)
    spans_dropped = drop_any_span(sql_toks)

    tok_replaced = set()
    for idx, tok in enumerate(sql_toks):
        if tok.ttype != tokens.Whitespace:
            replacement_toks = get_possible_replacement(tok, column_names)
            for corrupted_tok in replacement_toks:
                perturbed_q = join_tokens(sql_toks[:idx] + [corrupted_tok] + sql_toks[idx + 1:])
                tok_replaced.add(perturbed_q)

    empty_path = init_empty_db_from_orig_(sqlite_path)
    all_neighbor_queries = spans_dropped | tok_replaced
    all_neighbor_queries = [q for q in all_neighbor_queries if can_execute_path(empty_path, q)]
    results = []
    os.unlink(empty_path)
    for neighbor_query in all_neighbor_queries:
        if neighbor_query == query or equivalent_count(neighbor_query, query):
            continue
        results.append(neighbor_query)
    return results
