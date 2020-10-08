from sql_util.dbinfo import get_all_db_info_path, get_process_order, get_primary_keys
from collections import defaultdict
from typing import List, Dict, Tuple

def is_num(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def contain_is_num(column_elements):
    return {is_num(e) for e in column_elements}


def tab_col_ancestor(tab_col, dep):
    ancestor = tab_col
    while ancestor in dep:
        ancestor = dep[ancestor]
    return ancestor


def type_values_w_db(orig_path: str, typed_values: List[Tuple[Tuple[str, str], str]], loose: bool) \
        -> Dict[Tuple[str, str], List[str]]:
    t2cproperties, dep, table_col2column_elements = get_all_db_info_path(orig_path)
    new_values = defaultdict(list)
    for (lhs_table, lhs_col), value in typed_values:
        # find the anchor (table, column) that match the table and column name
        anchors = [(table, col) for table, col in table_col2column_elements.keys()
                   if (lhs_table is None or lhs_table == table) and lhs_col == col]

        # find all the ancestors of the anchors
        all_possible_ancestors = {tab_col_ancestor(tab_col, dep) for tab_col in anchors}
        value_is_num = is_num(value)

        # filter by string/numerical type,
        # if the ancestor column is ambiguous
        if len(all_possible_ancestors) > 1:
            for tab_col in set(all_possible_ancestors):
                column_elements = table_col2column_elements[tab_col]
                if len(column_elements) > 0 and value_is_num not in contain_is_num(column_elements):
                    all_possible_ancestors.remove(tab_col)

        # if string, try filtering the ancestor columns
        # by checking whether the string is contained in the column
        if len(all_possible_ancestors) > 1 and not value_is_num:
            ancestors_w_vals = {a for a in all_possible_ancestors if value in table_col2column_elements[a]}
            # if no column contains the string, do not filter
            if len(ancestors_w_vals) == 0:
                pass
            # if loose, we consider all ancestor that either contains the value, or intersects with columns that has the value
            elif loose:
                extended_ancestors = set(ancestors_w_vals)
                for a in all_possible_ancestors:
                    if a not in ancestors_w_vals and any(
                            set(table_col2column_elements[a]) & set(table_col2column_elements[w]) for w in
                            ancestors_w_vals):
                        extended_ancestors.add(a)
                all_possible_ancestors = extended_ancestors

            # if not a loose version, only consider columns that contain the value, as long as there is such ancestor column
            else:
                all_possible_ancestors = ancestors_w_vals
        for t_c in all_possible_ancestors:
            new_values[t_c].append(value)
    return new_values
