import pickle as pkl
from typing import Set, List, Tuple, Dict, Any, TypeVar
from collections import OrderedDict
from sql_util.run import exec_db_path_
from sql_util.dbpath import get_value_path
from collections import defaultdict
import random


table_name_query = "SELECT name FROM sqlite_master WHERE type='table';"
column_type_query = "pragma table_info('%s');"
foreign_key_query = "pragma foreign_key_list('%s')"
table_schema_query = "select sql from sqlite_master where type='table' and name='%s'"
select_all_query = "SELECT * from %s;"


def get_values(db_name: str) -> Set[str]:
    values = pkl.load(open(get_value_path(db_name), 'rb'))
    return values


def get_schema_path(sqlite_path: str, table_name: str) -> str:
    _, schema = exec_db_path_(sqlite_path, table_schema_query % table_name)
    schema = schema[0][0]
    return schema


def get_unique_keys(schema: str) -> Set[str]:
    schema_by_list = schema.split('\n')
    unique_keys = set()
    for r in schema_by_list:
        if 'unique' in r.lower():
            unique_keys.add(r.strip().split()[0].upper().replace("\"", '').replace('`', ''))
    return unique_keys


def get_checked_keys(schema: str) -> Set[str]:
    schema_by_list = schema.split('\n')
    checked_keys = set()
    for r in schema_by_list:
        if 'check (' in r or 'check(' in r:
            checked_keys.add(r.strip().split()[0].upper().replace("\"", '').replace('`', ''))
    return checked_keys


def get_table_names_path(sqlite_path: str) -> List[str]:
    table_names = [x[0] for x in exec_db_path_(sqlite_path, table_name_query)[1]]
    return table_names


def extract_table_column_properties_path(sqlite_path: str) \
        -> Tuple[Dict[str, Dict[str, Any]], Dict[Tuple[str, str], Tuple[str, str]]]:
    table_names = get_table_names_path(sqlite_path)
    table_name2column_properties = OrderedDict()
    child2parent = OrderedDict()
    for table_name in table_names:
        schema = get_schema_path(sqlite_path, table_name)
        unique_keys, checked_keys = get_unique_keys(schema), get_checked_keys(schema)
        table_name = table_name.lower()
        column_properties = OrderedDict()
        result_type, result = exec_db_path_(sqlite_path, column_type_query % table_name)
        for (
                columnID, column_name, columnType,
                columnNotNull, columnDefault, columnPK,
        ) in result:
            column_name = column_name.upper()
            column_properties[column_name] = {
                'ID': columnID,
                'name': column_name,
                'type': columnType,
                'notnull': columnNotNull,
                'default': columnDefault,
                'PK': columnPK,
                'unique': column_name in unique_keys,
                'checked': column_name in checked_keys
            }
        table_name2column_properties[table_name.lower()] = column_properties

        # extract foreign keys and population child2parent
        result_type, result = exec_db_path_(sqlite_path, foreign_key_query % table_name)
        for (
                keyid, column_seq_id, other_tab_name, this_column_name, other_column_name,
                on_update, on_delete, match
        ) in result:
            # these lines handle a foreign key exception in the test set
            # due to implicit reference
            if other_column_name is None:
                other_column_name = this_column_name

            table_name, other_tab_name = table_name.lower(), other_tab_name.lower()
            this_column_name, other_column_name = this_column_name.upper(), other_column_name.upper()

            # these lines handle a foreign key exception in the test set
            # due to typo in the column name
            if other_tab_name == 'author' and other_column_name == 'IDAUTHORA':
                other_column_name = 'IDAUTHOR'

            child2parent[(table_name, this_column_name)] = (other_tab_name, other_column_name)

    # make sure that every table, column in the dependency are in the table.
    dep_table_columns = set(child2parent.keys()) | set(child2parent.values())
    for table_name, column_name in dep_table_columns:
        assert table_name.lower() == table_name, "table name should be lower case"
        assert column_name.upper() == column_name, "column name should be upper case"
        assert table_name in table_name2column_properties, "table name %s missing." % table_name
        assert column_name in table_name2column_properties[table_name], \
            "column name %s should be present in table %s" % (column_name, table_name)

    return table_name2column_properties, child2parent


T = TypeVar('T')
# collapse a two level dictionary into a single level dictionary
def collapse_key(d: Dict[str, Dict[str, T]]) -> Dict[Tuple[str, str], T]:
    result = OrderedDict()
    for k1, v1 in d.items():
        for k2, v2 in v1.items():
            result[(k1, k2)] = v2
    return result


E = TypeVar('E')
def process_order_helper(dep: Dict[E, Set[E]], all: Set[E]) -> List[Set[E]]:
    dep_ks = set(dep.keys())
    for k in dep.values():
        dep_ks |= set(k)
    # assert that all the elements in the dependency relations are in the universe set
    assert len(dep_ks - all) == 0, dep_ks - all
    order = list(my_top_sort({k: v for k, v in dep.items()}))
    if len(order) == 0:
        order.append(set())
    for k in all:
        if k not in dep_ks:
            order[0].add(k)
    s = set()
    for o in order:
        s |= set(o)
    assert len(s) == len(all), (s - all, all - s)
    return order


def my_top_sort(dep: Dict[E, Set[E]]) -> List[Set[E]]:
    order = []
    elements_left = set()
    for child, parents in dep.items():
        elements_left.add(child)
        elements_left |= parents

    while len(elements_left) != 0:
        level_set = set()
        for e in elements_left:
            if e not in dep.keys():
                level_set.add(e)
            else:
                if all(parent not in elements_left for parent in dep[e]):
                    level_set.add(e)
        for e in level_set:
            elements_left.remove(e)
        order.append(level_set)
    return order


# order the columns/tables by foreign key references
def get_process_order(child2parent: Dict[Tuple[str, str], Tuple[str, str]],
                      table_column_properties: Dict[Tuple[str, str], Dict[str, Any]])\
        -> Tuple[List[Set[Tuple[str, str]]], List[Set[str]]]:
    all_table_column = set(table_column_properties.keys())
    dep_child2parent = {c: {p} for c, p in child2parent.items()}
    table_column_order = process_order_helper(dep_child2parent, all_table_column)

    all_table = set([k[0] for k in all_table_column])
    table_child2parent = defaultdict(set)
    for k1, k2 in child2parent.items():
        table_child2parent[k1[0]].add(k2[0])
    table_order = process_order_helper(table_child2parent, all_table)
    return table_column_order, table_order


# load information from the database
# including:
# 1. column_properties: (table_name, column_name) -> column properties
#   where column properties are a map from property_name (str) -> value
# 2. foreign key relations: (table_name, column_name) -> (table_name, column_name)
# 3. column_content: (table_name, column_name) -> list, list of element types.
def get_all_db_info_path(sqlite_path: str) \
        -> Tuple[
            Dict[Tuple[str, str], Dict[str, Any]],
            Dict[Tuple[str, str], Tuple[str, str]],
            Dict[Tuple[str, str], List],
        ]:
    table_name2column_properties, child2parent = extract_table_column_properties_path(sqlite_path)

    table_name2content = OrderedDict()
    for table_name in table_name2column_properties:
        result_type, result = exec_db_path_(sqlite_path, select_all_query % table_name)
        # ensure that table retrieval succeeds
        if result_type == 'exception':
            raise result
        table_name2content[table_name] = result

    table_name2column_name2elements = OrderedDict()
    for table_name in table_name2column_properties:
        column_properties, content = table_name2column_properties[table_name], table_name2content[table_name]
        # initialize the map from column name to list of elements
        table_name2column_name2elements[table_name] = OrderedDict((column_name, []) for column_name in column_properties)
        # ensure that the number of columns per row
        # is the number of columns
        if len(content) > 0:
            assert len(content[0]) == len(column_properties)
        for row in content:
            for column_name, element in zip(column_properties, row):
                table_name2column_name2elements[table_name][column_name].append(element)

    return collapse_key(table_name2column_properties), child2parent, collapse_key(table_name2column_name2elements)


def get_table_size(table_column_elements: Dict[Tuple[str, str], List]) -> Dict[str, int]:
    table_name2size = OrderedDict()
    for k, elements in table_column_elements.items():
        table_name = k[0]
        if table_name not in table_name2size:
            table_name2size[table_name] = len(elements)
    return table_name2size


def get_primary_keys(table_column_properties: Dict[Tuple[str, str], Dict[str, Any]]) -> Dict[str, List[str]]:
    table_name2primary_keys = OrderedDict()
    for (table_name, column_name), property in table_column_properties.items():
        if table_name not in table_name2primary_keys:
            table_name2primary_keys[table_name] = []
        if property['PK'] != 0:
            table_name2primary_keys[table_name].append(column_name)
    return table_name2primary_keys


def get_indexing_from_db(db_path: str, shuffle=True) -> Dict[str, List[Dict[str, Any]]]:
    table_column_properties, _, _ = get_all_db_info_path(db_path)
    all_tables_names = {t_c[0] for t_c in table_column_properties}

    table_name2indexes = {}
    for table_name in all_tables_names:
        column_names = [t_c[1] for t_c in table_column_properties if t_c[0] == table_name]
        selection_query = 'select ' + ', '.join(['"%s"' % c for c in column_names]) + ' from "' + table_name + '";'
        retrieved_results = exec_db_path_(db_path, selection_query)[1]
        table_name2indexes[table_name] = [{name: e for name, e in zip(column_names, row)} for row in retrieved_results]
        if shuffle:
            random.shuffle(table_name2indexes[table_name])
    return table_name2indexes


def print_table(table_name, column_names, rows):
    print('table:', table_name)
    num_cols = len(column_names)
    template = " ".join(['{:20}'] * num_cols)
    print(template.format(*column_names))
    for row in rows:
        print(template.format(*[str(r) for r in row]))


def database_pprint(path):
    tc2_, _, _ = get_all_db_info_path(path)
    table_column_names = [tc for tc in tc2_.keys()]
    table_names = {t_c[0] for t_c in table_column_names}
    for table_name in table_names:
        column_names = [c for t, c in table_column_names if t == table_name]
        elements_by_column = []
        for column_name in column_names:
            _, elements = exec_db_path_(path, 'select {column_name} from {table_name}'.format(column_name=column_name, table_name=table_name))
            elements_by_column.append([e[0] for e in elements])
        rows = [row for row in zip(*elements_by_column)]
        print_table(table_name, column_names, rows)



def get_total_size_from_indexes(table_name2indexes: Dict[str, List[Dict[str, Any]]]) -> int:
    return sum([len(v) for _, v in table_name2indexes.items()])


def get_total_size_from_path(path):
    _, _, table_column2elements = get_all_db_info_path(path)
    return sum([v for _, v in get_table_size(table_column2elements).items()])
