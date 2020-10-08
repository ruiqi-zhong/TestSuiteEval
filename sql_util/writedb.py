import os
from typing import List, Tuple, Dict
from sql_util.run import get_cursor_path
from shutil import copyfile
from sql_util.dbinfo import table_name_query, get_table_names_path, extract_table_column_properties_path, \
    get_process_order, get_all_db_info_path, get_total_size_from_path
import random
from typing import Any


def insert_row(cursor, table_name: str, column_names: List[str], row: Tuple) -> str:
    assert len(row) == len(column_names), "number of elements per row needs to be the same as number of columns"
    dummy_args = " ,".join(["?"] * len(column_names))
    q = "INSERT INTO {table_name} VALUES ({dummy_args})"\
        .format(table_name=table_name, dummy_args=dummy_args)
    try:
        cursor.execute(q, row)
        return 'success'
    except Exception as e:
        print('unable to insert the following')
        print(q)
        print(row)
        return "fails"
        # raise e


def insert_table(cursor, table_name: str, column2elements: Dict[str, List]) -> None:
    column_names = list(column2elements.keys())
    num_rows = len(column2elements[column_names[0]])

    one_success = False
    for row_id in range(num_rows):
        row = tuple([column2elements[column_name][row_id] for column_name in column_names])
        insertion_result = insert_row(cursor, table_name, column_names, row)
        if insertion_result == 'success':
            one_success = True
    if not one_success:
        print('no successful insertion for table %s' % table_name)


# we write a new data base
# by copying from an empty database
# and insert columns
def write_db_path(orig_path: str, new_db_path: str, table2column2elements: Dict[str, Dict[str, List]],
             overwrite: bool = False) -> None:
    if os.path.exists(new_db_path) and not overwrite:
        print('new database already exists.')
        return
    empty_db_path = init_empty_db_from_orig_(orig_path)
    copyfile(empty_db_path, new_db_path)
    os.unlink(empty_db_path)
    cursor = get_cursor_path(new_db_path)
    table_name2column_properties, _ = extract_table_column_properties_path(orig_path)

    for table_name, column2elements in table2column2elements.items():
        # the order of the column should stay the same
        columns = list(column2elements.keys())
        orig_columns = list(table_name2column_properties[table_name].keys())
        assert columns == orig_columns, (columns, orig_columns)
        insert_table(cursor, table_name, column2elements)
    cursor.connection.commit()
    cursor.connection.close()


remove_query = 'delete from %s;'
EMPTY = 'EMPTYRARE'


def init_empty_db_from_orig_(sqlite_path: str, verbose: bool = False) -> str:
    empty_db_path = sqlite_path + EMPTY + str(random.randint(0, 10000000000))

    assert empty_db_path != sqlite_path

    # copy the old database
    # initialize a new one and get the cursor
    copyfile(sqlite_path, empty_db_path)
    cursor = get_cursor_path(empty_db_path)
    table_names = get_table_names_path(sqlite_path)
    for table_name in table_names:
        cursor.execute(remove_query % table_name)
    if verbose:
        cursor.execute(table_name_query)
        result = cursor.fetchall()
        print('Tables created: ')
        print(result)
    cursor.connection.commit()
    cursor.connection.close()
    return empty_db_path


def subsample_db(orig_path: str, target_path: str,
                 delete_fraction: float = 0.5, overwrite: bool = False):
    if os.path.exists(target_path) and not overwrite:
        raise Exception('Path %s exists, do not overwrite.' % target_path)
    copyfile(orig_path, target_path)
    cursor = get_cursor_path(target_path)

    table_column_properties, child2parent, _ = get_all_db_info_path(target_path)
    _, table_order = get_process_order(child2parent, table_column_properties)
    for table in table_order:
        cursor.execute('DELETE TOP (%d) PERCENT FROM %s;' % (int(delete_fraction * 100), table))
    cursor.connection.commit()
    cursor.connection.close()


# delete an entry from the original path and store the result in the target path
def delete_entry_from_db(orig_path: str, target_path: str, table_name: str, entry: Dict[str, Any]):
    if orig_path != target_path:
        os.system('cp {orig_path} {target_path}'.format(orig_path=orig_path, target_path=target_path))
    deletion_query = 'delete from "{table_name}" where '.format(table_name=table_name)
    for column_name, val in entry.items():
        deletion_query += '"{column_name}" = "{val}" AND '.format(column_name=column_name, val=val)
    deletion_query += ';'
    deletion_query = deletion_query.replace('AND ;', ';')

    cursor = get_cursor_path(target_path)
    cursor.execute(deletion_query)
    cursor.connection.commit()
    cursor.connection.close()