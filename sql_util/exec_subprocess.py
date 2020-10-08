import sys
sys.path.append('./')
from sql_util.run import exec_db_path_
import pickle as pkl

f_prefix = sys.argv[1]
func_args = pkl.load(open(f_prefix + '.in', 'rb'))
sqlite_path, query = func_args
result = exec_db_path_(sqlite_path, query)
pkl.dump(result, open(f_prefix + '.out', 'wb'))
