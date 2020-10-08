from typing import List


def load_sql_file(f_path: str) -> List[str]:
    with open(f_path, 'r') as in_file:
        lines = list(in_file.readlines())
    lines = [l.strip().split('\t')[0] for l in lines if len(l.strip()) > 0]
    return lines
