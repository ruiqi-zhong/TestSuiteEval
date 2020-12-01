import datetime


dtype_result = {str, }


month2num = {
    'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
    'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
}
num2month = {num: month for month, num in month2num.items()}


templates = {
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',
    '%Y%m%d',
    '%Y-%m-%d',
    '%d-%m-%Y',
    '%m/%d/%Y %H:%M'
}


def clean_str_month(o):
    if isinstance(o, int):
        o = str(o)
    for month, n in month2num.items():
        o = o.replace(month, n)
    return o

def date_parser(o):
    default_result = {
        'value': None,
        'template': 'N/A'
    }
    if o is None:
        return default_result

    o = clean_str_month(o)
    for t in templates:
        try:
            d = datetime.datetime.strptime(o, t)
            return {
                'value': d,
                'template': t
            }
        except ValueError:
            pass
    return default_result
