from datetime import timedelta, datetime
from utils.data_parser import date_parser, num2month
from fuzz.base import BaseFuzzer
import random


def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


START = datetime.strptime('01/01/1800 1:01 PM', '%m/%d/%Y %I:%M %p')
END = datetime.strptime('01/01/2100 1:01 PM', '%m/%d/%Y %I:%M %p')


class DateFuzzer(BaseFuzzer):

    def __init__(self, elements, p=0.5, max_l0=float('inf')):
        super(DateFuzzer, self).__init__(elements, p, max_l0)

        self.template = '%Y-%m-%d %H:%M:%S'
        template_found = False
        self.orig_type = str

        element_dates = []
        for element in elements:
            if type(element) == int:
                self.orig_type = int
            parse_result = date_parser(element)
            if parse_result['value'] is not None and not template_found:
                self.template = parse_result['template']
                template_found = True
            if parse_result['value'] is not None:
                element_dates.append(parse_result['value'])
        self.element_dates = element_dates

        if len(self.element_dates) != 0:
            self.min_date, self.max_date = min(self.element_dates), max(self.element_dates)
        else:
            self.min_date, self.max_date = START, END
        if self.min_date == self.max_date:
            self.min_date, self.max_date = START, END

    def one_sample(self):
        if random.random() < self.p and len(self.elements) != 0:
            return random.choice(self.elements)

        if len(self.rand_elements) >= self.max_l0:
            return random.choice(self.rand_elements)

        start, end = (START, END) \
            if (random.random() < self.p or len(self.element_dates) <= 1) \
            else (self.min_date, self.max_date)
        r = random_date(start, end)
        s = r.strftime(self.template)
        if self.template == '%d-%m-%Y':
            for num, month in num2month.items():
                s = s.replace('-' + num + '-', '-' + month + '-')

        if self.orig_type == str:
            start_w0 = False
            for e in self.elements:
                if '-0' in e or e[0] == '0':
                    start_w0 = True
                    break
            if not start_w0:
                s = s.replace('-0', '-')
                if s[0] == '0':
                    s = s[1:]
        result = self.orig_type(s)
        self.rand_elements.append(result)
        return result
