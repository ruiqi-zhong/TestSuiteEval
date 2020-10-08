from fuzz.base import BaseFuzzer
from fuzz.string import StringFuzzer
import random
from datetime import datetime, timedelta

start = datetime(2020, 1, 1, 00, 00, 00)


class SemesterFuzzer(BaseFuzzer):

    def __init__(self, elements, p):
        super(SemesterFuzzer, self).__init__(elements, p)
        self.semesters = ['Fall', 'Winter', 'Spring', 'Summer']

    def one_sample(self):
        return random.choice(self.semesters)


class AdvisingTimeFuzzer(BaseFuzzer):

    def rand_time_in_day(self):
        randtime = start + random.random() * timedelta(days=1)
        s = randtime.strftime('%H-%M-%S')
        return s

    def __init__(self, elements, p):
        super(AdvisingTimeFuzzer, self).__init__(elements, p)

    def one_sample(self):
        if random.random() > self.p:
            return random.choice(self.elements)
        else:
            return self.rand_time_in_day()



special_cases = {
    ('database/college_2/college_2.sqlite', 'section', 'SEMESTER'): SemesterFuzzer,
    ('database/advising/advising.sqlite', 'course_offering', 'START_TIME'): AdvisingTimeFuzzer,
    ('database/advising/advising.sqlite', 'course_offering', 'END_TIME'): AdvisingTimeFuzzer
}

corrected_keys = {
    ('database/imdb/imdb.sqlite', 'cast', 'ROLE'): 'text',
    ('database/imdb/imdb.sqlite', 'writer', 'NAME'): 'text',
    ('database/imdb/imdb.sqlite', 'writer', 'NATIONALITY'): 'text',
    ('database/imdb/imdb.sqlite', 'director', 'DID'): 'text'
}
