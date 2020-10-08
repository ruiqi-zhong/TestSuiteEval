from fuzz.base import BaseFuzzer
import random

CHARSET = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789()'


def represents_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def rand_string(length: int) -> str:
    return ''.join([random.choice(CHARSET) for _ in range(length)])


def contaminate(s: str) -> str:
    p = random.random()
    if p < 0.2:
        return s + rand_string(5)
    if p < 0.4:
        return rand_string(5) + s
    if p < 0.6:
        return rand_string(3) + s + rand_string(3)
    if p < 0.8:
        return s[:-1]
    return s[1:]


class StringFuzzer(BaseFuzzer):

    def __init__(self, elements, p, max_l0, length=20):
        super(StringFuzzer, self).__init__(elements, p, max_l0)
        self.allow_none = random.random() < 0.1
        self.length = length
        self.elements = [str(e) for e in self.elements]
        for e in self.elements:
            if '%' in e:
                self.elements.append(e.replace('%', ''))

        for s in set(self.elements):
            if represents_int(s):
                i = int(s)
                self.elements.append(str(i + 1))
                self.elements.append(str(i - 1))
        all_lengths = [len(e) for e in self.elements]
        if len(all_lengths) != 0:
            self.min_length, self.max_length = min(all_lengths), max(all_lengths) + 1
        else:
            self.min_length, self.max_length = 0, 20


    def one_sample(self):
        if random.random() < self.p:
            if len(self.rand_elements) <= self.max_l0:
                length = random.randint(self.min_length, self.max_length)
                r = rand_string(length)
                self.rand_elements.append(r)
                result = r
            else:
                result = random.choice(self.rand_elements)
        else:
            result = random.choice(self.elements)
        if random.random() < 0.5:
            return contaminate(result)
        else:
            return result

