import random
from fuzz.base import BaseFuzzer

epsilon = 1e-10
MAX = 9223372036854775807 - epsilon


def isint(x):
    try:
        int(x)
        return True
    except:
        return False


def tofloat(x):
    try:
        return float(x)
    except ValueError:
        return None


def perturb(x):
    if random.random() < 1. / 6:
        return x + 1
    elif random.random() < 1. / 5:
        return x - 1
    elif random.random() < 1. / 4:
        return x + 2
    elif random.random() < 1. / 3:
        return x - 2
    return x


def perturb_float(x):
    if random.random() < 1. / 6:
        return x + 0.01
    elif random.random() < 1. / 5:
        return x - 0.01
    elif random.random() < 1. / 4:
        return x + 0.02
    elif random.random() < 1. / 3:
        return x - 0.02
    return x



class NumberFuzzer(BaseFuzzer):

    def __init__(self, elements, p=0.5, max_l0=float('inf'), scale=10, unsigned=False, is_int=False, precision=0):
        super(NumberFuzzer, self).__init__(elements, p, max_l0)
        self.elements = [x for x in [tofloat(x) for x in self.elements] if x is not None]
        self.scale = scale
        self.min, self.max = -10 ** scale + epsilon, 10 ** scale - epsilon
        if unsigned:
            self.min = 0
            self.max *= 2

        if len(self.elements) != 0:
            self.list_min, self.list_max = min(self.elements), max(self.elements) + 1
        else:
            self.list_min, self.list_max = self.min, self.max
        self.valid_sample_from_elements = self.list_min > -MAX and self.list_max < MAX
        self.min = max(self.min, -MAX)
        self.max = min(self.max, MAX)
        self.list_min = max(self.list_min, -MAX)
        self.list_max = min(self.max, MAX)

        self.is_int = is_int
        self.precision = precision

    def one_sample(self):
        if len(self.elements) == 0 or random.random() < self.p or not self.valid_sample_from_elements:
            start, end = (self.min, self.max) \
                if (random.random() < self.p or len(self.elements) <= 1) \
                else (self.list_min, self.list_max)

            if len(self.rand_elements) <= self.max_l0:
                result = random.random() * (end - start) + start
                self.rand_elements.append(result)
            else:
                result = random.choice(self.rand_elements)

        else:
            result = random.choice(self.elements)

        if self.is_int:
            return perturb(int(result))
        else:
            return perturb_float(result)
