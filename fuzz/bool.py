from fuzz.base import BaseFuzzer
import random


class BoolFuzzer(BaseFuzzer):

    def __init__(self, elements, p):
        super(BoolFuzzer, self).__init__(elements, p)

    def one_sample(self):
        if random.random() < self.p or len(self.elements) == 0:
            return random.random() < 0.5
        else:
            return random.choice(self.elements)


class BitFuzzer(BaseFuzzer):

    def __init__(self, elements, p):
        super(BitFuzzer, self).__init__(elements, p)

    def one_sample(self):
        if random.random() < self.p or len(self.elements) == 0:
            return 1 if random.random() < 0.5 else 0
        else:
            return random.choice(self.elements)
