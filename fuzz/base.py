from typing import List


class BaseFuzzer:

    def __init__(self, elements: List, p: float, max_l0: float = float('inf')):
        self.elements = [e for e in elements if e is not None]
        self.p = p if len(self.elements) != 0 else 1

        self.max_l0 = max_l0
        self.rand_elements = []

    def one_sample(self):
        raise NotImplementedError

    def n_examples(self, num_samples: int) -> List:
        return [self.one_sample() for _ in range(num_samples)]

