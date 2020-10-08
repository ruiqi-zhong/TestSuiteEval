from fuzz.base import BaseFuzzer
import random


space = [
    [('0' if i < 10 else '') + str(i) for i in range(24)],
    [('0' if i < 10 else '') + str(i) for i in range(60)],
    [('0' if i < 10 else '') + str(i) for i in range(60)]
]


def random_time():
    return ':'.join([random.choice(l) for l in space])


def perturb(t):
    nums = [int(x) for x in t.split(':')]
    change_digit = random.randint(0, 2)
    nums[change_digit] = nums[change_digit] - 1
    return ':'.join([space[i][nums[i]] for i in range(3)])



class TimeFuzzer(BaseFuzzer):

    def __init__(self, elements, p=0.5, max_l0=float('inf')):
        super(TimeFuzzer, self).__init__(elements, p, max_l0)
        self.elements = elements

    def one_sample(self):
        if random.random() > self.p and len(self.elements) != 0:
            t = random.choice(self.elements)
            if random.random() < self.p:
                return perturb(t)
            else:
                return t
        else:
            return random_time()
