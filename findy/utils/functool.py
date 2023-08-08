from functools import wraps
import time


def time_it(func):
    @wraps(func)
    async def inner(*args):
        start = time.time()
        ret = await func(*args)
        return time.time() - start, ret
    return inner