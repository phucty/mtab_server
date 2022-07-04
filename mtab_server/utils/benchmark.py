from datetime import datetime
import psutil
import os
from mtab_server.utils import io_worker as iw


def profile(func):
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info()
        start = datetime.now()

        result = func(*args, **kwargs)

        end = datetime.now() - start
        mem_after = process.memory_info()
        rss = iw.get_size_obj(mem_after.rss - mem_before.rss)
        vms = iw.get_size_obj(mem_after.vms - mem_before.vms)
        print(f"{func.__name__}\tTime: {end}\tRSS: {rss}\tVMS: {vms}")
        return result

    return wrapper
