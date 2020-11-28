import time


def get_current_time_ms() -> int:
    return int(time.time() * 1000)
