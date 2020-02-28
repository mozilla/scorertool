
import sys
import time


KILO = 1024
KILOBYTE = 1 * KILO
MEGABYTE = KILO * KILOBYTE
GIGABYTE = KILO * MEGABYTE
TERABYTE = KILO * GIGABYTE
SIZE_PREFIX_LOOKUP = {'k': KILOBYTE, 'm': MEGABYTE, 'g': GIGABYTE, 't': TERABYTE}


def parse_file_size(file_size):
    file_size = file_size.lower().strip()
    if len(file_size) == 0:
        return 0
    n = int(keep_only_digits(file_size))
    if file_size[-1] == 'b':
        file_size = file_size[:-1]
    e = file_size[-1]
    return SIZE_PREFIX_LOOKUP[e] * n if e in SIZE_PREFIX_LOOKUP else n


def keep_only_digits(txt):
    return ''.join(filter(str.isdigit, txt))


def secs_to_hours(secs):
    hours, remainder = divmod(secs, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '%02d:%02d:%02d' % (hours, minutes, seconds)


def log_progress(it, total=None, interval=60.0, step=None, entity='it', file=sys.stderr):
    if total is None and hasattr(it, '__len__'):
        total = len(it)
    if total is None:
        line_format = ' {:8d} (elapsed: {}, speed: {:.2f} {}/{})'
    else:
        line_format = ' {:' + str(len(str(total))) + 'd} of {} : {:6.2f}% (elapsed: {}, speed: {:.2f} {}/{}, ETA: {})'

    overall_start = time.time()
    interval_start = overall_start
    interval_steps = 0

    def print_interval(steps, time_now):
        elapsed = time_now - overall_start
        elapsed_str = secs_to_hours(elapsed)
        speed_unit = 's'
        interval_duration = time_now - interval_start
        print_speed = speed = interval_steps / (0.001 if interval_duration == 0.0 else interval_duration)
        if print_speed < 0.1:
            print_speed = print_speed * 60
            speed_unit = 'm'
            if print_speed < 1:
                print_speed = print_speed * 60
                speed_unit = 'h'
        elif print_speed > 1000:
            print_speed = print_speed / 1000.0
            speed_unit = 'ms'
        if total is None:
            line = line_format.format(global_step, elapsed_str, print_speed, entity, speed_unit)
        else:
            percent = global_step * 100.0 / total
            eta = secs_to_hours(((total - global_step) / speed) if speed > 0 else 0)
            line = line_format.format(global_step, total, percent, elapsed_str, print_speed, entity, speed_unit, eta)
        print(line, file=file)
        file.flush()

    for global_step, obj in enumerate(it, 1):
        interval_steps += 1
        yield obj
        t = time.time()
        if (step is None and t - interval_start > interval) or (step is not None and interval_steps >= step):
            print_interval(interval_steps, t)
            interval_steps = 0
            interval_start = t
    if interval_steps > 0:
        print_interval(interval_steps, time.time())


class DownloadProgress():
    def __init__(self):
        self.downloaded = 0

    def __call__(self, block_num, block_size, total_size):
        self.downloaded += block_size

