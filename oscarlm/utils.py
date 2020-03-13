
import os
import sys
import math
import time
import requests
import subprocess
from functools import partial
from distutils.spawn import find_executable


KILO = 1024
KILOBYTE = 1 * KILO
MEGABYTE = KILO * KILOBYTE
GIGABYTE = KILO * MEGABYTE
TERABYTE = KILO * GIGABYTE
SIZE_PREFIX_LOOKUP = {'k': KILOBYTE, 'm': MEGABYTE, 'g': GIGABYTE, 't': TERABYTE}

UNZIP = 'unpigz' if find_executable('unpigz') else 'gunzip'


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


def section(title, width=100, border_width=12, empty_lines_before=3, empty_lines_after=1):
    print('\n' * empty_lines_before, end='')
    print('*' * width)
    title = ' ' * (((width - 2 * border_width) - len(title)) // 2) + str(title)
    title = title + ' ' * (width - len(title) - 2 * border_width)
    print('*' * border_width + title + '*' * border_width)
    print('*' * width)
    print('\n' * empty_lines_after, end='')


class log_progress:
    def __init__(self, it=None, total=None, interval=60, step=None, entity='it', format=None, file=sys.stderr):
        self.it = it
        self.total = total
        if self.total is None and self.it is not None and hasattr(it, '__len__'):
            self.total = len(it)
        if format is None:
            self.format = ':' + str(8 if self.total is None else len(str(self.total))) + 'd'
        else:
            self.format = format
        if self.total is None:
            self.line_format = ' {' + self.format + '} {} (elapsed: {}, speed: {:.2f} {}/{})'
        else:
            self.line_format = ' {' + self.format + '} of {' + self.format + '} {} : {:6.2f}% ' \
                               '(elapsed: {}, speed: {:.2f} {}/{}, ETA: {})'
        self.interval = interval
        self.global_step = 0
        self.step = step
        self.entity = entity
        self.file = file
        self.overall_start = time.time()
        self.interval_start = self.overall_start
        self.interval_steps = 0

    def print_interval(self, steps, time_now):
        elapsed = time_now - self.overall_start
        elapsed_str = secs_to_hours(elapsed)
        speed_unit = 's'
        interval_duration = time_now - self.interval_start
        print_speed = speed = self.interval_steps / (0.001 if interval_duration == 0.0 else interval_duration)
        if print_speed < 0.1:
            print_speed = print_speed * 60
            speed_unit = 'm'
            if print_speed < 1:
                print_speed = print_speed * 60
                speed_unit = 'h'
        elif print_speed > 1000:
            print_speed = print_speed / 1000.0
            speed_unit = 'ms'
        if self.total is None:
            line = self.line_format.format(self.global_step,
                                           self.entity,
                                           elapsed_str,
                                           print_speed,
                                           self.entity,
                                           speed_unit)
        else:
            percent = self.global_step * 100.0 / self.total
            eta = secs_to_hours(max(0, ((self.total - self.global_step) / speed) if speed > 0 else 0))
            line = self.line_format.format(self.global_step,
                                           self.total,
                                           self.entity,
                                           percent,
                                           elapsed_str,
                                           eta,
                                           print_speed,
                                           self.entity,
                                           speed_unit)
        print(line, file=self.file, flush=True)
        self.interval_steps = 0
        self.interval_start = time_now

    def increment(self, steps=1):
        self.global_step += steps
        self.interval_steps += steps
        t = time.time()
        if ((self.step is None and t - self.interval_start > self.interval) or
                (self.step is not None and self.interval_steps >= self.step)):
            self.print_interval(self.interval_steps, t)

    def end(self):
        if self.interval_steps > 0:
            self.print_interval(self.interval_steps, time.time())

    def __iter__(self):
        for obj in self.it:
            yield obj
            self.increment()
        self.end()


def download(from_url, to_path):
    r = requests.get(from_url, stream=True)
    total_size = int(r.headers.get('content-length', 0))
    block_size = 1 * MEGABYTE
    total_blocks = (total_size // block_size) + 1
    with open(to_path, 'wb') as to_file:
        print('Downloading "{}" to "{}"...'.format(from_url, to_path))
        for block in log_progress(r.iter_content(block_size), total=total_blocks, entity='MB'):
            to_file.write(block)


def maybe_download(from_url, to_path, force=False):
    if os.path.isfile(to_path) and not force:
        print('File "{}" already existing - not downloading again'.format(to_path))
        return False
    else:
        download(from_url, to_path)
        return True


def ungzip(from_path, to_path):
    total_size = os.path.getsize(from_path)
    block_size = 1 * MEGABYTE
    total_blocks = (total_size // block_size) + 1
    with open(from_path, 'rb') as from_file, open(to_path, 'wb') as to_file:
        gunzip = subprocess.Popen([UNZIP], stdin=subprocess.PIPE, stdout=to_file)
        print('Unzipping "{}" to "{}"...'.format(from_path, to_path))
        for block in log_progress(iter(partial(from_file.read, block_size), b''), total=total_blocks, entity='MB'):
            gunzip.stdin.write(block)


def maybe_ungzip(from_path, to_path, force=False):
    if os.path.isfile(to_path) and not force:
        print('File "{}" already existing - not unzipping again'.format(to_path))
        return False
    else:
        ungzip(from_path, to_path)
        return True


def join_files(from_paths, to_path):
    block_size = 1 * MEGABYTE
    total_blocks = sum(map(lambda f: math.ceil(os.path.getsize(f) / block_size), from_paths))

    def _read_blocks():
        for from_path in from_paths:
            with open(from_path, 'rb') as from_file:
                yield from iter(partial(from_file.read, block_size), b'')

    with open(to_path, 'wb') as to_file:
        print('Joining {} files to "{}"...'.format(len(from_paths), to_path))
        for block in log_progress(_read_blocks(), total=total_blocks, entity='MB'):
            to_file.write(block)


def maybe_join(from_paths, to_path, force=False):
    if os.path.isfile(to_path) and not force:
        print('File "{}" already existing - not joining'.format(to_path))
        return False
    else:
        join_files(from_paths, to_path)
        return True
