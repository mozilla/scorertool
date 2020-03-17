
import os
import sys
import math
import time
import inspect
import requests
import subprocess
from functools import partial
from distutils.spawn import find_executable


KILO = 1024
KILOBYTE = 1 * KILO
MEGABYTE = KILO * KILOBYTE
SIZE_PREFIXES = 'kMGTPEZY'
SIZE_PREFIX_LOOKUP = {}
for exp, prefix in enumerate(SIZE_PREFIXES):
    SIZE_PREFIX_LOOKUP[prefix.lower()] = int(math.pow(KILO, exp + 1))

UNZIP = 'unpigz' if find_executable('unpigz') else 'gunzip'


def announce(message, file=sys.stderr, flush=True, end='\n'):
    print(message, file=file, flush=flush, end=end)


def parse_file_size(file_size):
    file_size = file_size.lower().strip()
    if len(file_size) == 0:
        return 0
    n = int(keep_only_digits(file_size))
    if file_size[-1] == 'b':
        file_size = file_size[:-1]
    e = file_size[-1]
    return SIZE_PREFIX_LOOKUP[e] * n if e in SIZE_PREFIX_LOOKUP else n


def human_readable_file_size(file_size, sep=' '):
    exp = min(math.floor(math.log(file_size, KILO)), len(SIZE_PREFIXES))
    return ('{:.0f}{}{}B' if exp == 0 else '{:.2f}{}{}B')\
        .format(file_size / math.pow(KILO, exp), sep, SIZE_PREFIXES[exp - 1] if exp > 0 else '')


def keep_only_digits(txt):
    return ''.join(filter(str.isdigit, txt))


def secs_to_hours(secs):
    hours, remainder = divmod(secs, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '%02d:%02d:%02d' % (hours, minutes, seconds)


def section(title, width=100, border_width=12, empty_lines_before=3, empty_lines_after=1):
    announce('\n' * empty_lines_before, end='')
    announce('*' * width)
    title = ' ' * (((width - 2 * border_width) - len(title)) // 2) + str(title)
    title = title + ' ' * (width - len(title) - 2 * border_width)
    announce('*' * border_width + title + '*' * border_width)
    announce('*' * width)
    announce('\n' * empty_lines_after, end='')


class log_progress:
    def __init__(self,
                 it=None,
                 total=None,
                 max_interval_time=1,
                 max_interval_value=None,
                 format='{} it',
                 time_unit=None,
                 value_getter=lambda obj: 1,
                 absolute=False,
                 file=sys.stderr):
        self.it = it
        self.total = total
        if self.total is None and self.it is not None and hasattr(it, '__len__'):
            self.total = len(it)
        self.time_unit = time_unit
        self.value_getter = value_getter
        self.absolute = absolute
        if inspect.isfunction(format):
            self.format = format
        elif format == 'bytes':
            self.format = lambda nb: human_readable_file_size(nb)
            if self.time_unit is None:
                self.time_unit = 's'
        else:
            self.format = lambda n: format.format(n)
        self.line_format = ' {} (elapsed: {}, speed: {}/{})' if self.total is None \
            else ' {} of {} : {:6.2f}% (elapsed: {}, ETA: {}, speed: {}/{})'
        self.max_interval_time = max_interval_time
        self.max_interval_value = max_interval_value
        self.current_value = 0
        self.last_value = 0
        self.file = file
        self.overall_start = time.time()
        self.interval_start = self.overall_start

    def print_interval(self, time_now):
        elapsed = time_now - self.overall_start
        elapsed_str = secs_to_hours(elapsed)
        interval_duration = time_now - self.interval_start
        value_difference = self.current_value - self.last_value
        print_speed = speed = value_difference / (0.001 if interval_duration == 0.0 else interval_duration)
        time_unit = 's'
        if self.time_unit is not None:
            time_unit = self.time_unit
            print_speed = print_speed * {'ms': 1/1000, 's': 1, 'm': 60, 'h': 60 * 60, 'd': 24 * 60 * 60}[time_unit]
        elif print_speed < 0.1:
            print_speed = print_speed * 60
            time_unit = 'm'
            if print_speed < 1:
                print_speed = print_speed * 60
                time_unit = 'h'
        elif print_speed > 1000:
            print_speed = print_speed / 1000.0
            time_unit = 'ms'
        if self.total is None:
            line = self.line_format.format(self.format(self.current_value),
                                           elapsed_str,
                                           self.format(print_speed),
                                           time_unit)
        else:
            percent = self.current_value * 100.0 / self.total
            eta = secs_to_hours(max(0, ((self.total - self.current_value) / speed) if speed > 0 else 0))
            line = self.line_format.format(self.format(self.current_value),
                                           self.format(self.total),
                                           percent,
                                           elapsed_str,
                                           eta,
                                           self.format(print_speed),
                                           time_unit)
        announce(line, file=self.file, flush=True)
        self.last_value = self.current_value
        self.interval_start = time_now

    def update(self, value=None):
        if value is not None:
            self.current_value = value
        t = time.time()
        value_difference = self.current_value - self.last_value
        if ((self.max_interval_value is None and t - self.interval_start > self.max_interval_time) or
                (self.max_interval_value is not None and value_difference >= self.max_interval_value)):
            self.print_interval(t)

    def increment(self, value_difference=1):
        self.update(value=self.current_value + value_difference)

    def end(self):
        if self.current_value - self.last_value > 0:
            self.print_interval(time.time())

    def __iter__(self):
        for obj in self.it:
            yield obj
            value = self.value_getter(obj)
            if self.absolute:
                self.update(value=value)
            else:
                self.increment(value_difference=value)
        self.end()


def download(from_url, to_path, block_size=1 * MEGABYTE):
    r = requests.get(from_url, stream=True)
    total_size = int(r.headers.get('content-length', 0))
    with open(to_path, 'wb') as to_file:
        announce('Downloading "{}" to "{}"...'.format(from_url, to_path))
        for block in log_progress(r.iter_content(block_size), total=total_size, format='bytes', value_getter=len):
            to_file.write(block)


def maybe_download(from_url, to_path, force=False):
    if os.path.isfile(to_path) and not force:
        announce('File "{}" already existing - not downloading again'.format(to_path))
        return False
    else:
        download(from_url, to_path)
        return True


def ungzip(from_path, to_path, block_size=1 * MEGABYTE):
    total_size = os.path.getsize(from_path)
    with open(from_path, 'rb') as from_file, open(to_path, 'wb') as to_file:
        gunzip = subprocess.Popen([UNZIP], stdin=subprocess.PIPE, stdout=to_file)
        announce('Unzipping "{}" to "{}"...'.format(from_path, to_path))
        blocks = iter(partial(from_file.read, block_size), b'')
        for block in log_progress(blocks, total=total_size, format='bytes', value_getter=len):
            gunzip.stdin.write(block)


def maybe_ungzip(from_path, to_path, force=False):
    if os.path.isfile(to_path) and not force:
        announce('File "{}" already existing - not unzipping again'.format(to_path))
        return False
    else:
        ungzip(from_path, to_path)
        return True


def join_files(from_paths, to_path, block_size=1 * MEGABYTE):
    total_size = sum(map(lambda f: os.path.getsize(f), from_paths))

    def _read_blocks():
        for from_path in from_paths:
            with open(from_path, 'rb') as from_file:
                yield from iter(partial(from_file.read, block_size), b'')

    with open(to_path, 'wb') as to_file:
        announce('Joining {} files to "{}"...'.format(len(from_paths), to_path))
        for block in log_progress(_read_blocks(), total=total_size, format='bytes', value_getter=len):
            to_file.write(block)


def maybe_join(from_paths, to_path, force=False):
    if os.path.isfile(to_path) and not force:
        announce('File "{}" already existing - not joining'.format(to_path))
        return False
    else:
        join_files(from_paths, to_path)
        return True
