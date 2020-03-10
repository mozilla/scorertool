
import os
import sys
import math
import argparse
import itertools
import subprocess

from collections import Counter
from multiprocessing import Process, Queue
from languages import LANGUAGE_CODES, get_language
from utils import maybe_download, maybe_ungzip, maybe_join
from distutils.spawn import find_executable

STOP_TOKEN = False
TOP_WORDS = 500000
PRUNE_FACTOR = 10
LINES_PER_CHUNK = 100000
MAX_KEYS = 100000

KENLM_BIN = 'dependencies/kenlm/build/bin'
DEEPSPEECH_BIN = 'dependencies/deepspeech'


def get_partial_path(index):
    return os.path.join(LANG.model_dir, 'prepared.txt.partial{}'.format(index))


def count_words(index, counters):
    counter = Counter()
    unprepared_txt = os.path.join(LANG.model_dir, 'unprepared.txt')
    block_size = math.ceil(os.path.getsize(unprepared_txt) / ARGS.workers)
    start = index * block_size
    end = start + block_size
    with open(unprepared_txt, 'rb') as unprepared_file, open(get_partial_path(index), 'w') as partial_file:
        pos = start
        unprepared_file.seek(start)
        while pos < end:
            try:
                lines = unprepared_file.readlines(end - pos)
                if index > 0 and pos == start:
                    lines = lines[1:]
                lines = list(itertools.chain.from_iterable(map(lambda l: LANG.clean(l.decode()), lines)))
                pos = unprepared_file.tell()
                for line in lines:
                    for word in line.split():
                        counter[word] += 1
                partial_file.writelines(map(lambda l: l + '\n', lines))
                if len(counter.keys()) > MAX_KEYS or pos >= end:
                    counters.put(counter)
                    counter = Counter()
            except Exception as ex:
                print('Something went wrong:' + str(ex))


def aggregate_counters(vocab_filename, counters):
    overall_counter = Counter()
    while True:
        counter = counters.get()
        if counter == STOP_TOKEN:
            with open(sys.argv[1], 'w') as vocab_file:
                vocab_file.write('\n'.join(str(word) for word, count in overall_counter.most_common(TOP_WORDS)))
            return
        print('aggregate counter', flush=True, file=sys.stderr)
        overall_counter += counter
        if len(overall_counter.keys()) > PRUNE_FACTOR * TOP_WORDS:
            print('pruning counter', flush=True, file=sys.stderr)
            overall_counter = Counter(overall_counter.most_common(TOP_WORDS))


def write_lines(prepared_txt_gz, resulting_lines):
    with open(prepared_txt_gz, 'wb') as archive_file:
        gzip = subprocess.Popen(['pigz'], stdin=subprocess.PIPE, stdout=archive_file)
        while True:
            lines = resulting_lines.get()
            if lines == STOP_TOKEN:
                return
            print('writing cleaned chunk', flush=True, file=sys.stderr)
            gzip.stdin.writelines(lines)


def main():
    raw_txt_gz = os.path.join(LANG.model_dir, 'raw.txt.gz')
    unprepared_txt = os.path.join(LANG.model_dir, 'unprepared.txt')
    prepared_txt = os.path.join(LANG.model_dir, 'prepared.txt')
    vocab_txt = os.path.join(LANG.model_dir, 'vocabular.txt')
    unfiltered_arpa = os.path.join(LANG.model_dir, 'unfiltered.arpa')
    filtered_arpa = os.path.join(LANG.model_dir, 'filtered.arpa')

    maybe_download(LANG.text_url, raw_txt_gz)
    maybe_ungzip(raw_txt_gz, unprepared_txt)

    counters = Queue(ARGS.workers)

    aggregator_process = Process(target=aggregate_counters, args=(vocab_txt, counters))
    aggregator_process.start()

    counter_processes = list(map(lambda index: Process(target=count_words, args=(index, counters)),
                                 range(ARGS.workers)))
    for p in counter_processes:
        p.start()
    for p in counter_processes:
        p.join()

    counters.put(STOP_TOKEN)
    aggregator_process.join()

    maybe_join(list(map(lambda i: get_partial_path(i), range(ARGS.workers))), prepared_txt)

    subprocess.check_call([
        KENLM_BIN + '/lmplz',
        # '--temp_prefix', tmp_prefix,
        '--memory', '25%',
        '--discount_fallback',
        '--text', prepared_txt,
        '--arpa', unfiltered_arpa,
        '--skip', 'symbols',
        '--order', '5',
        '--prune', '0', '0', '1'
    ])

    subprocess.check_call([
        KENLM_BIN + '/filter',
        'single',
        'model:' + unfiltered_arpa,
        filtered_arpa
    ])

    subprocess.check_call([
        KENLM_BIN + '/build_binary',
        '-a', '255',
        '-q', '8',
        'trie',
        '-s',
        filtered_arpa,
        lm_path
    ])

    subprocess.check_call([
        DEEPSPEECH_BIN + '/generate_trie',
        alphabet_path,
        lm_path,
        trie_path
    ])


def parse_args():
    parser = argparse.ArgumentParser(description='Generate language models from OSCAR corpora', prog='genlm')
    parser.add_argument('--language', default='en', choices=LANGUAGE_CODES,
                        help='language of the model to generate')
    parser.add_argument('--workers', type=int, default=os.cpu_count(),
                        help='number of preparation and counting workers')
    parser.add_argument('--simulate', action='store_true',
                        help='simulate language model generation with small amount of input data')

    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_args()
    LANG = get_language(ARGS.language)
    print(LANG.model_dir)
    main()
