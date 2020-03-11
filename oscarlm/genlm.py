
import os
import sys
import math
import argparse
import itertools
import subprocess

from collections import Counter
from multiprocessing import Process, Queue
from languages import LANGUAGE_CODES, get_language
from utils import maybe_download, maybe_ungzip, maybe_join, section

STOP_TOKEN = False
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
                print('Preparation worker failed:' + str(ex))


def aggregate_counters(vocabulary_txt, counters):
    overall_counter = Counter()
    while True:
        counter = counters.get()
        if counter == STOP_TOKEN:
            with open(vocabulary_txt, 'w') as vocabulary_file:
                vocabulary_file.write('\n'.join(str(word) for word, count in overall_counter.most_common(ARGS.vocabulary_size)))
            return
        overall_counter += counter
        if len(overall_counter.keys()) > ARGS.prune_factor * ARGS.vocabulary_size:
            overall_counter = Counter(overall_counter.most_common(ARGS.vocabulary_size))


def main():
    raw_txt_gz = os.path.join(LANG.model_dir, 'raw.txt.gz')
    unprepared_txt = os.path.join(LANG.model_dir, 'unprepared.txt')
    prepared_txt = os.path.join(LANG.model_dir, 'prepared.txt')
    vocabulary_txt = os.path.join(LANG.model_dir, 'vocabulary.txt')
    unfiltered_arpa = os.path.join(LANG.model_dir, 'unfiltered.arpa')
    filtered_arpa = os.path.join(LANG.model_dir, 'filtered.arpa')
    lm_binary = os.path.join(LANG.model_dir, 'lm.binary')

    section('Downloading text data', empty_lines_before=1)
    maybe_download(LANG.text_url, raw_txt_gz)

    section('Unzipping text data')
    maybe_ungzip(raw_txt_gz, unprepared_txt)

    section('Preparing text and building vocabulary')
    if not os.path.isfile(prepared_txt) or not os.path.isfile(vocabulary_txt):
        counters = Queue(ARGS.workers)
        aggregator_process = Process(target=aggregate_counters, args=(vocabulary_txt, counters))
        aggregator_process.start()
        counter_processes = list(map(lambda index: Process(target=count_words, args=(index, counters)),
                                     range(ARGS.workers)))
        try:
            for p in counter_processes:
                p.start()
            for p in counter_processes:
                p.join()
            counters.put(STOP_TOKEN)
            aggregator_process.join()
            partials = list(map(lambda i: get_partial_path(i), range(ARGS.workers)))
            maybe_join(partials, prepared_txt)
            for partial in partials:
                os.unlink(partial)
        except KeyboardInterrupt:
            aggregator_process.terminate()
            for p in counter_processes:
                p.terminate()
            raise
    else:
        print('Files "{}" and "{}" existing - not preparing'.format(prepared_txt, vocabulary_txt))

    section('Building unfiltered language model')
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

    section('Filtering language model')
    with open(vocabulary_txt, 'rb') as vocabulary_file:
        vocabulary_content = vocabulary_file.read()
    subprocess.run([
        KENLM_BIN + '/filter',
        'single',
        'model:' + unfiltered_arpa,
        filtered_arpa
    ], input=vocabulary_content, check=True)

    section('Building binary representation')
    subprocess.check_call([
        KENLM_BIN + '/build_binary',
        '-a', '255',
        '-q', '8',
        'trie',
        '-s',
        filtered_arpa,
        lm_binary
    ])


def parse_args():
    parser = argparse.ArgumentParser(description='Generate language models from OSCAR corpora', prog='genlm')
    parser.add_argument('language', choices=LANGUAGE_CODES,
                        help='language of the model to generate')
    parser.add_argument('--workers', type=int, default=os.cpu_count(),
                        help='number of preparation and counting workers')
    parser.add_argument('--simulate', action='store_true',
                        help='simulate language model generation with small amount of input data')
    parser.add_argument('--prune-factor', type=int, default=10,
                        help='times --vocabulary-size of items to keep in each vocabulary aggregator')
    parser.add_argument('--vocabulary-size', type=int, default=500000,
                        help='final number of words in vocabulary')

    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_args()
    LANG = get_language(ARGS.language)
    try:
        main()
    except KeyboardInterrupt:
        print('\nInterrupted')
        sys.exit()
