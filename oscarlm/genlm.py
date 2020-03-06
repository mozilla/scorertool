
import os
import sys
import argparse
import subprocess

from collections import Counter
from multiprocessing import Process, Queue
from languages import LANGUAGE_CODES, get_language
from utils import maybe_download
from distutils.spawn import find_executable

STOP_TOKEN = False
NUM_WORKERS = 10
MAX_CHUNKS = 10 * NUM_WORKERS
TOP_WORDS = 500000
PRUNE_FACTOR = 10
LINES_PER_CHUNK = 100000
MAX_KEYS = 100000

KENLM_BIN = 'dependencies/kenlm/build/bin'
DEEPSPEECH_BIN = 'dependencies/deepspeech'


def count_words(cid, input_lines, resulting_lines, counters):
    counter = Counter()
    while True:
        lines = input_lines.get()
        if len(counter.keys()) > MAX_KEYS or lines == STOP_TOKEN:
            counters.put(counter)
            counter = Counter()
        if lines == STOP_TOKEN:
            return
        new_lines = []
        for line in lines:
            line_lower = line.lower()
            new_lines.append(line_lower)
            for w in line_lower.split():
                cw = ''
                for c in w:
                    c = str(c)
                    if c in LANG.alphabet:
                        cw += c
                if len(cw) > 0:
                    counter[cw] += 1
        resulting_lines.put(new_lines)


def aggregate_counters(vocab_filename, counters):
    overall_counter = Counter()
    while True:
        counter = counters.get()
        if counter == STOP_TOKEN:
            with open(sys.argv[1], 'w') as vocab_file:
                vocab_file.write('\n'.join(str(word) for word, count in overall_counter.most_common(TOP_WORDS)))
            return
        overall_counter += counter
        if len(overall_counter.keys()) > PRUNE_FACTOR * TOP_WORDS:
            overall_counter = Counter(overall_counter.most_common(TOP_WORDS))


def write_lines(prepared_txt_gz, resulting_lines):
    with open(prepared_txt_gz, 'wb') as archive_file:
        gzip = subprocess.Popen(['gzip'], stdin=subprocess.PIPE, stdout=archive_file)
        while True:
            lines = resulting_lines.get()
            if lines == STOP_TOKEN:
                return
            gzip.stdin.writelines(lines)


def main():

    raw_txt_gz = os.path.join(LANG.model_dir, 'raw.txt.gz')
    prepared_txt_gz = os.path.join(LANG.model_dir, 'prepared.txt.gz')
    vocab_filename = os.path.join(LANG.model_dir, 'vocabular.txt')

    maybe_download(LANG.text_url, raw_txt_gz)

    input_lines = Queue(MAX_CHUNKS)
    resulting_lines = Queue(MAX_CHUNKS)
    counters = Queue(NUM_WORKERS)

    writer_process = Process(target=write_lines, args=(prepared_txt_gz, resulting_lines,))
    writer_process.start()

    aggregator_process = Process(target=aggregate_counters, args=(vocab_filename, counters))
    aggregator_process.start()

    counter_processes = map(lambda index: Process(target=count_words,
                                                  args=(vocab_filename + '_' + str(index),
                                                        input_lines,
                                                        resulting_lines,
                                                        counters)),
                            range(NUM_WORKERS))
    for p in counter_processes:
        p.start()

    gunzip = subprocess.Popen(['gunzip'], stdin=open(raw_txt_gz, 'rb'), stdout=subprocess.PIPE)

    lines = []
    for line in iter(gunzip.stdout.readline, ''):
        lines.append(line)
        if len(lines) >= LINES_PER_CHUNK:
            input_lines.put(lines)
            lines = []
    input_lines.put(lines)

    for _ in counter_processes:
        input_lines.put(STOP_TOKEN)
    for p in counter_processes:
        p.join()

    counters.put(STOP_TOKEN)
    aggregator_process.join()

    resulting_lines.put(STOP_TOKEN)
    writer_process.join()

    subprocess.check_call([
        KENLM_BIN + '/lmplz',
        '--temp_prefix', tmp_prefix,
        '--memory', '25%',
        '--discount_fallback',
        '--text', clean_text_path,
        '--arpa', arpa_path,
        '--skip', 'symbols',
        '--order', '5',
        '--prune', '0', '0', '1'
    ])

    subprocess.check_call([
        KENLM_BIN + '/filter',
        'single',
        'model:' + arpa_path,
        filtered_arpa_path
    ])

    subprocess.check_call([
        KENLM_BIN + '/build_binary',
        '-a', '255',
        '-q', '8',
        'trie',
        '-s',
        filtered_arpa_path,
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
    parser.add_argument('--simulate', action='store_true',
                        help='simulate language model generation with small amount of input data')
    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_args()
    LANG = get_language(ARGS.language)
    print(LANG.model_dir)
    main()
