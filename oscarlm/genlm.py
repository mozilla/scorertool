
import os
import sys
import math
import struct
import shutil
import argparse
import subprocess

from collections import Counter
from multiprocessing import Process, Queue
from languages import LANGUAGE_CODES, get_language
from utils import maybe_download, maybe_ungzip, join_files, section, log_progress, announce, parse_file_size

STOP_TOKEN = False

SW_DIR = os.getenv('SW_DIR', 'dependencies')
KENLM_BIN = SW_DIR + '/kenlm/build/bin'
DEEPSPEECH_BIN = SW_DIR + '/deepspeech'


def get_partial_path(index):
    return os.path.join(LANG.model_dir, 'prepared.txt.partial{}'.format(index))


def count_words(index, counters):
    try:
        counter = Counter()
        unprepared_txt = os.path.join(LANG.model_dir, 'unprepared.txt')
        file_size = os.path.getsize(unprepared_txt)
        block_size = math.ceil(file_size / ARGS.workers)
        start = index * block_size
        end = min(file_size, start + block_size)
        with open(unprepared_txt, 'rb', buffering=ARGS.block_size) as unprepared_file, \
                open(get_partial_path(index), 'w', buffering=ARGS.block_size) as partial_file:
            pos = old_pos = start
            unprepared_file.seek(start)
            first = True
            while pos < end:
                line = unprepared_file.readline()
                pos = unprepared_file.tell()
                if index > 0 and first:
                    first = False
                    continue
                try:
                    line = line.decode()
                except UnicodeDecodeError:
                    continue
                lines = LANG.clean(line)
                for line in lines:
                    for word in line.split():
                        counter[word] += 1
                    partial_file.write(line + '\n')
                if len(counter.keys()) > ARGS.vocabulary_size or pos >= end:
                    counters.put((counter, pos - old_pos))
                    old_pos = pos
                    counter = Counter()
    except Exception as ex:
        announce('Shard worker {}: Error - {}'.format(index, ex))


def aggregate_counters(vocabulary_txt, source_bytes, counters):
    overall_counter = Counter()
    progress_indicator = log_progress(total=source_bytes, format='bytes')
    while True:
        counter_and_read_bytes = counters.get()
        if counter_and_read_bytes == STOP_TOKEN:
            with open(vocabulary_txt, 'w') as vocabulary_file:
                vocabulary_file.write('\n'.join(str(word) for word, count in overall_counter.most_common(ARGS.vocabulary_size)))
            progress_indicator.end()
            return
        counter, read_bytes = counter_and_read_bytes
        overall_counter += counter
        progress_indicator.increment(value_difference=read_bytes)
        if len(overall_counter.keys()) > ARGS.keep_factor * ARGS.vocabulary_size:
            overall_counter = Counter(overall_counter.most_common(ARGS.vocabulary_size))


def get_serialized_utf8_alphabet():
    res = bytearray()
    res += struct.pack('<h', 255)
    for i in range(255):
        # Note that we also shift back up in the mapping constructed here
        # so that the native client sees the correct byte values when decoding.
        res += struct.pack('<hh1s', i, 1, bytes([i+1]))
    return bytes(res)


def main():
    alphabet_txt = os.path.join(LANG.model_dir, 'alphabet.txt')
    raw_txt_gz = os.path.join(LANG.model_dir, 'raw.txt.gz')
    unprepared_txt = os.path.join(LANG.model_dir, 'unprepared.txt')
    prepared_txt = os.path.join(LANG.model_dir, 'prepared.txt')
    vocabulary_txt = os.path.join(LANG.model_dir, 'vocabulary.txt')
    unfiltered_arpa = os.path.join(LANG.model_dir, 'unfiltered.arpa')
    filtered_arpa = os.path.join(LANG.model_dir, 'filtered.arpa')
    lm_binary = os.path.join(LANG.model_dir, 'lm.binary')
    kenlm_scorer = os.path.join(LANG.model_dir, 'kenlm.scorer')
    temp_prefix = os.path.join(LANG.model_dir, 'tmp')

    section('Writing alphabet file', empty_lines_before=1)
    with open(alphabet_txt, 'w', encoding='utf-8') as alphabet_file:
        alphabet_file.write('\n'.join(LANG.alphabet) + '\n')

    redo = ARGS.force_download

    section('Downloading text data')
    redo = maybe_download(LANG.text_url, raw_txt_gz, force=redo)

    section('Unzipping text data')
    redo = maybe_ungzip(raw_txt_gz, unprepared_txt, force=redo)

    redo = redo or ARGS.force_prepare

    section('Preparing text and building vocabulary')
    if redo or not os.path.isfile(prepared_txt) or not os.path.isfile(vocabulary_txt):
        redo = True
        announce('Preparing {} shards of "{}"...'.format(ARGS.workers, unprepared_txt))
        counters = Queue(ARGS.workers)
        source_bytes = os.path.getsize(unprepared_txt)
        aggregator_process = Process(target=aggregate_counters, args=(vocabulary_txt, source_bytes, counters))
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
            print('')
            partials = list(map(lambda i: get_partial_path(i), range(ARGS.workers)))
            join_files(partials, prepared_txt)
            for partial in partials:
                os.unlink(partial)
        except KeyboardInterrupt:
            aggregator_process.terminate()
            for p in counter_processes:
                p.terminate()
            raise
    else:
        announce('Files "{}" and \n\t"{}" existing - not preparing'.format(prepared_txt, vocabulary_txt))

    redo = redo or ARGS.force_generate

    section('Building unfiltered language model')
    if redo or not os.path.isfile(unfiltered_arpa):
        redo = True
        lmplz_args = [
            KENLM_BIN + '/lmplz',
            '--temp_prefix', temp_prefix,
            '--memory', '80%',
            '--discount_fallback',
            '--limit_vocab_file', vocabulary_txt,
            '--text', prepared_txt,
            '--arpa', unfiltered_arpa,
            '--skip', 'symbols',
            '--order', str(LANG.order)
        ]
        if len(LANG.prune) > 0:
            lmplz_args.append('--prune')
            lmplz_args.extend(list(map(str, LANG.prune)))
        subprocess.check_call(lmplz_args)
    else:
        announce('File "{}" existing - not generating'.format(unfiltered_arpa))

    section('Filtering language model')
    if redo or not os.path.isfile(filtered_arpa):
        redo = True
        with open(vocabulary_txt, 'rb') as vocabulary_file:
            vocabulary_content = vocabulary_file.read()
        subprocess.run([
            KENLM_BIN + '/filter',
            'single',
            'model:' + unfiltered_arpa,
            filtered_arpa
        ], input=vocabulary_content, check=True)
    else:
        announce('File "{}" existing - not filtering'.format(filtered_arpa))

    section('Generating binary representation')
    if redo or not os.path.isfile(lm_binary):
        redo = True
        subprocess.check_call([
            KENLM_BIN + '/build_binary',
            '-a', '255',
            '-q', '8',
            '-v',
            'trie',
            filtered_arpa,
            lm_binary
        ])
    else:
        announce('File "{}" existing - not generating'.format(lm_binary))

    section('Building scorer')
    if redo or not os.path.isfile(kenlm_scorer):
        redo = True
        words = set()
        vocab_looks_char_based = True
        with open(vocabulary_txt) as vocabulary_file:
            for line in vocabulary_file:
                for word in line.split():
                    words.add(word.encode())
                    if len(word) > 1:
                        vocab_looks_char_based = False
        announce("{} unique words read from vocabulary file.".format(len(words)))
        announce(
            "{} like a character based model.".format(
                "Looks" if vocab_looks_char_based else "Doesn't look"
            )
        )
        if ARGS.alphabet_mode == 'auto':
            use_utf8 = vocab_looks_char_based
        elif ARGS.alphabet_mode == 'utf8':
            use_utf8 = True
        else:
            use_utf8 = False
        serialized_alphabet = get_serialized_utf8_alphabet() if use_utf8 else LANG.get_serialized_alphabet()
        from ds_ctcdecoder import Scorer, Alphabet
        alphabet = Alphabet()
        err = alphabet.deserialize(serialized_alphabet, len(serialized_alphabet))
        if err != 0:
            announce('Error loading alphabet: {}'.format(err))
            sys.exit(1)
        scorer = Scorer()
        scorer.set_alphabet(alphabet)
        scorer.set_utf8_mode(use_utf8)
        scorer.reset_params(LANG.alpha, LANG.beta)
        scorer.load_lm(lm_binary)
        scorer.fill_dictionary(list(words))
        shutil.copy(lm_binary, kenlm_scorer)
        scorer.save_dictionary(kenlm_scorer, True)  # append, not overwrite
        announce('Package created in {}'.format(kenlm_scorer))
        announce('Testing package...')
        scorer = Scorer()
        scorer.load_lm(kenlm_scorer)
    else:
        announce('File "{}" existing - not building'.format(kenlm_scorer))


def parse_args():
    parser = argparse.ArgumentParser(description='Generate language models from OSCAR corpora', prog='genlm')
    parser.add_argument('language', choices=LANGUAGE_CODES,
                        help='language of the model to generate')
    parser.add_argument('--workers', type=int, default=os.cpu_count(),
                        help='number of preparation and counting workers')
    parser.add_argument('--block-size', type=str, default='100M',
                        help='(maximum) preparation block size per worker to read at once during preparation')
    parser.add_argument('--vocabulary-size', type=int, default=500000,
                        help='final number of words in vocabulary')
    parser.add_argument('--keep-factor', type=int, default=10,
                        help='times --vocabulary-size of entries to keep after pruning in each vocabulary aggregator')
    parser.add_argument('--order', type=int,
                        help='overrides language-specific KenLM order')
    parser.add_argument('--prune', type=str,
                        help='overrides language-specific KenLM pruning - format: x:y:z:...')
    parser.add_argument('--alpha', type=float, default=None,
                        help='overrides language-specific alpha parameter')
    parser.add_argument('--beta', type=float, default=None,
                        help='overrides language-specific beta parameter')
    parser.add_argument('--alphabet-mode', choices=['auto', 'utf8', 'specific'], default='auto',
                        help='if alphabet-mode should be determined from the vocabulary (auto), '
                             'or the alphabet should be all utf-8 characters (utf8), '
                             'or the alphabet should be language specific (specific)')
    parser.add_argument('--force-download', action='store_true',
                        help='forces downloading, preparing and generating from scratch')
    parser.add_argument('--force-prepare', action='store_true',
                        help='forces preparing and generating from scratch (reusing available download)')
    parser.add_argument('--force-generate', action='store_true',
                        help='forces generating from scratch (reusing prepared data)')
    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_args()
    LANG = get_language(ARGS.language)
    if ARGS.order is not None:
        LANG.order = ARGS.order
    if ARGS.prune is not None:
        LANG.prune = list(map(int, ARGS.prune.split(':')))
    if ARGS.alpha is not None:
        LANG.alpha = ARGS.alpha
    if ARGS.beta is not None:
        LANG.beta = ARGS.beta
    ARGS.block_size = parse_file_size(ARGS.block_size)
    try:
        main()
    except KeyboardInterrupt:
        announce('\nInterrupted')
        sys.exit()
