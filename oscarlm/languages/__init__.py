
import os
import importlib
import unicodedata
from glob import glob


def code_from_filename(filename):
    return os.path.splitext(os.path.split(filename)[1])[0]


FILE_DIR = os.path.dirname(__file__)
LANGUAGE_CODES = list(map(code_from_filename, glob(FILE_DIR + '/[!_]*.py')))
BASE_DIR = os.path.dirname(os.path.dirname(FILE_DIR))
MODELS_DIR = os.path.join(BASE_DIR, 'models')


class LanguageBase:
    def __init__(self, filename):
        self.code = code_from_filename(filename)
        self.alphabet = None
        self.model_dir = os.path.join(MODELS_DIR, self.code)
        self.text_url = 'https://traces1.inria.fr/oscar/files/Compressed/{}_dedup.txt.gz'.format(self.code)
        self.substitutions = []
        self.pre_filter = str.maketrans(dict.fromkeys('/()[]{}<>:'))
        self.simplify = True
        if not os.path.isdir(self.model_dir):
            os.mkdir(self.model_dir)

    def pre_clean(self, line):
        line = line.translate(self.pre_filter)
        return line.lower().strip()

    def clean(self, line):
        line = self.pre_clean(line)
        if len(line) == 0:
            return []
        for pattern, replacement in self.substitutions:
            if replacement is None:
                if pattern.match(line):
                    return []
            else:
                line = pattern.sub(replacement, line)
        chars = []
        for c in line:
            if self.simplify and c not in self.alphabet:
                c = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode("ascii", "ignore")
            for sc in c:
                if sc not in self.alphabet:
                    continue
                chars.append(sc)
        return [''.join(chars)]


def get_language(code):
    language_module = importlib.import_module('languages.' + code)
    return language_module.Language()
