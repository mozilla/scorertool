
import os
import importlib
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
        if not os.path.isdir(self.model_dir):
            os.mkdir(self.model_dir)


def get_language(code):
    language_module = importlib.import_module('languages.' + code)
    return language_module.Language()
