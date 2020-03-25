
import sys
sys.path.insert(0, '../..')

import re
from languages import LanguageBase


class Language(LanguageBase):
    def __init__(self):
        super(Language, self).__init__(__file__)
        self.alphabet = ' abcdefghijklmnopqrstuvwxyz\'äöüß'
        self.substitutions = [
            (re.compile(r'\$'), 'dollar'),
            (re.compile(r'€'), 'euro'),
            (re.compile(r'£'), 'pfund')
        ]
        self.alpha = 0.931289039105002
        self.beta = 1.1834137581510284
