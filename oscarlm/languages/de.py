
import re
import sys
sys.path.insert(0, '../..')
from languages import LanguageBase


class Language(LanguageBase):
    def __init__(self):
        super(Language, self).__init__(__file__)
        self.alphabet = ' abcdefghijklmnopqrstuvwxyzäöüß'
        self.substitutions = [
            (re.compile(r'\$'), 'dollar'),
            (re.compile(r'€'), 'euro'),
            (re.compile(r'£'), 'pfund')
        ]
