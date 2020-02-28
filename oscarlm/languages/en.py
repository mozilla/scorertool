import sys
sys.path.insert(0, '../..')
from languages import LanguageBase


class Language(LanguageBase):
    def __init__(self):
        super(Language, self).__init__(__file__)
        self.alphabet = 'abcdefghijklmnopqrstuvwxyz\''
