from luigi_bigquery import ResultProxy

import os
import shutil
import tempfile

class TestConfig(object):
    def __init__(self, tables=[], jobs=[]):
        self.tables = tables
        self._jobs = jobs
        self.tmp_dir = None
