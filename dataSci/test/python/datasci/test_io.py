import datasci.io
import os
import unittest

HOME = os.getenv('HOME')

class TestIOFunctions(unittest.TestCase):
    def setUp(self):
        pass

    def test_data_io_load(self):
        folder = os.path.join(HOME, 'code/adServer/model-builder/test/python/files/test')
        data_io = datasci.io.DataIO(folder, nrows=1)
        df = data_io.load_data_frame()
        self.assertItemsEqual(df.columns, ['column_1', 'column_2', 'column_3'])

