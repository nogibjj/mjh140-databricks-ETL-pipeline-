import os
import sys
import unittest
from unittest.mock import patch

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, parent_dir)

from mylib.sqlconn import sqlConnect

class TestSQLConnect(unittest.TestCase):

    @patch('databricks.sql.connect')
    def test_sqlConnect_successful(self, mock_connect):
        # Test a successful connection
        mock_connect.return_value.cursor.return_value = 'mock_cursor'
        result = sqlConnect()
        self.assertEqual(result[0], 'mock_cursor')

if __name__ == '__main__':
    unittest.main()