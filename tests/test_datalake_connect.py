import os
import sys
import unittest
from unittest.mock import patch, MagicMock

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, parent_dir)

from mylib.DLconn import datalake_connect

class Test_DataLake_Connect(unittest.TestCase):

    @patch('mylib.DLconn.DataLakeServiceClient')  # Mock the DataLakeServiceClient
    def test_successful_connection(self, mock_service_client):
        # Mock the methods of DataLakeServiceClient
        mock_service = MagicMock()
        mock_service.list_file_systems.return_value = []
        mock_service.get_file_system_client.return_value.get_paths.return_value = []
        mock_service_client.return_value = mock_service

        file_system_client, status = datalake_connect()

        self.assertIsNotNone(file_system_client)
        self.assertEqual(status, "Success")
        mock_service.list_file_systems.assert_called_once()
        mock_service.get_file_system_client.assert_called_once()

if __name__ == '__main__':
    unittest.main()