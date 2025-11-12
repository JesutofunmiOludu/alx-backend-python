#!/usr/bin/env python3
import unittest
from parameterized import parameterized
from utils import access_nested_map, get_json
from unittest.mock import patch
import requests


class TestAccessNestedMap(unittest.TestCase):
    @parameterized.expand([
        ({"a": 1}, ("a",), 1),
        ({"a": {"b": 2}}, ("a",), {"b": 2}),
        ({"a": {"b": 2}}, ("a", "b"), 2),
    ])
    def test_access_nested_map(self,nested_map, path, expected):
        self.assertEqual(access_nested_map(nested_map, path), expected)
    @parameterized.expand([
        ({}, ("a",), "a"),
        ({"a": 1}, ("a", "b"), "b"),
    ])
    
    def test_access_nested_map_exception(self,nested_map, path, expected_key):
        with self.assertRaises(KeyError) as context:
            access_nested_map(nested_map, path)
        self.assertEqual(context.exception.args[0], expected_key)

class TestGetJson(unittest.TestCase):
    @parameterized.expand([ 
        ("http://example.com", {"payload": True}),
        ("http://holberton.io", {"payload": False}),
    ])
    @patch('utils.requests.get')
    def test_get_json(self, url, expected_payload, mock_get):
        mock_response = unittest.mock.Mock()
        mock_response.json.return_value = expected_payload
        mock_get.return_value = mock_response

        result = get_json(url)
        self.assertEqual(result, expected_payload)
        mock_get.assert_called_once_with(url)

if __name__ == "__main__":
    unittest.main()
