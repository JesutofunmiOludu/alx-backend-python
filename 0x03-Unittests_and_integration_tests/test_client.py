#!/usr/bin/env python3
#!/usr/bin/env python3
"""Unit tests for GithubOrgClient
"""
import unittest
from unittest.mock import patch
from parameterized import parameterized
from client import GithubOrgClient


class TestGithubOrgClient(unittest.TestCase):
    """Test cases for GithubOrgClient class"""

    @parameterized.expand([
        ("google",),
        ("abc",),
    ])
    @patch('client.get_json')
    def test_org(self, org_name, mock_get_json):
        """Test that GithubOrgClient.org returns the correct value
        
        Args:
            org_name: The organization name to test
            mock_get_json: Mocked get_json function
        """
        # Setup mock return value
        expected_org_data = {"login": org_name, "id": 12345}
        mock_get_json.return_value = expected_org_data
        
        # Create client instance
        client = GithubOrgClient(org_name)
        
        # Call the org method
        result = client.org()
        
        # Assertions
        self.assertEqual(result, expected_org_data)
        
        # Verify get_json was called once with the correct URL
        expected_url = f"https://api.github.com/orgs/{org_name}"
        mock_get_json.assert_called_once_with(expected_url)


if __name__ == "__main__":
    unittest.main()
