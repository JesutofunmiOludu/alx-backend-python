#!/usr/bin/env python3
#!/usr/bin/env python3
import unittest
from parameterized import parameterized
from unittest.mock import patch, PropertyMock
import requests
from client import GithubOrgClient

class TestGithubOrgClient(unittest.TestCase):
    @parameterized.expand([
        ("google"),
        ("abc"),
    ])
    @patch('client.get_json')
    def test_org(self, org_name, mock_get_json):
        """Test org method"""
        mock_get_json.return_value = {"org": org_name}
        client = GithubOrgClient(org_name)
        self.assertEqual(client.org, {"org": org_name})
        mock_get_json.assert_called_once_with(
            f"https://api.github.com/orgs/{org_name}"
        )
    
    def test_public_repos_url(self):
        expected_repos_url = "https://api.github.com/orgs/google/repos"
        with patch('client.GithubOrgClient.org', new_callable=PropertyMock) as mock_org:
            mock_org.return_value = {
                "repos_url": expected_repos_url
            }
            client = GithubOrgClient("google")
            self.assertEqual(client._public_repos_url, expected_repos_url)
            mock_org.assert_called_once()
    
   @patch('client.get_json')
    def test_public_repos(self, mock_get_json):
        """Test that public_repos returns the expected list of repos
        
        Args:
            mock_get_json: Mocked get_json function
        """
        # Define the payload that get_json will return
        # This simulates the response from GitHub API for repos
        mock_repos_payload = [
            {"name": "repo1", "license": {"key": "mit"}},
            {"name": "repo2", "license": {"key": "apache-2.0"}},
            {"name": "repo3", "license": {"key": "mit"}},
        ]
        mock_get_json.return_value = mock_repos_payload
        
        # Use patch as context manager to mock _public_repos_url
        with patch('client.GithubOrgClient._public_repos_url',
                   new_callable=PropertyMock) as mock_public_repos_url:
            
            # Set the mocked URL
            mock_public_repos_url.return_value = "https://api.github.com/orgs/google/repos"
            
            # Create client instance
            client = GithubOrgClient("google")
            
            # Call public_repos
            result = client.public_repos()
            
            # Expected result: list of repo names
            expected_repos = ["repo1", "repo2", "repo3"]
            
            # Assertions
            self.assertEqual(result, expected_repos)
            
            # Verify _public_repos_url was accessed once
            mock_public_repos_url.assert_called_once()
            
            # Verify get_json was called once with the mocked URL
            mock_get_json.assert_called_once_with("https://api.github.com/orgs/google/repos")

if __name__ == "__main__":
    unittest.main()


        mock_get_json.assert_called_once_with(expected_url)


if __name__ == "__main__":
    unittest.main()
