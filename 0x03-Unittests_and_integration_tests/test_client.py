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
    def _public_repos_url(self, mock_get_json):
        expected_repos = [
            {"name": "repo1", "license": {"key": "mit"}},
            {"name": "repo2", "license": {"key": "apache-2.0"}},
            {"name": "repo3"},
        ]
        mock_get_json.return_value = expected_repos
        with patch('client.GithubOrgClient._public_repos_url', new_callable=PropertyMock) as mock_repos_url:
            mock_repos_url.return_value = "https://api.github.com/orgs/google/repos"
            client = GithubOrgClient("google")
            repos = client.public_repos()
            self.assertEqual(repos, ["repo1", "repo2", "repo3"])
            mock_repos_url.assert_called_once()

if __name__ == "__main__":
    unittest.main()


        mock_get_json.assert_called_once_with(expected_url)


if __name__ == "__main__":
    unittest.main()
