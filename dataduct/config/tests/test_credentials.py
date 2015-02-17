"""Tests for credentials file
"""
from mock import patch
from nose.tools import eq_
import json

from ..credentials import get_aws_credentials_from_iam

@patch("requests.get")
def test_get_aws_credentials_from_iam(patched_requests_get):
    """Test for get credentials from IAM
    """
    class MockedReturn:
        """Mock request response
        """
        def __init__(self, content):
            self.content = content
            self.ok = True

        def json(self):
            """Returns a json for the content
            """
            return json.loads(self.content)

    def server_response(url):
        """Mocked server responses
        """
        if url == "http://169.254.169.254/latest/meta-data/iam/security-credentials/":  # NOQA
            return MockedReturn("role")
        if url == "http://169.254.169.254/latest/meta-data/iam/security-credentials/role":  # NOQA
            return MockedReturn("""
            {
                "Code" : "Success",
                "LastUpdated" : "2012-04-26T16:39:16Z",
                "Type" : "AWS-HMAC",
                "AccessKeyId" : "access_id",
                "SecretAccessKey" : "secret_key",
                "Token" : "token",
                "Expiration" : "2012-04-27T22:39:16Z"
            }
            """)

    patched_requests_get.side_effect = server_response
    access_id, secret_key, token = get_aws_credentials_from_iam()
    eq_(access_id, "access_id")
    eq_(secret_key, "secret_key")
    eq_(token, "token")
