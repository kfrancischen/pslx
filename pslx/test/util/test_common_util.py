import unittest

from pslx.schema.common_pb2 import Credentials
from pslx.util.common_util import CommonUtil


class CommonUtilTest(unittest.TestCase):

    def test_make_email_credentials(self):
        email_addr = "test@gmail.com"
        password = "test"
        credentials = CommonUtil.make_email_credentials(
            email_addr=email_addr,
            password=password
        )
        expected_credentials = Credentials()
        expected_credentials.user_name = email_addr
        expected_credentials.password = password
        expected_credentials.others['email_server'] = "smtp.gmail.com"
        expected_credentials.others['email_server_port'] = '25'
        self.assertEqual(credentials, expected_credentials)
