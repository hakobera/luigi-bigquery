import os

from apiclient.discovery import build
from apiclient.errors import HttpError
import httplib2

def get_gcs_client(project_id, credentials=None, service_account=None,
               private_key=None, private_key_file=None):

    if not credentials:
        assert service_account and (private_key or private_key_file), \
            'Must provide AssertionCredentials or service account and key'

    if private_key_file:
        with open(private_key_file, 'rb') as key_file:
            private_key = key_file.read()

    gcs_service = _get_gcs_service(credentials=credentials,
                                 service_account=service_account,
                                 private_key=private_key)

    return GCSClient(gcs_service)

def _get_gcs_service(credentials=None, service_account=None, private_key=None):

    assert credentials or (service_account and private_key), \
        'Must provide AssertionCredentials or service account and key'

    if not credentials:
        credentials = _credentials()(service_account, private_key, scope='https://www.googleapis.com/auth/devstorage.read_write')

    http = httplib2.Http()
    http = credentials.authorize(http)
    service = build('storage', 'v1', http=http)

    return service

def _credentials():
    from oauth2client.client import SignedJwtAssertionCredentials

    return SignedJwtAssertionCredentials

class GCSClient(object):

    def __init__(self, gcs_service):
        self.gcs = gcs_service

    def get_bucket(self, bucket_name):
        try:
            bucket = self.gcs.buckets().get(bucket=bucket_name).execute()
        except HttpError:
            bucket = {}

        return bucket

    def check_bucket(self, bucket_name):
        bucket = self.get_bucket(bucket_name)
        return bool(bucket)

    def get_file(self, bucket_name, path):
        try:
            file = self.gcs.objects().get(bucket=bucket_name, object=path).execute()
        except HttpError:
            file = {}

        return file

    def check_file(self, bucket_name, path):
        file = self.get_file(bucket_name, path)
        return bool(file)
