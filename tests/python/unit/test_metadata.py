import pytest
from dku_googledrive.googledrive_utils import GoogleDriveUtils


class TestCommonMethods:

    def test_is_directory(self):
        not_a_directory = {'mimeType': "text/csv"}
        directory = {'mimeType': "application/vnd.google-apps.folder"}
        assert GoogleDriveUtils.is_directory(not_a_directory) == False
        assert GoogleDriveUtils.is_directory(directory) == True
