import os, sys

## Add stuff to the path to enable exec outside of DSS
plugin_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.join(plugin_root, 'python-lib'))

from dku_googledrive.googledrive_helpers import is_directory

def test_is_directory():
    not_a_directory = {'mimeType' : "text/csv"}
    directory = {'mimeType' : "application/vnd.google-apps.folder"}
    assert is_directory(not_a_directory) == False
    assert is_directory(directory) == True
