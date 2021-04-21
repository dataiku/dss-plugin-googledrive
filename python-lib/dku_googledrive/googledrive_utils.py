import os
import string
from datetime import datetime
from dataikuapi.utils import DataikuException


class GoogleDriveUtils(object):
    MODIFIED_TIME = "modifiedTime"
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    API = "drive"
    API_VERSION = "v3"
    ROOT_ID = "root"
    NAME = "name"
    MIME_TYPE = "mimeType"
    PARENTS = "parents"
    SIZE = "size"
    ID_PARENTS_FIELDS = "id, parents"
    ID = "id"
    TRUE = "true"
    FALSE = "false"
    FOLDER = "application/vnd.google-apps.folder"
    SPREADSHEET = "application/vnd.google-apps.spreadsheet"
    GOOGLE_DOCUMENT = "application/vnd.google-apps.document"
    CSV = "text/csv"
    GOOGLE_APPS = "google-apps"
    BINARY_STREAM = "binary/octet-stream"
    LIST_FIELDS = "nextPageToken, files(id, name, size, parents, mimeType, createdTime, modifiedTime)"
    GOOGLE_DOC_MIME_EQUIVALENCE = {
        SPREADSHEET: CSV,
        GOOGLE_DOCUMENT: "text/plain",
        "application/vnd.google-apps.drawing": "image/svg+xml",
        "application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    }
    DEFAULT_MIME_TYPE = CSV

    @staticmethod
    def split_path(path_and_file):
        path, file = os.path.split(path_and_file)
        folders = []
        while 1:
            path, folder = os.path.split(path)
            if folder != '':
                folders.append(folder)
            else:
                if path != '':
                    folders.append(path)
                break
        folders.reverse()
        if file != "":
            folders.append(file)
        return folders

    @staticmethod
    def is_directory(file):
        return file['mimeType'] == GoogleDriveUtils.FOLDER

    @staticmethod
    def get_id(item):
        return item[GoogleDriveUtils.ID]

    @staticmethod
    def keep_files_with(items, name=None, name_starting_with=None):
        ret = []
        for item in items:
            if name_starting_with is not None:
                if GoogleDriveUtils.get_name(item).startswith(name_starting_with):
                    ret.append(item)
            if name is not None:
                if GoogleDriveUtils.get_name(item) == name:
                    ret.append(item)
        return ret

    # from http://helpful-nerd.com/2018/01/30/folder-and-directory-management-for-google-drive-using-python/
    @staticmethod
    def get_name(file):
        return file[GoogleDriveUtils.NAME]

    @staticmethod
    def is_file(file):
        return file[GoogleDriveUtils.MIME_TYPE] != GoogleDriveUtils.FOLDER

    @staticmethod
    def get_files_ids(files):
        parents = []
        for file in files:
            parents.append(GoogleDriveUtils.get_id(file))
        return GoogleDriveUtils.remove_duplicates(parents)

    @staticmethod
    def remove_duplicates(to_filter):
        return list(set(to_filter))

    @staticmethod
    def get_last_modified(item):
        if GoogleDriveUtils.MODIFIED_TIME in item:
            return int(GoogleDriveUtils.format_date(item[GoogleDriveUtils.MODIFIED_TIME]))

    @staticmethod
    def format_date(date):
        if date is not None:
            utc_time = datetime.strptime(date, GoogleDriveUtils.TIME_FORMAT)
            epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
            return int(epoch_time) * 1000
        else:
            return None

    @staticmethod
    def is_file_google_doc(file):
        return GoogleDriveUtils.GOOGLE_APPS in file[GoogleDriveUtils.MIME_TYPE]

    @staticmethod
    def get_google_doc_type(file):
        return file[GoogleDriveUtils.MIME_TYPE]

    @staticmethod
    def get_google_doc_mime_equivalence(gdoc_type):
        return GoogleDriveUtils.GOOGLE_DOC_MIME_EQUIVALENCE.get(gdoc_type, GoogleDriveUtils.DEFAULT_MIME_TYPE)

    @staticmethod
    def file_size(item):
        if GoogleDriveUtils.is_directory(item):
            return 0
        else:
            if GoogleDriveUtils.SIZE in item:
                return int(item[GoogleDriveUtils.SIZE])
            else:
                return 1  # have to lie to get DSS to read virtual files

    @staticmethod
    def check_path_format(path):
        special_names = [".", ".."]
        if not all(c in string.printable for c in path):
            raise DataikuException('The path contains non-printable char(s)')
        for element in path.split('/'):
            if len(element) > 1024:
                raise DataikuException('An element of the path is longer than the allowed 1024 characters')
            if element in special_names:
                raise DataikuException('Special name "{0}" is not allowed in a box.com path'.format(element))
            if element.endswith(' '):
                raise DataikuException('An element of the path contains a trailing space')
            if element.startswith('.well-known/acme-challenge'):
                raise DataikuException('An element of the path starts with ".well-known/acme-challenge"')

    @staticmethod
    def query_parents_in(parent_ids, name=None, name_contains=None, trashed=None):
        query = "("
        is_first = True
        for parent_id in parent_ids:
            if is_first:
                is_first = False
            else:
                query = query + " or "
            query = query + "'{}' in parents".format(parent_id)
        query = query + ")"
        if trashed is not None:
            query = query + ' and trashed=' + (GoogleDriveUtils.TRUE if trashed else GoogleDriveUtils.FALSE)
        if name is not None:
            query = query + " and name='" + name + "'"
        if name_contains is not None:
            query = query + " and name contains '" + name_contains + "'"
        return query

    @staticmethod
    def get_root_id(config):
        root_id = config.get("googledrive_root_id")
        if not root_id:
            root_id = GoogleDriveUtils.ROOT_ID
        return root_id
