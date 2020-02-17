import os, string
from datetime import datetime

GD_MODIFIED_TIME = "modifiedTime"
GD_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
GD_API = "drive"
GD_API_VERSION = "v3"
GD_ROOT_ID = "root"
GD_NAME = "name"
GD_MIME_TYPE = "mimeType"
GD_PARENTS = "parents"
GD_SIZE = "size"
GD_ID_PARENTS_FIELDS = "id, parents"
GD_ID = "id"
GD_TRUE = "true"
GD_FALSE = "false"
GD_FOLDER = "application/vnd.google-apps.folder"
GD_SPREADSHEET = "application/vnd.google-apps.spreadsheet"
GD_CSV = "text/csv"
GD_GOOGLE_APPS = "google-apps"
GD_BINARY_STREAM = "binary/octet-stream"
GD_LIST_FIELDS = "nextPageToken, files(id, name, size, parents, mimeType, createdTime, modifiedTime)"

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

def is_directory(file):
    return file['mimeType'] == "application/vnd.google-apps.folder"

def get_id(item):
        return item[GD_ID]

def keep_files_with(items, name=None, name_starting_with=None):
    ret = []
    for item in items:
        if name_starting_with is not None:
            if get_name(item).startswith(name_starting_with):
                ret.append(item)
        if name is not None:
            if get_name(item) == name:
                ret.append(item)
    return ret

# from http://helpful-nerd.com/2018/01/30/folder-and-directory-management-for-google-drive-using-python/
def get_name(file):
    return file[GD_NAME]

def is_file(file):
    return file[GD_MIME_TYPE] != GD_FOLDER

def get_files_ids(files):
    parents = []
    for file in files:
        parents.append(get_id(file))
    return remove_duplicates(parents)

def remove_duplicates(to_filter):
    return list(set(to_filter))

def get_last_modified(item):
    if GD_MODIFIED_TIME in item:
        return int(format_date(item[GD_MODIFIED_TIME]))

def format_date(date):
    if date is not None:
        utc_time = datetime.strptime(date, GD_TIME_FORMAT)
        epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
        return int(epoch_time) * 1000
    else:
        return None

def is_file_google_doc(file):
    return GD_GOOGLE_APPS in file[GD_MIME_TYPE]

def file_size(item):
    if is_directory(item):
        return 0
    else:
        if GD_SIZE in item:
            return int(item[GD_SIZE])
        else:
            return 1 # have to lie to get DSS to read virtual files

def check_path_format(path):
    special_names = [".",".."]
    if not all(c in string.printable for c in path):
        raise Exception('The path contains non-printable char(s)')
    for element in path.split('/'):
        if len(element) > 1024:
            raise Exception('An element of the path is longer than the allowed 1024 characters')
        if element in special_names:
            raise Exception('Special name "{0}" is not allowed in a box.com path'.format(element))
        if element.endswith(' '):
            raise Exception('An element of the path contains a trailing space')
        if element.startswith('.well-known/acme-challenge'):
            raise Exception('An element of the path starts with ".well-known/acme-challenge"')

def query_parents_in(parent_ids, name = None, name_contains = None, trashed = None):
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
        query = query + ' and trashed=' + (GD_TRUE if trashed else GD_FALSE)
    if name is not None:
        query = query + " and name='" + name + "'"
    if name_contains is not None:
        query = query + " and name contains '" + name_contains + "'"
    return query

def get_root_id(config):
    root_id = config.get("googledrive_root_id")
    if root_id is None:
        root_id = GD_ROOT_ID
    return root_id