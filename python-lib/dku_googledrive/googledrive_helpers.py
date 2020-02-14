import os

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
