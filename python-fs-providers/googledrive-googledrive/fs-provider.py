from dataiku.fsprovider import FSProvider

import os
import shutil
import re
import logging

from googleapiclient.errors import HttpError

from dku_googledrive.googledrive_helpers import GD, get_id, query_parents_in, get_name, is_file, is_directory
from dku_googledrive.googledrive_helpers import get_last_modified, file_size, get_root_id, check_path_format
from dss_constants import DSSConstants
from dku_googledrive.session import GoogleDriveSession

try:
    from BytesIO import BytesIO  # for Python 2
except ImportError:
    from io import BytesIO  # for Python 3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='googledrive plugin %(levelname)s - %(message)s')


class GoogleDriveFSProvider(FSProvider):
    def __init__(self, root, config, plugin_config):
        """
        :param root: the root path for this provider
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        if len(root) > 0 and root[0] == '/':
            root = root[1:]
        self.root = root
        self.provider_root = "/"
        check_path_format(self.get_normalized_path(self.root))
        self.root_id = get_root_id(config)
        self.session = GoogleDriveSession(config, plugin_config)

    # util methods
    def get_rel_path(self, path):
        if len(path) > 0 and path[0] == '/':
            path = path[1:]
        return path

    def get_normalized_path(self, path):
        if len(path) == 0 or path == '/':
            return '/'
        elts = path.split('/')
        elts = [e for e in elts if len(e) > 0]
        return '/' + '/'.join(elts)

    def get_full_path(self, path):
        path_elts = [self.provider_root, self.get_rel_path(self.root), self.get_rel_path(path)]
        path_elts = [e for e in path_elts if len(e) > 0]
        ret = os.path.join(*path_elts)
        return ret

    def get_root_path(self):
        path_elts = [self.provider_root, self.get_rel_path(self.root)]
        path_elts = [e for e in path_elts if len(e) > 0]
        return os.path.join(*path_elts)

    def close(self):
        """
        Perform any necessary cleanup
        """
        logger.info('closing googledrive session')

    def stat(self, path):
        """
        Get the info about the object at the given path inside the provider's root, or None
        if the object doesn't exist
        """
        full_path = self.get_full_path(path)
        logger.info('stat:path="{}", full_path="{}"'.format(path, full_path))

        item = self.session.get_item_from_path(full_path)

        if item is None:
            return None

        return {
            DSSConstants.PATH: self.get_normalized_path(path),
            DSSConstants.SIZE: file_size(item),
            DSSConstants.LAST_MODIFIED: get_last_modified(item),
            DSSConstants.IS_DIRECTORY: is_directory(item)
        }

    def set_last_modified(self, path, last_modified):
        """
        Set the modification time on the object denoted by path. Return False if not possible
        """
        return False

    def browse(self, path):
        """
        List the file or directory at the given path, and its children (if directory)
        """
        full_path = self.get_full_path(self.get_rel_path(path))
        logger.info('browse:path="{}", full_path="{}"'.format(path, full_path))

        item = self.session.get_item_from_path(full_path)

        if item is None:
            return {
                DSSConstants.FULL_PATH: None,
                DSSConstants.EXISTS: False
            }
        if is_file(item):
            return {
                DSSConstants.FULL_PATH: self.get_normalized_path(path),
                DSSConstants.EXISTS: True,
                DSSConstants.DIRECTORY: False,
                DSSConstants.SIZE: file_size(item),
                DSSConstants.LAST_MODIFIED: get_last_modified(item)
            }
        children = []

        files = self.session.directory(item, root_path=self.get_rel_path(full_path))
        for file in files:
            sub_path = self.get_normalized_path(os.path.join(path, get_name(file)))
            children.append({
                DSSConstants.FULL_PATH: sub_path,
                DSSConstants.EXISTS: True,
                DSSConstants.DIRECTORY: is_directory(file),
                DSSConstants.SIZE: file_size(file),
                DSSConstants.LAST_MODIFIED: get_last_modified(file)
            })
        return {
            DSSConstants.FULL_PATH: self.get_normalized_path(path),
            DSSConstants.EXISTS: True,
            DSSConstants.DIRECTORY: True,
            DSSConstants.CHILDREN: children,
            DSSConstants.LAST_MODIFIED: get_last_modified(item)
        }

    def enumerate(self, path, first_non_empty):
        """
        Enumerate files recursively from prefix. If first_non_empty, stop at the first non-empty file.

        If the prefix doesn't denote a file or folder, return None
        """
        full_path = self.get_full_path(path)
        logger.info('enumerate:path="{}", full_path="{}"'.format(path, full_path))

        item = self.session.get_item_from_path(full_path)

        if item is None:
            no_directory_item = self.session.get_item_from_path(self.get_root_path())
            query = query_parents_in(
                [get_id(no_directory_item)],
                name_contains=self.get_rel_path(path),
                trashed=False
            )
            files = self.session.googledrive_list(query)
            if len(files) == 0:
                return None
            paths = []
            for file in files:
                paths.append({
                    DSSConstants.PATH: self.get_normalized_path(get_name(file)),
                    DSSConstants.SIZE: file[GD.SIZE],
                    DSSConstants.LAST_MODIFIED: get_last_modified(file)
                })
            return paths

        if item is None:
            return None

        if is_file(item):
            return [{
                DSSConstants.PATH: self.get_normalized_path(path),
                DSSConstants.SIZE: file_size(item),
                DSSConstants.LAST_MODIFIED: get_last_modified(item)
            }]

        paths = []
        paths = self.list_recursive(path, item, first_non_empty)

        return paths

    def substract_path_base(self, base, path):
        return re.sub(r'^' + base + r'([a-zA-Z0-9\-_/\.]+)', r'\1', path)

    def list_recursive(self, path, folder, first_non_empty):
        paths = []
        if path == "/":
            path = ""
        children = self.session.directory(folder, root_path=self.get_rel_path(path))
        for child in children:
            if is_directory(child):
                paths.extend(self.list_recursive(path + '/' + get_name(child), child, first_non_empty))
            else:
                paths.append({
                    DSSConstants.PATH: path + '/' + get_name(child),
                    DSSConstants.SIZE: file_size(child),
                    DSSConstants.LAST_MODIFIED: get_last_modified(child)
                })
                if first_non_empty:
                    return paths
        return paths

    def delete_recursive(self, path):
        """
        Delete recursively from path. Return the number of deleted files (optional)
        """
        full_path = self.get_full_path(path)
        logger.info('delete_recursive:path="{}", full_path="{}"'.format(path, full_path))
        self.assert_path_is_not_root(full_path)
        deleted_item_count = 0

        folder = self.session.get_item_from_path(full_path)

        if is_directory(folder):

            if folder is None or GD.PARENTS not in folder:
                return deleted_item_count
            else:
                query = query_parents_in([get_id(folder)])
                items = self.session.googledrive_list(query)
                for item in items:
                    self.session.googledrive_delete(item, parent_id=get_id(folder))
                    deleted_item_count = deleted_item_count + 1
        else:
            self.session.googledrive_delete(folder)
            deleted_item_count = deleted_item_count + 1

        return deleted_item_count

    def move(self, from_path, to_path):
        """
        Move a file or folder to a new path inside the provider's root. Return false if the moved file didn't exist
        """
        full_from_path = self.get_full_path(from_path)
        full_to_path = self.get_full_path(to_path)
        from_name = os.path.basename(from_path)
        to_name = os.path.basename(to_path)
        logger.info('move:from "{}" to "{}"'.format(full_from_path, full_to_path))

        try:
            from_item = self.session.get_item_from_path(full_from_path)
            if from_item is None:
                return False

            if from_name == to_name:
                to_item = self.session.get_item_from_path(os.path.split(full_to_path)[0])

                prev_parents = ','.join(p for p in from_item.get(GD.PARENTS))
                self.drive.files().update(
                    fileId=get_id(from_item),
                    addParents=get_id(to_item),
                    removeParents=prev_parents,
                    fields=GD.ID_PARENTS_FIELDS,
                ).execute()
            else:
                file = self.drive.files().get(fileId=get_id(from_item)).execute()
                del file[GD.ID]
                file[GD.NAME] = to_name
                self.drive.files().update(
                    fileId=get_id(from_item),
                    body=file,
                    fields=GD.ID_PARENTS_FIELDS,
                ).execute()
        except HttpError as err:
            raise Exception('Error from Google Drive while moving files: ' + err)

        return True

    def read(self, path, stream, limit):
        """
        Read the object denoted by path into the stream. Limit is an optional bound on the number of bytes to send
        """
        full_path = self.get_full_path(path)
        logger.info('read:path="{}", full_path="{}"'.format(path, full_path))
        item = self.session.get_item_from_path(full_path)

        if item is None:
            raise Exception('Path doesn t exist')

        self.session.googledrive_download(item, stream)

    def write(self, path, stream):
        """
        Write the stream to the object denoted by path into the stream
        """
        full_path = self.get_full_path(path)
        logger.info('write:path="{}", full_path="{}"'.format(path, full_path))

        bio = BytesIO()
        shutil.copyfileobj(stream, bio)
        bio.seek(0)
        base_path, file_name = os.path.split(full_path)
        directory_id = self.session.create_directory_from_path(base_path)
        self.session.googledrive_upload(file_name, bio, parent_id=directory_id)

    def assert_path_is_not_root(self, path):
        black_list = [None, "", "root"]
        if self.root_id in black_list and path is None or path.strip("/") == "":
            logger.error("Will not delete root directory. root_id={}, path={}".format(self.root_id, path))
            raise Exception("Cannot delete root path")
