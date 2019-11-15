#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataiku.fsprovider import FSProvider

import os, shutil, re, logging, json

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
from urllib2 import HTTPError
from apiclient import errors

from mimetypes import MimeTypes
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError
from random import randrange
from time import sleep
from datetime import datetime

from StringIO import StringIO ## for Python 2
try:
    from BytesIO import BytesIO ## for Python 2
except ImportError:
    from io import BytesIO ## for Python 3

try:
    from FileIO import FileIO ## for Python 2
except ImportError:
    from io import FileIO ## for Python 3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='confluence plugin %(levelname)s - %(message)s')

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
        #self.scopes = ['https://www.googleapis.com/auth/drive.readonly']
        scopes = ['https://www.googleapis.com/auth/drive']
        connection = plugin_config.get("googledrive_connection")
        self.write_as_google_doc = config.get("googledrive_write_as_google_doc")
        self.nodir_mode = config.get("googledrive_nodir_mode")
        self.root_id = config.get("googledrive_root_id")
        if self.root_id is None:
            self.root_id = 'root'
        self.max_attempts = 5
        credentials_dict = eval(connection['credentials'])
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scopes)
        http_auth = credentials.authorize(Http())
        self.drive = build('drive', 'v3', http=http_auth)

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

        item = self.get_item_from_path(full_path)

        if item is None:
            return None

        return {'path': self.get_normalized_path(path), 'size': self.file_size(item), 'lastModified': self.get_last_modified(item), 'isDirectory': self.is_directory(item)}

    def get_last_modified(self, item):
        if "modifiedTime" in item:
            return int(self.format_date(item["modifiedTime"]))
        return

    def format_date(self, date):
        if date is not None:
            utc_time = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
            epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
            return int(epoch_time) * 1000
        else:
            return None

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

        item = self.get_item_from_path(full_path)

        if item is None:
            return {'fullPath' : None, 'exists' : False}
        if self.is_file(item):
            return {'fullPath' : self.get_normalized_path(path), 'exists' : True, 'directory' : False, 'size' : self.file_size(item), 'lasModified' : self.get_last_modified(item)}
        children = []

        files = self.directory(item, root_path=self.get_rel_path(full_path))
        for file in files:
            sub_path = self.get_normalized_path(os.path.join(path, self.get_name(file)))
            children.append({
                'fullPath' : sub_path,
                'exists' : True,
                'directory' : self.is_directory(file),
                'size' : self.file_size(file),
                'lastModified' : self.get_last_modified(file)
            })
        
        return {'fullPath' : self.get_normalized_path(path), 'exists' : True, 'directory' : True, 'children' : children, 'lasModified' : self.get_last_modified(item)}

    # from http://helpful-nerd.com/2018/01/30/folder-and-directory-management-for-google-drive-using-python/

    def split_path(self, path_and_file):
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

    def file_size(self, item):
        if self.is_directory(item):
            return 0
        else:
            if 'size' in item:
                return int(item['size'])
            else:
                return 1 # have to lie to get DSS to read virtual files

    def get_name(self, file):
        return file['name']

    def is_directory(self, file):
        return file['mimeType'] == "application/vnd.google-apps.folder"

    def is_file(self, file):
        return file['mimeType'] != "application/vnd.google-apps.folder"

    def get_item_from_path(self, path_and_file):
        tokens = self.split_path(path_and_file)
        if len(tokens) == 1:
            return {u'mimeType': u'application/vnd.google-apps.folder', u'size': u'0', u'id': self.root_id, u'name': u'/'}
        parent_ids = [self.root_id]
        
        for token in tokens:
            if token == '/':
                token = ''
                continue

            query = self.query_parents_in(parent_ids, name_contains = token, trashed = False)
            files = self.googledrive_list(query)
            files = self.keep_files_with(files, name_starting_with=token)
            files = self.keep_files_with(files, name=token) # we only keep files / parent_ids for names = current token for the next loop
            
            if len(files) == 0:
                return None
            parent_ids = self.get_files_ids(files)
        if len(files)>1:
            raise Exception("There are several files with this path.")
        return files[0]

    def directory(self, item, root_path = None):

        query = self.query_parents_in([self.get_id(item)], trashed = False)
        files = self.googledrive_list(query)

        return files

    def keep_files_with(self, items, name=None, name_starting_with=None):
        ret = []
        for item in items:
            if name_starting_with is not None:
                if self.get_name(item).startswith(name_starting_with):
                    ret.append(item)
            if name is not None:
                if self.get_name(item) == name:
                    ret.append(item)
        return ret

    def query_parents_in(self, parent_ids, name = None, name_contains = None, trashed = None):
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
            query = query + ' and trashed=' + ("true" if trashed else "false")
        if name is not None:
            query = query + " and name='" + name + "'"
        if name_contains is not None:
            query = query + " and name contains '" + name_contains + "'"
        return query

    def get_files_ids(self, files):
        parents = []
        for file in files:
            parents.append(self.get_id(file))
        return self.remove_duplicates(parents)

    def remove_duplicates(self, to_filter):
        return list(set(to_filter))
        
    def create_directory_from_path(self, path):
        tokens = self.split_path(path)

        parent_ids = [self.root_id]
        current_path = ""

        for token in tokens:
            current_path = os.path.join(current_path, token)
            item = self.get_item_from_path(current_path)
            if item is None:
                new_directory_id = self.create_directory(token, parent_ids)
                parent_ids = [new_directory_id]
            else:
                new_directory_id = self.get_id(item)
                parent_ids = [new_directory_id]
        return new_directory_id

    def create_directory(self, name, parent_ids):
        file_metadata = {
            'name': name,
            'parents': parent_ids,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        file = self.googledrive_create(body=file_metadata)
        return self.get_id(file)

    def enumerate(self, path, first_non_empty):
        """
        Enumerate files recursively from prefix. If first_non_empty, stop at the first non-empty file.
        
        If the prefix doesn't denote a file or folder, return None
        """
        full_path = self.get_full_path(path)

        item = self.get_item_from_path(full_path)
        
        if item is None:
            no_directory_item = self.get_item_from_path(self.get_root_path())
            query = self.query_parents_in([self.get_id(no_directory_item)], name_contains = self.get_rel_path(path), trashed = False)
            files = self.googledrive_list(query)
            if len(files) == 0:
                return None
            paths = []
            for file in files:
                paths.append({'path':self.get_normalized_path(self.get_name(file)), 'size':file['size'], 'lastModified':self.get_last_modified(file)})
            if len(files) > 0:
                item = {u'mimeType': u'application/vnd.google-apps.folder', u'size': u'0', u'id': full_path, u'name': u'root'}
            return paths

        if item is None:
            return None

        if self.is_file(item):
            return [{'path':self.get_normalized_path(path), 'size':self.file_size(item), 'lastModified':self.get_last_modified(item)}]

        paths = []
        paths = self.list_recursive(path, item, first_non_empty)

        return paths

    def substract_path_base(self, base, path):
        return re.sub(r'^' + base + r'([a-zA-Z0-9\-_/\.]+)', r'\1', path)

    def list_recursive(self, path, folder, first_non_empty):
        paths = []
        if path == "/":
            path = ""
        children = self.directory(folder, root_path = self.get_rel_path(path))
        for child in children:
            if self.is_directory(child):
                paths.extend(self.list_recursive(path + '/' + self.get_name(child), child, first_non_empty))
            else:
                paths.append({'path':path + '/' + self.get_name(child), 'size':self.file_size(child), 'lastModified':self.get_last_modified(child)})
                if first_non_empty:
                    return paths
        return paths

    def delete_recursive(self, path):
        """
        Delete recursively from path. Return the number of deleted files (optional)
        """
        full_path = self.get_full_path(path)
        deleted_item_count = 0

        folder = self.get_item_from_path(full_path)
        
        if self.is_directory(folder):
            
            if folder is None or "parents" not in folder:
                return deleted_item_count
            else:
                query = self.query_parents_in([self.get_id(folder)])
                items = self.googledrive_list(query)
                for item in items:
                    self.googledrive_delete(item, parent_id = self.get_id(folder))
                    deleted_item_count = deleted_item_count + 1
        else:
            self.googledrive_delete(folder)
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

        try:
            from_item = self.get_item_from_path(full_from_path)
            if from_item is None:
                return False

            if from_name == to_name:
                to_item = self.get_item_from_path(os.path.split(full_to_path)[0])

                prev_parents = ','.join(p for p in from_item.get('parents'))
                self.drive.files().update( fileId = self.get_id(from_item),
                        addParents = self.get_id(to_item),
                        removeParents = prev_parents,
                        fields = 'id, parents',
                        ).execute()
            else:
                file = self.drive.files().get(fileId=self.get_id(from_item)).execute()
                del file['id']
                file['name'] = to_name
                self.drive.files().update(fileId = self.get_id(from_item),
                        body = file,
                        fields = 'id, parents',
                        ).execute()
        except HttpError as err:
            raise Exception('Error from Google Drive while moving files: ' + err)

        return True

    def read(self, path, stream, limit):
        """
        Read the object denoted by path into the stream. Limit is an optional bound on the number of bytes to send
        """
        full_path = self.get_full_path(path)
        
        item = self.get_item_from_path(full_path)

        if item is None:
            raise Exception('Path doesn t exist')

        data = self.googledrive_download(item, stream)

    def googledrive_download(self, item, stream):
        if self.is_file_google_doc(item):
            data =  self.drive.files().export_media(fileId = self.get_id(item), mimeType = "text/csv").execute()
            file_handle = BytesIO()
            file_handle.write(data)
            file_handle.seek(0)
            shutil.copyfileobj(file_handle, stream)
        else:
            request =  self.drive.files().get_media(fileId = self.get_id(item))
            downloader = MediaIoBaseDownload(stream, request, chunksize=1024*1024)
            done = False
            while done is False:
                status, done = downloader.next_chunk()


    def is_file_google_doc(self, file):
        return "google-apps" in file['mimeType']

    def write(self, path, stream):
        """
        Write the stream to the object denoted by path into the stream
        """
        full_path = self.get_full_path(path)
        file_name = self.get_rel_path(path)

        folder = self.get_item_from_path(self.get_root_path())
        bio = BytesIO()
        shutil.copyfileobj(stream, bio)
        bio.seek(0)
        if self.nodir_mode:
            self.googledrive_upload(file_name, bio, parent_id = self.get_id(folder))
        else:
            base_path, file_name = os.path.split(full_path)
            directory_id = self.create_directory_from_path(base_path)
            self.googledrive_upload(file_name, bio, parent_id = directory_id)

    def get_id(self, item):
        return item['id']

    def googledrive_upload(self, filename, file_handle, parent_id=None):
        mime = MimeTypes()
        guessed_type = mime.guess_type(filename)[0]

        file_metadata = {
            'name': filename
        }
        if self.write_as_google_doc and guessed_type == "text/csv":
            file_metadata['mimeType'] = 'application/vnd.google-apps.spreadsheet'

        if guessed_type is None:
            guessed_type = "binary/octet-stream"

        media = MediaIoBaseUpload(file_handle,
                                mimetype=guessed_type,
                                resumable=True)

        query = self.query_parents_in([parent_id], name = filename, trashed = False)
        files = self.googledrive_list(query)

        if len(files) == 0:
            if parent_id:
                file_metadata['parents'] = [parent_id]

            file = self.googledrive_create(body=file_metadata,
                                    media_body=media)
        else:
            file = self.googledrive_update(file_id=self.get_id(files[0]),
                                    body=file_metadata,
                                    media_body=media)

    def googledrive_list(self, query):
        fields = "nextPageToken, files(id, name, size, parents, mimeType, createdTime, modifiedTime)"

        attempts = 0
        while attempts < self.max_attempts:
            try:
                request = self.drive.files().list(q=query, fields=fields).execute()
                files = request.get('files', [])
                return files
            except HttpError as err:
                self.handle_googledrive_errors(err, "list")
            attempts = attempts + 1
            logger.info('googledrive_list:attempts={}'.format(attempts))
        raise Exception("Max number of attempts reached in Google Drive directory list operation")

    def googledrive_create(self, body, media_body = None):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                file = self.drive.files().create(body=body,
                                    media_body=media_body,
                                    fields='id').execute()
                return file
            except HttpError as err:
                self.handle_googledrive_errors(err, "create")
            attempts = attempts + 1
            logger.info('googledrive_create:attempts={}'.format(attempts))
        raise Exception("Max number of attempts reached in Google Drive directory create operation")

    def googledrive_update(self, file_id, body, media_body = None):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                file = self.drive.files().update(fileId=file_id,
                                    body=body,
                                    media_body=media_body,
                                    fields='id').execute()
                return file
            except HttpError as err:
                self.handle_googledrive_errors(err, "update")
            attempts = attempts + 1
            logger.info('googledrive_update:attempts={}'.format(attempts))
        raise Exception("Max number of attempts reached in Google Drive directory update operation")

    def googledrive_delete(self, item, parent_id = None):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                if len(item['parents']) == 1 or parent_id is None:
                    self.drive.files().delete(fileId=self.get_id(item)).execute()
                else:
                    self.drive.files().update(fileId=self.get_id(item), removeParents=parent_id).execute()
            except HttpError as err:
                if err.resp.status == 404:
                    return
                self.handle_googledrive_errors(err, "delete")
            attempts = attempts + 1
            logger.info('googledrive_update:attempts={}'.format(attempts))
        raise Exception("Max number of attempts reached in Google Drive directory delete operation")

    def handle_googledrive_errors(self, err, context = ""):
        if err.resp.status in [403, 500, 503]:
            sleep( 5 + randrange(5))
        else:
            reason = ""
            if err.resp.get('content-type', '').startswith('application/json'):
                reason = json.loads(err.content).get('error').get('errors')[0].get('reason')
            raise Exception("Googledrive " + context + " error : " + reason)
