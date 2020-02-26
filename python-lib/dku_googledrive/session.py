import logging
import shutil
import os
import json
from random import randrange

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import AccessTokenCredentials
from httplib2 import Http
from mimetypes import MimeTypes
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError
from dku_googledrive.googledrive_helpers import GD, get_id, get_root_id, get_files_ids, split_path
from dku_googledrive.googledrive_helpers import query_parents_in, keep_files_with, is_file_google_doc
from time import sleep

try:
    from BytesIO import BytesIO  # for Python 2
except ImportError:
    from io import BytesIO  # for Python 3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='googledrive plugin %(levelname)s - %(message)s')


class GoogleDriveSession():
    def __init__(self, config, plugin_config):
        scopes = ['https://www.googleapis.com/auth/drive']
        connection = plugin_config.get("googledrive_connection")
        self.auth_type = config.get("auth_type")
        self.write_as_google_doc = config.get("googledrive_write_as_google_doc")
        self.nodir_mode = False  # Future development

        if self.auth_type == "oauth":
            self.access_token = config.get("oauth_credentials")["access_token"]
            credentials = AccessTokenCredentials(self.access_token, "dss-googledrive-plugin/2.0")
            http_auth = credentials.authorize(Http())
        else:
            credentials_dict = eval(connection['credentials'])
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scopes)
            http_auth = credentials.authorize(Http())
        self.root_id = config.get("googledrive_root_id")
        if self.root_id is None:
            self.root_id = GD.ROOT_ID
        self.max_attempts = 5
        self.root_id = get_root_id(config)
        self.drive = build(GD.API, GD.API_VERSION, http=http_auth)

    def get_item_from_path(self, path_and_file):
        tokens = split_path(path_and_file)
        if len(tokens) == 1:
            return {
                GD.MIME_TYPE: GD.FOLDER,
                GD.SIZE: u'0',
                GD.ID: self.root_id,
                GD.NAME: u'/'
            }
        parent_ids = [self.root_id]

        for token in tokens:
            if token == '/':
                token = ''
                continue

            query = query_parents_in(parent_ids, name_contains=token, trashed=False)
            files = self.googledrive_list(query)
            files = keep_files_with(files, name_starting_with=token)
            files = keep_files_with(files, name=token)  # we only keep files / parent_ids for names = current token for the next loop

            if len(files) == 0:
                return None
            parent_ids = get_files_ids(files)
        return files[0]

    def googledrive_download(self, item, stream):
        if is_file_google_doc(item):
            data = self.drive.files().export_media(fileId=get_id(item), mimeType=GD.CSV).execute()
            file_handle = BytesIO()
            file_handle.write(data)
            file_handle.seek(0)
            shutil.copyfileobj(file_handle, stream)
        else:
            request = self.drive.files().get_media(fileId=get_id(item))
            downloader = MediaIoBaseDownload(stream, request, chunksize=1024*1024)
            done = False
            while done is False:
                status, done = downloader.next_chunk()

    def directory(self, item, root_path=None):
        query = query_parents_in([get_id(item)], trashed=False)
        files = self.googledrive_list(query)
        return files

    def googledrive_list(self, query):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                request = self.drive.files().list(q=query, fields=GD.LIST_FIELDS).execute()
                files = request.get('files', [])
                return files
            except HttpError as err:
                self.handle_googledrive_errors(err, "list")
            attempts = attempts + 1
            logger.info('googledrive_list:attempts={}'.format(attempts))
        raise Exception("Max number of attempts reached in Google Drive directory list operation")

    def create_directory_from_path(self, path):
        tokens = split_path(path)

        parent_ids = [self.root_id]
        current_path = ""

        for token in tokens:
            current_path = os.path.join(current_path, token)
            item = self.get_item_from_path(current_path)
            if item is None:
                new_directory_id = self.create_directory(token, parent_ids)
                parent_ids = [new_directory_id]
            else:
                new_directory_id = get_id(item)
                parent_ids = [new_directory_id]
        return new_directory_id

    def create_directory(self, name, parent_ids):
        file_metadata = {
            GD.NAME: name,
            GD.PARENTS: parent_ids,
            GD.MIME_TYPE: GD.FOLDER
        }
        file = self.googledrive_create(body=file_metadata)
        return get_id(file)

    def googledrive_create(self, body, media_body=None):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                file = self.drive.files().create(
                    body=body,
                    media_body=media_body,
                    fields=GD.ID
                ).execute()
                return file
            except HttpError as err:
                self.handle_googledrive_errors(err, "create")
            attempts = attempts + 1
            logger.info('googledrive_create:attempts={}'.format(attempts))
        raise Exception("Max number of attempts reached in Google Drive directory create operation")

    def googledrive_upload(self, filename, file_handle, parent_id=None):
        mime = MimeTypes()
        guessed_type = mime.guess_type(filename)[0]

        file_metadata = {
            GD.NAME: filename
        }
        if self.write_as_google_doc and guessed_type == GD.CSV:
            file_metadata[GD.MIME_TYPE] = GD.SPREADSHEET

        if guessed_type is None:
            guessed_type = GD.BINARY_STREAM

        media = MediaIoBaseUpload(
            file_handle,
            mimetype=guessed_type,
            resumable=True
        )

        query = query_parents_in([parent_id], name=filename, trashed=False)
        files = self.googledrive_list(query)

        if len(files) == 0:
            if parent_id:
                file_metadata[GD.PARENTS] = [parent_id]

            self.googledrive_create(
                body=file_metadata,
                media_body=media
            )
        else:
            self.googledrive_update(
                file_id=get_id(files[0]),
                body=file_metadata,
                media_body=media
            )

    def googledrive_update(self, file_id, body, media_body=None):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                file = self.drive.files().update(
                    fileId=file_id,
                    body=body,
                    media_body=media_body,
                    fields=GD.ID
                ).execute()
                return file
            except HttpError as err:
                self.handle_googledrive_errors(err, "update")
            attempts = attempts + 1
            logger.info('googledrive_update:attempts={}'.format(attempts))
        raise Exception("Max number of attempts reached in Google Drive directory update operation")

    def googledrive_delete(self, item, parent_id=None):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                if len(item[GD.PARENTS]) == 1 or parent_id is None:
                    self.drive.files().delete(fileId=get_id(item)).execute()
                else:
                    self.drive.files().update(fileId=get_id(item), removeParents=parent_id).execute()
            except HttpError as err:
                if err.resp.status == 404:
                    return
                self.handle_googledrive_errors(err, "delete")
            attempts = attempts + 1
            logger.info('googledrive_update:attempts={}'.format(attempts))
        raise Exception("Max number of attempts reached in Google Drive directory delete operation")

    def handle_googledrive_errors(self, err, context=""):
        if err.resp.status in [403, 500, 503]:
            sleep(5 + randrange(5))
        else:
            reason = ""
            if err.resp.get('content-type', '').startswith('application/json'):
                reason = json.loads(err.content).get('error').get('errors')[0].get('reason')
            raise Exception("Googledrive " + context + " error : " + reason)
