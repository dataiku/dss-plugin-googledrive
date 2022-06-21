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
from dku_googledrive.googledrive_utils import GoogleDriveUtils as gdu
from time import sleep
from dku_googledrive.memory_cache import MemoryCache

try:
    from BytesIO import BytesIO  # for Python 2
except ImportError:
    from io import BytesIO  # for Python 3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='googledrive plugin %(levelname)s - %(message)s')


class GoogleDriveSessionError(ValueError):
    pass


class GoogleDriveSession():
    """
    Google Drive Session

    :param config: the dict of the configuration of the object
    :param plugin_config: contains the plugin settings
    """
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
        if not self.root_id:
            self.root_id = gdu.ROOT_ID
        self.max_attempts = 5
        self.root_id = gdu.get_root_id(config)
        self.drive = build(
            gdu.API,
            gdu.API_VERSION,
            http=http_auth,
            cache=MemoryCache()  # Fix for ImportError messages https://github.com/googleapis/google-api-python-client/issues/325
        )

    def get_item_from_path(self, path_and_file):
        tokens = gdu.split_path(path_and_file)
        if len(tokens) == 1:
            return {
                gdu.MIME_TYPE: gdu.FOLDER,
                gdu.SIZE: u'0',
                gdu.ID: self.root_id,
                gdu.NAME: u'/'
            }
        parent_ids = [self.root_id]

        for token in tokens:
            if token == '/':
                token = ''
                continue

            query = gdu.query_parents_in(parent_ids, name_contains=token, trashed=False)
            files = self.googledrive_list(query)
            files = gdu.keep_files_with(files, name_starting_with=token)
            files = gdu.keep_files_with(files, name=token)  # we only keep files / parent_ids for names = current token for the next loop

            if len(files) == 0:
                return None
            parent_ids = gdu.get_files_ids(files)
        return files[0]

    def googledrive_download(self, item, stream):
        if gdu.is_file_google_doc(item):
            document_type = gdu.get_google_doc_type(item)
            data = self.drive.files().export_media(
                fileId=gdu.get_id(item),
                mimeType=gdu.get_google_doc_mime_equivalence(document_type)
            ).execute()
            file_handle = BytesIO()
            file_handle.write(data)
            file_handle.seek(0)
            shutil.copyfileobj(file_handle, stream)
        else:
            request = self.drive.files().get_media(fileId=gdu.get_id(item))
            downloader = MediaIoBaseDownload(stream, request, chunksize=1024*1024)
            done = False
            while done is False:
                status, done = downloader.next_chunk()

    def directory(self, item, root_path=None):
        query = gdu.query_parents_in([gdu.get_id(item)], trashed=False)
        files = self.googledrive_list(query)
        return files

    def googledrive_list(self, query):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                files = []
                kwargs = {
                    'q': query,
                    'fields': gdu.LIST_FIELDS,
                    'includeItemsFromAllDrives': True,
                    'supportsAllDrives': True
                }
                initial_call = True
                next_page_token = None
                while initial_call or next_page_token:
                    initial_call = False
                    if next_page_token:
                        kwargs['pageToken'] = next_page_token
                    response = self.drive.files().list(**kwargs).execute()
                    files.extend(response.get('files', []))
                    next_page_token = response.get('nextPageToken')
                return files
            except HttpError as err:
                self.handle_googledrive_errors(err, "list")
            attempts = attempts + 1
            logger.info('googledrive_list:attempts={} on {}'.format(attempts, query))
        raise GoogleDriveSessionError("Max number of attempts reached in Google Drive directory list operation")

    def create_directory_from_path(self, path):
        tokens = gdu.split_path(path)

        parent_ids = [self.root_id]
        current_path = ""

        for token in tokens:
            current_path = os.path.join(current_path, token)
            item = self.get_item_from_path(current_path)
            if item is None:
                new_directory_id = self.create_directory(token, parent_ids)
                parent_ids = [new_directory_id]
            else:
                new_directory_id = gdu.get_id(item)
                parent_ids = [new_directory_id]
        return new_directory_id

    def create_directory(self, name, parent_ids):
        file_metadata = {
            gdu.NAME: name,
            gdu.PARENTS: parent_ids,
            gdu.MIME_TYPE: gdu.FOLDER
        }
        file = self.googledrive_create(body=file_metadata)
        return gdu.get_id(file)

    def googledrive_create(self, body, media_body=None, parent_id=None):
        attempts = 0
        if parent_id:
                body[gdu.PARENTS] = [parent_id]

        while attempts < self.max_attempts:
            try:
                file = self.drive.files().create(
                    body=body,
                    media_body=media_body,
                    fields=gdu.ID,
                    supportsAllDrives=True
                ).execute()
                return file
            except HttpError as err:
                self.handle_googledrive_errors(err, "create")
            attempts = attempts + 1
            logger.info('googledrive_create:attempts={} on {}'.format(attempts, body))
        raise GoogleDriveSessionError("Max number of attempts reached in Google Drive directory create operation")

    def googledrive_upload(self, filename, file_handle, parent_id=None):
        mime = MimeTypes()
        guessed_type = mime.guess_type(filename)[0]

        file_metadata = {
            gdu.NAME: filename
        }
        if self.write_as_google_doc and guessed_type == gdu.CSV:
            file_metadata[gdu.MIME_TYPE] = gdu.SPREADSHEET
            if filename.lower().endswith(".csv"):
                file_metadata[gdu.NAME] = filename + ".csv"

        if guessed_type is None:
            guessed_type = gdu.BINARY_STREAM

        media = MediaIoBaseUpload(
            file_handle,
            mimetype=guessed_type,
            resumable=True
        )

        query = gdu.query_parents_in([parent_id], name=filename, trashed=False)
        files = self.googledrive_list(query)

        if len(files) == 0:
            self.googledrive_create(
                body=file_metadata,
                media_body=media,
                parent_id=parent_id
            )
        else:
            self.googledrive_update(
                file_id=gdu.get_id(files[0]),
                body=file_metadata,
                media_body=media,
                parent_id=parent_id
            )

    def googledrive_update(self, file_id, body, media_body=None, parent_id=None):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                file = self.drive.files().update(
                    fileId=file_id,
                    body=body,
                    media_body=media_body,
                    fields=gdu.ID,
                    supportsAllDrives=True
                ).execute()
                logger.info("googledrive_update on {} successfull".format(body))
                return file
            except HttpError as err:
                if err.resp.status == 404:
                    logger.info("googledrive_update: 404 on {}, trying googledrive_create".format(body))
                    file = self.googledrive_create(
                        body=body,
                        media_body=media_body,
                        parent_id=parent_id
                    )
                    logger.info("googledrive_create:googledrive_create done")
                    return file
                else:
                    self.handle_googledrive_errors(err, "update")
            attempts = attempts + 1
            logger.info('googledrive_update:attempts={} on {}'.format(attempts, body))
        raise GoogleDriveSessionError("Max number of attempts reached in Google Drive directory update operation")

    def googledrive_delete(self, item, parent_id=None):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                if len(item[gdu.PARENTS]) == 1 or parent_id is None:
                    self.drive.files().delete(
                        fileId=gdu.get_id(item),
                        supportsAllDrives=True
                    ).execute()
                else:
                    self.drive.files().update(
                        fileId=gdu.get_id(item),
                        removeParents=parent_id,
                        supportsAllDrives=True
                    ).execute()
            except HttpError as err:
                logger.warn("HttpError={}".format(err))
                if err.resp.status == 404:
                    return
                self.handle_googledrive_errors(err, "delete")
            attempts = attempts + 1
            logger.info('googledrive_delete:attempts={} on {}'.format(attempts, item))
        raise GoogleDriveSessionError("Max number of attempts reached in Google Drive directory delete operation")

    def handle_googledrive_errors(self, err, context=""):
        if err.resp.status in [403, 500, 503]:
            sleep(5 + randrange(5))
        else:
            reason = ""
            if err.resp.get('content-type', '').startswith('application/json'):
                reason = json.loads(err.content).get('error').get('errors')[0].get('reason')
            raise GoogleDriveSessionError("Googledrive {} error : {}".format(context, reason))
