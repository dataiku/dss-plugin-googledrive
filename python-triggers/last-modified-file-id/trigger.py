import logging
from dataiku.customtrigger import get_plugin_config
from dataiku.customtrigger import get_trigger_config
from dataiku.scenario import Trigger
from dku_googledrive.session import GoogleDriveSession
from dataiku import Dataset, default_project_key
from datetime import datetime


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='googledrive plugin %(levelname)s - %(message)s')

current_project_key = default_project_key()
plugin_config = get_plugin_config()
trigger_config = get_trigger_config()

trigger = Trigger()
config = plugin_config.get("config", {})

google_sheet_file_id = config.get("google_sheets_file_id")
if google_sheet_file_id is None:
    logger.error("File ID is empty")
    raise Exception("File ID cannot be left empty")

target_dataset = config.get("target_dataset")
if target_dataset is None:
    logger.error("Target dataset is empty")
    raise Exception("Target dataset cannot be left empty")

plugin_config = plugin_config.get("pluginConfig", {})
session = GoogleDriveSession(config, plugin_config)

remote_file_last_modified = session.get_last_modified_by_file_id(google_sheet_file_id)
remote_file_last_modified_epoch = 0
try:
    remote_file_last_modified_epoch = int(datetime.fromisoformat(remote_file_last_modified).timestamp()) * 1000
except Exception as error_message:
    logger.error("Could not convert remote file last modified date: {}. Error {}".format(remote_file_last_modified,error_message ))

local_dataset_info = Dataset(target_dataset, project_key=current_project_key, ignore_flow=False).get_files_info()
local_dataset_global_paths =local_dataset_info.get("globalPaths", [])
local_dataset_last_modified = None
if len(local_dataset_global_paths) > 0:
    local_dataset_last_modified = local_dataset_global_paths[0].get("lastModified")

if isinstance(remote_file_last_modified_epoch, int) and isinstance(local_dataset_last_modified, int):
    if remote_file_last_modified_epoch > local_dataset_last_modified:
        logger.info("remote epoch {} > local epoch {}, firing the trigger".format(remote_file_last_modified_epoch, local_dataset_last_modified))
        trigger.fire()
    else:
        logger.info("Target dataset is up to date")
