from dku_plugin_test_utils import dss_scenario

TEST_PROJECT_KEY = "PLUGINTESTGOOGLEDRIVE"


def test_run_googledrive_directory_pagination(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="DirectoryPagination")
