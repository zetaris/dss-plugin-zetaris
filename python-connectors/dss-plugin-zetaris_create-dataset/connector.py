# This file is the actual code for the custom Python dataset dss-plugin-zetaris_create-dataset

# import the base class for the custom dataset
from six.moves import xrange
from dataiku.connector import Connector

"""
A custom Python dataset is a subclass of Connector.

The parameters it expects and some flags to control its handling by DSS are
specified in the connector.json file.

Note: the name of the class itself is not relevant.
"""
import logging
from numpy import isnan
from zstr_session import ZstrSession

import dataiku
from dataiku.exporter import Exporter


logging.basicConfig(level=logging.INFO, format='dss-plugin-microstrategy %(levelname)s - %(message)s')
logger = logging.getLogger()


class CustomExporter(Exporter):
    """
    The methods will be called like this:
       __init__
       open
       write_row
       write_row
       write_row
       ...
       write_row
       close
    """

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
                "self.project_id, self.folder_id = self.get_ui_browse_results(config)
        """
        self.row_buffer = []
        self.buffer_size = 5000
        logger.info("Starting Zetaris exporter v1.3.0")
        # Plugin settings

        self.base_url = get_base_url(config, plugin_config)
        self.project_name = config["zetaris_project"].get("project_name", None)
        self.project_id = ""  # the project id, obtained through a later request
        self.dataset_name = str(config.get("dataset_name", None)).replace(" (created by Dataiku DSS)", "") + " (created by Dataiku DSS)"
        self.dataset_id = ""  # the dataset id, obtained at creation or on update
        self.table_name = "dss_data"
        self.username = config["zetaris_api"].get("username", None)
        self.password = config["zetaris_api"].get("password", '')
        self.query_param = config.get("dataset_name", False)
        generate_verbose_logs = config.get("generate_verbose_logs", False)
        self.upload_session_id = None
        Z = ZstrSession(self.base_url, self.username, self.password, generate_verbose_logs=generate_verbose_logs)
        Z.execute_select(self.query_param ,100)


        if not (self.username and self.base_url):
            logger.error('Connection params: {}'.format(
                {
                    'username:': self.username,
                    'password:': '#' * len(self.password),
                    'base_url:': self.base_url
                })
            )
            raise ValueError("username and base_url must be filled")

    def get_ui_browse_results(self, config):
        import json
        folder_id = None
        project_id = config.get("selected_project_id", None)
        selected_folder_id = json.loads(config.get("selected_folder_id", "{}"))
        folder_ids = selected_folder_id.get("ids")
        if folder_ids:
            folder_id = folder_ids[-1]
        if config.get("destination", "my_reports") == "my_reports":
            folder_id = None
        return project_id, folder_id

    def open(self, schema):
        self.dss_columns_types = get_dss_columns_types(schema)
        (self.schema, dtypes, parse_dates_columns) = dataiku.Dataset.get_dataframe_schema_st(schema["columns"])

        # Prevent problems when reading int
        # If we don't use it too, the initialization of the cube does not have the same dtypes as the read data (in write_row)
        # and we get mismatchs when updating
        # if dtypes is not None:

        # Get a project list, search for our project in the list, get the project ID for future API calls.
        if not self.project_id:
            self.project_id = self.session.get_project_id(self.project_name)

        # Search for objects of type 3 (datasets/cubes) with the right name
        logger.info("Searching for existing '{}' dataset in project '{}'.".format(self.dataset_name, self.project_id))
        self.dataset_id = self.session.get_dataset_id(self.project_id, self.dataset_name, folder_id=self.folder_id)

        # No result, create a new dataset
        if not self.dataset_id:
            logger.info("Creating dataset '{}'".format(self.dataset_name))
            self.dataset_id = self.session.create_dataset(
                self.project_id,
                self.dataset_name,
                self.table_name,
                self.schema,
                self.dss_columns_types,
                self.folder_id
            )

        # Replace data (drop existing) by sending the empty dataframe, with correct schema
        self.session.update_dataset([], self.project_id, self.dataset_id, self.table_name, self.schema, self.dss_columns_types, update_policy='replace')
        self.upload_session_id = self.session.open_upload_session(self.project_id, self.dataset_id, self.table_name, schema, self.dss_columns_types, update_policy='replace', can_raise=True)

    def write_row(self, row):
        row_dict = {}
        for (column_name, cell_value, dtype) in zip(self.schema, row, self.dss_columns_types):
            if (type(cell_value) == float and isnan(cell_value)):
                cell_value = None
            row_dict[column_name] = cell_value
        self.row_buffer.append(row_dict)

        if len(self.row_buffer) > self.buffer_size:
            logger.info("Sending {} rows to Zetaris.".format(self.buffer_size))
            self.flush_data(self.row_buffer)
            self.row_buffer = []

    def close(self):
        logger.info("Sending {} final rows to Zetaris.".format(len(self.row_buffer)))
        self.flush_data(self.row_buffer)
        logger.info("Logging out.")
        self.session.publish_upload_session()
        self.session.upload_session_publish_status()
        response = self.session.get(url=self.base_url+"/auth/logout")
        logger.info("Logout returned status {}".format(response.status_code))

    def flush_data(self, rows):
        try:
            self.session.upload_session_push_rows(rows)
        except Exception as error_message:
            logger.exception("Dataset update issue: {}".format(error_message))
            raise error_message


def get_dss_columns_types(schema):
    columns = schema.get("columns", [])
    columns_types = []
    for column in columns:
        column_type = column.get("type")
        columns_types.append(column_type)
    return columns_types